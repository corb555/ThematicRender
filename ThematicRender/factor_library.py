from functools import wraps
from typing import Dict, Callable

import numpy as np

# factor_library.py
from ThematicRender.keys import FactorKey, DriverKey
from ThematicRender.settings import EDGE_PROFILES
from ThematicRender.spatial_math import normalize_step, lerp, refine_organic_signal
from ThematicRender.utils import print_once

# Registry for the FactorEngine
FACTOR_REGISTRY: Dict[str, Callable] = {}


def spatial_factor(function_id: str):
    def decorator(func):
        @wraps(func)
        def wrapper(val_2d, vld_2d, name, ctx):
            # 1. Start the timer for this specific factor
            # We use the 'name' (e.g. 'lith') rather than function_id
            # so we can distinguish between factors sharing the same logic.
            timer_key = f"    Factor:{name}"

            # Use getattr in case a context is passed without a timer during testing
            tmr = getattr(ctx, 'tmr', None)
            if tmr: tmr.start(timer_key)

            # 2. THE FIREWALL: Execute the math
            res_2d = func(val_2d, vld_2d, name, ctx)

            # 3. Stop the timer
            if tmr: tmr.end()

            # 4. Restore 3D storage shape
            return res_2d[..., np.newaxis]

        FACTOR_REGISTRY[function_id] = wrapper
        return wrapper

    return decorator


class FactorLibrary:
    @staticmethod
    @spatial_factor("elevation_raw")
    def elevation_raw(val_2d, vld_2d, name, ctx):
        """Pass-through of raw DEM meters."""
        return val_2d[DriverKey.DEM]

    @staticmethod
    @spatial_factor("water_alpha")
    def water_alpha(val_2d, vld_2d, name, ctx):
        prox = val_2d.get(DriverKey.WATER_PROXIMITY)
        # Get the binary mask for water from the theme
        label_to_val = ctx.themes.label_to_val
        water_val = label_to_val.get("water", 4)
        mask = (val_2d.get(DriverKey.THEME) == water_val)

        # 1. Define the fade distance (e.g., 15 meters)
        # 0m from shore = 0.0 alpha, 15m from shore = 1.0 alpha
        alpha = np.clip(prox / 15.0, 0.0, 1.0)

        # 2. Return the mask * alpha (0.0 on land, 0.0-1.0 transition in water)
        return alpha * mask

    @staticmethod
    @spatial_factor("elevation_norm")
    def elevation_norm(val_2d, vld_2d, name, ctx):
        """Normalized elevation signal with organic smoothing."""
        params = ctx.cfg.get_logic("dem")
        raw = val_2d[DriverKey.DEM]

        # Standardize to 0..1 based on project bounds
        w = np.clip((raw - params['start']) / (params['full'] - params['start'] + 1e-6), 0, 1)

        # Apply the organic refiner (noise swings, etc.)
        return refine_organic_signal(
            mask=w, blur_px=0.0,  # Handled by render_task
            noise_amp=params.get("noise_amp", 0.1), noise_id="biome",
            contrast=params.get("contrast", 1.0), max_opacity=1.0, ctx=ctx, name=name
        )

    @staticmethod
    @spatial_factor("moisture")
    def moisture(val_2d, vld_2d, name, ctx):
        return _gated_step_logic(
            val_2d, vld_2d, name, ctx, d_key=DriverKey.PRECIP, f_key=FactorKey.PRECIP, lerp_low=1.0,
            noise_id="biome"
        )

    @staticmethod
    @spatial_factor("canopy")
    def canopy(val_2d, vld_2d, name, ctx):
        params = ctx.cfg.get_logic("forest")
        height_data = val_2d[DriverKey.FOREST]

        # 1. Normalize and apply Sensitivity (Gamma)
        start, full = float(params["start"]), float(params["full"])
        res = np.clip((height_data - start) / (full - start + 1e-6), 0.0, 1.0)

        sensitivity = float(params.get("sensitivity", 1.0))
        if sensitivity != 1.0:
            res = np.power(res, 1.0 / max(sensitivity, 0.01))

        # 2. THE REFINER: This handles the edge softening and noise variation
        t_2d = refine_organic_signal(
            mask=res, blur_px=0.0,  # Already blurred by render_task (r=4.0)
            noise_amp=float(params.get("noise_amp", 0.0)), noise_id="forest",
            # Uses the multi-scale forest noise
            contrast=float(params.get("contrast", 1.0)),
            max_opacity=float(params.get("max_opacity", 1.0)), ctx=ctx, name=name
        )

        return t_2d * vld_2d[DriverKey.FOREST]

    @staticmethod
    @spatial_factor("lith")
    def lith(val_2d, vld_2d, name, ctx):
        return _gated_step_logic(
            val_2d, vld_2d, name, ctx, d_key=DriverKey.LITH, f_key=FactorKey.LITH, lerp_low=0.0,
            noise_id="biome"
        )

    @staticmethod
    @spatial_factor("theme")
    def theme(val_2d, vld_2d, name, ctx):
        """
        Thematic mask refiner.
        Converts sanitized 2D IDs into an organic multi-category alpha mask.
        """
        # 1. Access sanitized 2D data directly from the Firewall dict
        theme_ids = val_2d.get(DriverKey.THEME)

        # Use 'is None' to avoid the Ambiguity Error
        if theme_ids is None or not np.any(theme_ids):
            return np.zeros(ctx.target_shape, dtype="float32")

        # 2. Extract spatial dimensions
        # Ensure we are working with exactly (H, W)
        idx = theme_ids[:, :, 0] if theme_ids.ndim == 3 else theme_ids
        h, w = idx.shape

        label_to_val = ctx.themes.label_to_val
        composite_alpha = np.zeros((h, w), dtype=np.float32)

        # 3. Iterate through defined categories (from settings/registry)
        # EDGE_PROFILES defines which categories get organic treatment

        for label, profile in EDGE_PROFILES.items():
            val = label_to_val.get(label)
            if val is None:
                continue

            mask = (idx == val)
            if not np.any(mask):
                continue

            # 4. Pull category-specific math params from settings.py
            # If a category (like 'playa') isn't in logic params, we use defaults
            try:
                params = ctx.cfg.get_logic(label)
            except KeyError:
                params = {"noise_amp": 0.3, "contrast": 0.7, "max_opacity": 0.8}

            # 5. Refine the category mask using the new 2D Contract helper
            cat_alpha = refine_organic_signal(
                mask=mask, blur_px=profile.edge_blur_px,
                noise_amp=float(params.get("noise_amp", 0.3)),
                noise_id=params.get("noise_id", "biome"),
                contrast=float(params.get("contrast", 0.7)),
                max_opacity=float(params.get("max_opacity", 0.8)), ctx=ctx, name=label
            )

            # 6. Composite using MAX (Priority is handled by Precedence in render_task)
            composite_alpha = np.maximum(composite_alpha, cat_alpha)

        # 7. Final Cleanup: Ensure category 0 remains transparent
        composite_alpha[idx == 0] = 0.0

        # Return 2D (spatial_factor decorator will add the final [..., np.newaxis])
        return composite_alpha

    @staticmethod
    @spatial_factor("snow")
    def snow(val_2d, vld_2d, name, ctx):
        params = ctx.cfg.get_logic("snow")
        raw_dem = val_2d[DriverKey.DEM]

        start = float(params["snowline"]) - float(params["ramp"])
        end = float(params["snowline"]) + float(params["ramp"])
        density = np.clip((raw_dem - start) / (end - start + 1e-6), 0.0, 1.0)

        # Handle noise if present in drivers
        noise = val_2d.get("noise", 0.5)
        return np.clip(((density - noise) * 1.0) + 0.5, 0.0, 1.0)

    @staticmethod
    @spatial_factor("water_ripples")
    def water_ripples(val_2d, vld_2d, name, ctx):
        """Generates soft wave shading. Returns 1.0 on land to avoid blacking out the map."""
        params = ctx.cfg.get_logic("water")
        noise_provider = ctx.noises.get("water")

        # 1. Get the 2D water mask from the previous 'water' factor
        # We use our safe helper to ensure 'water' was already generated
        water_mask = _get_required_factor(ctx, "water")

        # 2. Sample Noise for the waves
        scale = float(params.get("ripple_scale", 3.0))
        noise = np.squeeze(noise_provider.window_noise(ctx.window, scale_override=scale))

        # 3. Calculate shading (e.g., ranges from 0.8 to 1.0)
        intensity = float(params.get("ripple_intensity", 0.2))
        shading = (1.0 - intensity) + (noise * intensity)

        # We want 'shading' where there is water, and '1.0' where there is land.
        # Use a 2D lerp: lerp(LandValue, WaterValue, Mask)
        # (1.0 * (1 - mask)) + (shading * mask)
        res = 1.0 + water_mask * (shading - 1.0)

        return res

    @staticmethod
    @spatial_factor("water_glint")
    def water_glint(val_2d, vld_2d, name, ctx):
        """Generates sharp sun sparkles (highlights)."""
        params = ctx.cfg.get_logic("water")
        noise_provider = ctx.noises.get("water")

        # Access ctx.factors and squeeze to 2D
        raw_water_mask = _get_required_factor(ctx, "water")
        if raw_water_mask is None:
            return np.zeros(ctx.target_shape)

        water_mask = np.squeeze(raw_water_mask)

        # 1. Sample Noise at a fine scale
        scale = float(params.get("glint_scale", 6.0))
        noise = np.squeeze(noise_provider.window_noise(ctx.window, scale_override=scale))

        # 2. Glint Math
        n = np.clip(noise + float(params.get("glint_floor", 0.4)) - 0.5, 0, 1)
        sensitivity = float(params.get("glint_sensitivity", 2.0))
        glints = np.power(n, 10.0 / max(sensitivity, 0.1))

        return glints * water_mask

    @staticmethod
    @spatial_factor("hillshade")
    def hillshade(val_2d, vld_2d, name, ctx):
        hs_data = val_2d.get(DriverKey.HILLSHADE)
        if hs_data is None:
            return np.ones(ctx.target_shape, dtype="float32")

        p = ctx.cfg.get_logic("hillshade")

        # 1. Math is performed on 2D HS data (0-255)
        val = np.clip(hs_data / 255.0, 0.0, 1.0)

        # Protection Math (Shadows/Highlights)
        t_shad = (val - float(p["shadow_start"])) / max(
            float(p["shadow_end"]) - float(p["shadow_start"]), 1e-6
            )
        w_shad = (1.0 - np.clip(t_shad, 0, 1)) * float(
            p["protect_shadows"]
            )  # Use clip instead of smoothstep for simplicity

        t_high = (val - float(p["highlight_start"])) / max(
            float(p["highlight_end"]) - float(p["highlight_start"]), 1e-6
            )
        w_high = np.clip(t_high, 0, 1) * float(p["protect_highlights"])

        # Final blend
        m_protected = val + np.maximum(w_shad, w_high) * (1.0 - val)
        m_final = 1.0 + float(p.get("strength", 0.8)) * (m_protected - 1.0)

        # lerp(1.0, m_final, valid) where valid is 2D
        return 1.0 + vld_2d[DriverKey.HILLSHADE] * (m_final - 1.0)

    @staticmethod
    @spatial_factor("water_mask")
    def water_mask(val_2d, vld_2d, name, ctx):
        """
        Extracts a binary mask for the 'water' category from the theme.
        Input: val_2d is a dict of sanitized 2D arrays.
        """
        # 1. Access the sanitized 2D IDs
        theme_ids = val_2d.get(DriverKey.THEME)

        # 2. Use 'is None' for a safe check
        if theme_ids is None:
            return np.zeros(ctx.target_shape, dtype="float32")

        # 3. Lookup the integer ID for water from the theme registry
        label_to_val = ctx.themes.label_to_val
        water_val = label_to_val.get("water")

        if water_val is None:
            # Fallback if QML doesn't define 'water'
            print_once(
                "missing_water_id",
                "⚠️ Warning: 'water' label not found in QML. Water mask will be empty."
                )
            return np.zeros(ctx.target_shape, dtype="float32")

        # 4. Create the mask (2D boolean -> 2D float32)
        # firewall guaranteed theme_ids is (H, W) or (H, W, 1)
        # We slice to ensure we have a 2D result for the math
        idx = theme_ids[:, :, 0] if theme_ids.ndim == 3 else theme_ids
        mask = (idx == water_val).astype("float32")

        # Return 2D: The @spatial_factor decorator will handle the rest
        return mask

    @staticmethod
    @spatial_factor("water_depth")
    def water_depth(val_2d, vld_2d, name, ctx):
        params = ctx.cfg.get_logic("water")
        prox_data = val_2d.get(DriverKey.WATER_PROXIMITY)

        # Get the binary water mask (factor) created in Step 7
        water_mask = np.squeeze(ctx.factors_2d.get("water", 0.0))

        if prox_data is None:
            return np.zeros(ctx.target_shape, dtype="float32")

        # 1. Linear Map
        max_d = float(params.get("max_depth_px", 100.0))
        res = np.clip(prox_data / max_d, 0.0, 1.0)

        # 2. SENSITIVITY (The Stretch)
        # Use a low value (e.g. 0.4) to make the "shallow" zone stay
        # shallow much further from the shore.
        sensitivity = float(params.get("depth_sensitivity", 1.0))
        if sensitivity != 1.0:
            res = np.power(res, 1.0 / max(sensitivity, 0.01))

        # 3. Mask to water only
        return res * water_mask


def _gated_step_logic(val_2d, vld_2d, name, ctx, d_key, f_key, lerp_low, noise_id):
    # 1. Fetch sanitized logic params from settings.py
    params = ctx.cfg.get_logic(f_key.value)

    # 2. Extract 2D Spatial Data (The Firewall ensures val_2d[d_key] is either (H,W) or (H,W,B))
    raw_data = val_2d[d_key]
    if raw_data.ndim == 3:
        # Surgical extraction of the correct band
        band_idx = int(params.get("band", 1)) - 1
        data_2d = raw_data[:, :, band_idx]
    else:
        data_2d = raw_data

    # 3. Math (Guaranteed 2D)
    w_2d = normalize_step(data_2d, float(params["start"]), float(params["full"]))

    # 4. Refinement
    t_2d = refine_organic_signal(
        mask=w_2d, blur_px=float(params.get("blur_px", 0.0)),
        noise_amp=float(params.get("noise_amp", 0.0)), noise_id=noise_id,
        contrast=float(params.get("contrast", 1.0)),
        max_opacity=float(params.get("max_opacity", 1.0)), ctx=ctx, name=name
    )

    # 5. Result (Return 2D - the Decorator will expand it)
    return lerp(float(lerp_low), t_2d, vld_2d[d_key])


def _compute_gated_step_factor(*, drivers, name, ctx, spec):
    blk = drivers.get(spec.driver_key)
    if blk is None:
        raise ValueError(f"Factor {name} missing driver {spec.driver_key}")

    params = ctx.cfg.driver(spec.factor_key.value)
    # Squeeze input data to 2D for logic
    data_2d = np.squeeze(blk.value)

    w_2d = normalize_step(data_2d, float(params["start"]), float(params["full"]))

    # Pass through refinement (2D)
    t_2d = refine_organic_signal(
        mask=w_2d, blur_px=float(params.get("blur_px", 0.0)),
        noise_amp=float(params.get("noise_amp", 0.0)), noise_id=spec.noise_id,
        contrast=float(params.get("contrast", 1.0)), ctx=ctx, name=name
    )

    # Final Assembly: Convert to 3D only at the very end to match blk.valid (H,W,1)
    t_3d = t_2d[..., np.newaxis]
    lerp_low = float(spec.lerp_low)

    return lerp(lerp_low, t_3d, blk.valid)


def _get_required_factor(ctx, name):
    """Safe lookup for internal factor dependencies."""
    f = ctx.factors.get(name)
    if f is not None:
        return np.squeeze(f)
    else:
        # Check if the factor  exists in the master spec list
        from ThematicRender.settings import FACTOR_SPECS
        all_names = [s.name for s in FACTOR_SPECS]

        if name not in all_names:
            raise KeyError(f"Factor Logic Error: '{name}' is not defined in FACTOR_SPECS.")
        else:
            raise KeyError(
                f"Factor Sequence Error: A factor tried to access '{name}', "
                f"but '{name}' hasn't been generated yet. Move '{name}' higher in settings.py."
                f"See xx_describe.md for details."
            )
