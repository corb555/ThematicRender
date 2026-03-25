from functools import wraps
from typing import Dict, Callable

import numpy as np

# factor_library.py
from ThematicRender.keys import DriverKey
from ThematicRender.spatial_math import normalize_step, lerp
from ThematicRender.theme_registry import refine_organic_signal_a, refine_organic_signal_b

# factor_library.py

FACTOR_REGISTRY: Dict[str, Callable] = {}


def spatial_factor(function_id: str):
    """
    Registers a library function and enforces the (H, W, 1) storage contract.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(data_2d, masks_2d, name, ctx):
            # The Engine passes a SimpleNamespace 'ctx' which includes the current 'spec'
            timer_key = f"Factor: {name}"
            if ctx.tmr: ctx.tmr.start(timer_key)

            # Execute the 2D math
            res_2d = func(data_2d, masks_2d, name, ctx)

            if ctx.tmr: ctx.tmr.end()

            # Promote to 3D for the storage layer firewall
            return res_2d[..., np.newaxis]

        FACTOR_REGISTRY[function_id] = wrapper
        return wrapper

    return decorator


# -----------------------------------------------------------------------------
# Logic Blocks
# -----------------------------------------------------------------------------

def _map_and_refine(data_2d, masks_2d, name, lib_ctx, driver_key):
    """
    Standard pipeline for 'Remapping' factors (DEM, Precip, Forest).
    Raw -> Normalize -> Organic Refine -> Masked Default.
    """
    params = lib_ctx.cfg.get_logic(name)
    # 1. Catch the mismatch immediately
    if not params:
        raise KeyError(
            f"Factor '{name}' failed to find its parameters in config"
            f"Ensure 'driver_logic_params' contains a key named '{name}'."
        )

    # Identify the specific driver band requested
    raw_data = data_2d[driver_key]
    if raw_data.ndim == 3:
        band_idx = int(params.get("band", 1)) - 1
        raw_plane = raw_data[:, :, band_idx]
    else:
        raw_plane = raw_data

    # 1. Physical Remapping: Linear normalization based on config start/full
    remaped = normalize_step(raw_plane, float(params.get("start")), float(params.get("full")))

    # 2. Organic Naturalization: Blur, Noise, Contrast, and Power-curves
    refined = refine_organic_signal_a(
        mask=remaped, blur_px=float(params.get("blur_px", 0.0)),
        noise_amp=float(params.get("noise_amp", 0.0)), noise_id=params.get("noise_id"),
        # Uses 'geology', 'biome', etc.
        contrast=float(params.get("contrast", 1.0)),
        max_opacity=float(params.get("max_opacity", 1.0)), ctx=lib_ctx, name=name
    )

    # 3. Gating: Handle NoData areas using a default fill value (e.g. 1.0 for Arid moisture)
    default_val = float(params.get("default_fill", 0.0))
    valid_mask = np.squeeze(masks_2d[driver_key])

    return lerp(default_val, refined, valid_mask)


# -----------------------------------------------------------------------------
# The Library
# -----------------------------------------------------------------------------

class FactorLibrary:

    @staticmethod
    @spatial_factor("elevation_raw")
    def elevation_raw(data_2d, masks_2d, name, lib_ctx):
        """Pass-through for raw physical meters (used for ramp sampling)."""
        # Get the  driver defined in the spec for this factor
        driver_key = next(iter(lib_ctx.spec.drivers))
        return data_2d[driver_key]

    @staticmethod
    @spatial_factor("mapped_signal")
    def mapped_signal(data_2d, masks_2d, name, lib_ctx):
        """
        Generic entry for Lith, Moisture, Canopy, and Normalized Elevation.
        Logic is entirely defined by the config params for 'name'.
        """
        driver_key = next(iter(lib_ctx.spec.drivers))
        return _map_and_refine(data_2d, masks_2d, name, lib_ctx, driver_key)

    @staticmethod
    @spatial_factor("theme_composite")
    def theme_composite(data_2d, masks_2d, name, lib_ctx):
        """Aggregate configured thematic categories into a composite alpha."""
        theme_ids = data_2d[DriverKey.THEME]

        # cleanup of categorical IDs
        theme_ids = lib_ctx.themes.get_smoothed_ids(theme_ids)

        tile_ctx = lib_ctx.themes.build_tile_context(theme_ids)
        composite_alpha = np.zeros(lib_ctx.target_shape, dtype=np.float32)

        for spec in tile_ctx.active_specs:
            binary_mask = tile_ctx.masks_by_id[spec.theme_id]
            if not np.any(binary_mask):
                continue

            cat_alpha = refine_organic_signal_b(
                mask=binary_mask,
                spec=spec,
                ctx=lib_ctx,
            )
            composite_alpha = np.maximum(composite_alpha, cat_alpha)

        return composite_alpha * np.squeeze(masks_2d[DriverKey.THEME])

    @staticmethod
    @spatial_factor("hillshade")
    def hillshade(data_2d, masks_2d, name, lib_ctx):
        """Luminance-protected hillshade factor."""
        driver_key = next(iter(lib_ctx.spec.drivers))
        raw_hs = data_2d.get(driver_key)
        if raw_hs is None:
            return np.ones(lib_ctx.target_shape, dtype="float32")

        p = lib_ctx.cfg.get_logic("hillshade")
        val = np.clip(raw_hs / 255.0, 0.0, 1.0)

        # Gamma adjustment for shading volume
        gamma = float(p.get("gamma", 1.0))
        if gamma != 1.0:
            val = np.power(val, gamma)

        # Apply standard shadow/highlight protection to preserve  colors
        t_shad = (val - float(p["shadow_start"])) / max(
            float(p["shadow_end"]) - float(p["shadow_start"]), 1e-6
        )
        w_shad = (1.0 - np.clip(t_shad, 0, 1)) * float(p["protect_shadows"])

        t_high = (val - float(p["highlight_start"])) / max(
            float(p["highlight_end"]) - float(p["highlight_start"]), 1e-6
        )
        w_high = np.clip(t_high, 0, 1) * float(p["protect_highlights"])

        m_protected = val + np.maximum(w_shad, w_high) * (1.0 - val)
        m_final = 1.0 + float(p.get("strength", 0.8)) * (m_protected - 1.0)

        return 1.0 + np.squeeze(masks_2d[driver_key]) * (m_final - 1.0)

    @staticmethod
    @spatial_factor("specular_highlights")
    def specular_highlights(data_2d, masks_2d, name, lib_ctx):
        params = lib_ctx.cfg.get_logic(name)
        noise_id = lib_ctx.spec.required_noise
        noise_provider = lib_ctx.noises.get(noise_id)

        mask_key = lib_ctx.spec.required_factors[0] if lib_ctx.spec.required_factors else None
        mask = _get_required_factor(lib_ctx, mask_key) if mask_key else 1.0

        scale = float(params.get("scale", 6.0))
        floor = float(params.get("floor", 0.4))
        sensitivity = float(params.get("sensitivity", 2.0))

        noise = np.squeeze(noise_provider.window_noise(lib_ctx.window, scale_override=scale))

        # Math: Subtract floor, clip, then apply aggressive power curve
        n = np.clip(noise + floor - 0.5, 0, 1)
        glints = np.power(n, 10.0 / max(sensitivity, 0.1))

        return glints * mask

    @staticmethod
    @spatial_factor("noise_overlay")
    def noise_overlay(data_2d, masks_2d, name, lib_ctx):
        params = lib_ctx.cfg.get_logic(name)

        # Use the noise profile defined in the spec (e.g., "water")
        noise_id = lib_ctx.spec.required_noise
        noise_provider = lib_ctx.noises.get(noise_id)

        mask_key = lib_ctx.spec.required_factors[0] if lib_ctx.spec.required_factors else None
        mask = _get_required_factor(lib_ctx, mask_key) if mask_key else 1.0

        scale = float(params.get("scale", 3.0))
        noise = np.squeeze(noise_provider.window_noise(lib_ctx.window, scale_override=scale))

        # Pattern: 1.0 is neutral for multiply. Noise pushes it up or down.
        intensity = float(params.get("intensity", 0.2))
        shading = (1.0 - intensity) + (noise * intensity)

        # Blend shading only where the mask exists
        return 1.0 + mask * (shading - 1.0)

    @staticmethod
    @spatial_factor("proximity_power")
    def proximity_power(data_2d, masks_2d, name, lib_ctx):
        params = lib_ctx.cfg.get_logic(name)

        # Use the primary driver from the spec (WATER_PROXIMITY)
        driver_key = next(iter(lib_ctx.spec.drivers))
        prox_data = data_2d.get(driver_key)

        # Use the dependency defined in the spec (usually "water")
        # This removes the hardcoded "water" factor lookup
        mask_key = lib_ctx.spec.required_factors[0] if lib_ctx.spec.required_factors else None
        mask = _get_required_factor(lib_ctx, mask_key) if mask_key else 1.0

        if prox_data is None:
            return np.zeros(lib_ctx.target_shape, dtype="float32")

        # Parameters drive the curve
        max_d = float(params.get("max_range_px", 100.0))
        sensitivity = float(params.get("sensitivity", 1.0))

        res = np.clip(prox_data / max_d, 0.0, 1.0)
        if sensitivity != 1.0:
            res = np.power(res, 1.0 / max(sensitivity, 0.01))

        return res * mask

    @staticmethod
    @spatial_factor("categorical_mask")  # renamed to be generic
    def categorical_mask(data_2d, masks_2d, name, lib_ctx):
        # Pull the label from the config for THIS factor (e.g., params['label'] = "water")
        params = lib_ctx.cfg.get_logic(name)
        target_label = params.get("label", name)

        theme_ids = data_2d.get(DriverKey.THEME)
        if theme_ids is None:
            return np.zeros(lib_ctx.target_shape, dtype="float32")

        # Bridge between YAML logic and QML IDs
        target_val = lib_ctx.theme_registry.name_to_id.get(target_label)
        if target_val is None:
            return np.zeros(lib_ctx.target_shape, dtype="float32")

        # Logic is now generic for any ID in the theme
        return (theme_ids == target_val).astype("float32")

    @staticmethod
    @spatial_factor("edge_fade")
    def edge_fade(data_2d, masks_2d, name, lib_ctx):
        """
        Creates an organic alpha transition based on proximity within a specific category.
        Useful for fading water at the shore or thinning forest at the tree-line.
        """
        params = lib_ctx.cfg.get_logic(name)

        # 1. Fetch Drivers from data dictionary
        # Proximity represents distance (in meters or pixels) from a feature boundary
        prox_data = data_2d.get(DriverKey.WATER_PROXIMITY)
        theme_ids = data_2d.get(DriverKey.THEME)

        if prox_data is None or theme_ids is None:
            return np.zeros(lib_ctx.target_shape, dtype="float32")

        # 2. Identify the target category from Config
        # Allows this function to work for 'Water', 'Forest', 'Playa', etc.
        target_label = params.get("label", name)
        target_id = lib_ctx.theme_registry.name_to_id.get(target_label.lower())

        if target_id is None:
            return np.zeros(lib_ctx.target_shape, dtype="float32")

        # 3. Create the binary gate (Where is this feature?)
        binary_mask = (theme_ids == target_id).astype("float32")

        # 4. Calculate the Alpha Ramp
        # ramp_width: The distance over which the feature goes from 0% to 100% opaque.
        ramp_width = float(params.get("ramp_width", 15.0))
        alpha = np.clip(prox_data / max(ramp_width, 0.1), 0.0, 1.0)

        # 5. Apply Non-linear Shaping (Power Curve)
        # Allows for 'silky' vs 'hard' transitions
        sensitivity = float(params.get("sensitivity", 1.0))
        if sensitivity != 1.0:
            alpha = np.power(alpha, 1.0 / max(sensitivity, 0.01))

        # Mask the alpha so it only exists inside the categorical boundary
        return alpha * binary_mask

    @staticmethod
    @spatial_factor("snow")
    def snow(data_2d, masks_2d, name, lib_ctx):
        # NOTE - this is going to get completely rewritten
        params = lib_ctx.cfg.get_logic("snow")
        raw_dem = data_2d[DriverKey.DEM]

        start = float(params["snowline"]) - float(params["ramp"])
        end = float(params["snowline"]) + float(params["ramp"])
        density = np.clip((raw_dem - start) / (end - start + 1e-6), 0.0, 1.0)
        noise = data_2d.get("noise", 0.5)
        return np.clip(((density - noise) * 1.0) + 0.5, 0.0, 1.0)


# -----------------------------------------------------------------------------
# Internal Helpers
# -----------------------------------------------------------------------------

def _get_required_factor(ctx, name):
    """
    Safely retrieves a previously computed factor.
    Provides high-fidelity error messages for dependency/sequence issues.
    """
    f = ctx.factors.get(name)  # Check the SimpleNamespace factors dict
    if f is not None:
        return np.squeeze(f)

    # If missing, investigate why to help the designer fix the pipeline
    # from ThematicRender.settings import FACTOR_SPECS
    all_defined = [s.name for s in FACTOR_SPECS]

    if name not in all_defined:
        raise KeyError(f"Factor Logic Error: '{name}' is used but not defined in settings.py.")
    else:
        raise KeyError(
            f"Factor Sequence Error: A factor tried to access '{name}', "
            f"but '{name}' hasn't been generated yet. Move '{name}' higher in FACTOR_SPECS."
        )
