from dataclasses import dataclass
from functools import wraps
from typing import Callable, Dict, Any, Tuple

import numpy as np
from rasterio.windows import Window
from scipy.interpolate import interp1d

# surface_library.py
SURFACE_PROVIDER_REGISTRY: Dict[str, Callable] = {}
MODIFIER_REGISTRY: Dict[str, Callable] = {}


@dataclass(frozen=True, slots=True)
class SurfaceContext:
    """Explicit contract for Surface Provider functions."""
    cfg: Any  # RenderConfig
    noises: Any  # NoiseLibrary (already attached to SHM)
    window: Window  # Current tile window
    surfaces: Dict[Any, interp1d]  # Pre-calculated color ramps
    target_shape: Tuple[int, int]  # (H, W) for the current tile


def spatial_surface(provider_id: str):
    """
    Updated Decorator Contract:
    Receives 6 arguments (ctx, spec, data_2d, masks_2d, factors_2d, style_engine)
    Enforces (H, W, 3) float32 output.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(ctx, spec, data_2d, masks_2d, factors_2d, style_engine):
            # Pass all 6 arguments to the underlying provider function
            res = func(ctx, spec, data_2d, masks_2d, factors_2d, style_engine)

            if res is None:
                return np.zeros((*ctx.target_shape, 3), dtype="float32")

            # Coerce to RGB if needed
            if res.ndim == 2:
                res = np.stack([res] * 3, axis=-1)

            return res.astype("float32", copy=False)

        SURFACE_PROVIDER_REGISTRY[provider_id] = wrapper
        return wrapper

    return decorator

@spatial_surface("theme")
def _theme_provider(ctx: SurfaceContext, spec, data_2d, masks_2d, factors_2d, style_engine):
    """Generate the theme surface for the current tile."""
    theme_ids = data_2d.get(spec.driver)

    if theme_ids is None:
        available_keys = list(data_2d.keys())
        raise ValueError(
            f"Theme Provider: Driver '{spec.driver}' ({type(spec.driver)}) not found. "
            f"Available keys: {available_keys}"
        )

    smoothed_ids = style_engine.get_smoothed_ids(theme_ids)
    tile_ctx = style_engine.build_tile_context(smoothed_ids)

    return style_engine.get_theme_surface(
        smoothed_ids,
        ctx,
        tile_ctx=tile_ctx,
    )

@spatial_surface("ramp")
def _ramp_provider(ctx: SurfaceContext, spec, data_2d, masks_2d, factors_2d, style_engine):
    # This factor is "elev_m" (Raw Meters)
    f_id = spec.coord_factor
    factor_val = factors_2d.get(f_id)

    interp_func = ctx.surfaces.get(spec.key)
    if interp_func is None:
        print(f" Key='{spec.key}' Surfaces: {ctx.surfaces}")
        print(ctx)
    u_min, u_max = float(interp_func.x[0]), float(interp_func.x[-1])

    coords = np.clip(factor_val, u_min, u_max)
    return interp_func(coords)



def register_modifier(mod_id: str):
    def decorator(func):
        MODIFIER_REGISTRY[mod_id] = func
        return func

    return decorator


@register_modifier("mottle")
def _mottle_modifier(img_block: np.ndarray, noise: np.ndarray, profile: Any) -> np.ndarray:
    """
    Standard Mottle: Centered at 0 to provide dark and light variation.
    """
    # Shift noise from [0.0, 1.0] to [-0.5, 0.5]
    centered_noise = noise - 0.5

    # Calculate RGB shift
    shift = (centered_noise * np.array(profile.shift_vector, dtype="float32")) * profile.intensity

    # Apply and clip to valid 8-bit color range
    return np.clip(img_block + shift, 0, 255)
