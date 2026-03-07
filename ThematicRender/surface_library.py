from functools import wraps
from typing import Callable, Dict, Any
import warnings

import numpy as np

from ThematicRender.utils import print_once, stats_once

#surface_library.py
# --- Surface Provider Registry ---
SURFACE_PROVIDER_REGISTRY: Dict[str, Callable] = {}


def spatial_surface(provider_id: str):
    """
    Updated Decorator Contract:
    Receives 6 arguments (ctx, spec, val_2d, vld_2d, factors_2d, style_engine)
    Enforces (H, W, 3) float32 output.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(ctx, spec, val_2d, vld_2d, factors_2d, style_engine):
            # Pass all 6 arguments to the underlying provider function
            res = func(ctx, spec, val_2d, vld_2d, factors_2d, style_engine)

            if res is None:
                return np.zeros((*ctx.target_shape, 3), dtype="float32")

            # Coerce to RGB if needed
            if res.ndim == 2:
                res = np.stack([res] * 3, axis=-1)

            return res.astype("float32", copy=False)

        SURFACE_PROVIDER_REGISTRY[provider_id] = wrapper
        return wrapper
    return decorator


@spatial_surface("ramp")
def _ramp_provider(ctx, spec, val_2d, vld_2d, factors_2d, style_engine):
    # This factor is "elev_m" (Raw Meters)
    f_id = spec.coord_factor
    factor_val = factors_2d.get(f_id)

    interp_func = ctx.surfaces.get(spec.key)
    u_min, u_max = float(interp_func.x[0]), float(interp_func.x[-1])

    coords = np.clip(factor_val, u_min, u_max)
    return interp_func(coords)

@spatial_surface("style")
def _style_provider(ctx, spec, val_2d, vld_2d, factors_2d, style_engine):
    """
    Fetches categorical RGB.
    NOTE: We pass val_2d to the style engine  instead of DriverBlocks.
    """
    # The Theme Engine (style_engine) must be updated to accept a dict
    # of arrays instead of a dict of objects.
    rgb_full = style_engine.get_theme_surface(val_2d, context=f"surface:{spec.key.value}")

    if rgb_full is None:
        return np.zeros((*ctx.target_shape, 3), dtype="float32")

    return rgb_full

MODIFIER_REGISTRY: Dict[str, Callable] = {}

def register_modifier(mod_id: str):
    def decorator(func):
        MODIFIER_REGISTRY[mod_id] = func
        return func
    return decorator

@register_modifier("mottle")
def _mottle_modifier(img_block: np.ndarray, noise: np.ndarray, profile: Any) -> np.ndarray:
    """
    Standard Mottle: (H,W,3) + ((H,W,1) * (3,))
    """
    # Contract: noise is guaranteed (H,W,1) by Engine.
    # shift_vector is (3,)
    shift = (noise * np.array(profile.shift_vector, dtype="float32")) * profile.intensity
    return np.clip(img_block + shift, 0, 255)