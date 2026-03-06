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

@warnings.deprecated("deprecated")
@spatial_surface("ZZramp")
def ZZ_ramp_provider(ctx, spec, val_2d, vld_2d, factors_2d, style_engine):
    f_id = spec.coord_factor
    factor_val = factors_2d.get(f_id) # This is  Raw Meters

    interp_func = ctx.surfaces.get(spec.key)
    u_min, u_max = float(interp_func.x[0]), float(interp_func.x[-1])

    # If factor is 1261.89, we sample the ramp at 1261.89.
    # Clip only to ensure we don't extrapolate past the file bounds.
    coords = np.clip(factor_val, u_min, u_max)

    return interp_func(coords)

@warnings.deprecated("deprecated")
@spatial_surface("ZZZramp")
def ZZZ_ramp_provider(ctx, spec, val_2d, vld_2d, factors_2d, style_engine):
    """
    Samples a 1D color ramp.
    """
    f_id = spec.coord_factor
    factor_2d = factors_2d.get(f_id)

    if factor_2d is None:
        raise KeyError(
            f"Surface '{spec.key.value}' requires factor '{f_id}'. "
            f"Available factors: {list(factors_2d.keys())}"
        )

    if factor_2d.ndim != 2:
        raise ValueError(
            f"Surface '{spec.key.value}' expects factor '{f_id}' to be 2D (H,W), "
            f"got shape={getattr(factor_2d, 'shape', None)}"
        )

    interp_func = ctx.surfaces.get(spec.key)
    if interp_func is None:
        raise KeyError(
            f"SurfaceKey '{spec.key}' not found in loaded ramps. "
            f"Loaded: {list(ctx.surfaces.keys())}"
        )

    u_min, u_max = float(interp_func.x[0]), float(interp_func.x[-1])
    if not np.isfinite(u_min) or not np.isfinite(u_max) or abs(u_max - u_min) < 1e-9:
        raise ValueError(
            f"Ramp '{spec.key.value}' has invalid domain: u_min={u_min}, u_max={u_max}"
        )

    # ---- Debug stats  ----
    try:
        fmin = float(np.nanmin(factor_2d))
        fmax = float(np.nanmax(factor_2d))
        fmean = float(np.nanmean(factor_2d))
        # how much will clip?
        clip_lo = float(np.mean(factor_2d <= 0.0))
        clip_hi = float(np.mean(factor_2d >= 1.0))
        print_once(
            f"ramp_stats_{spec.key.value}",
            f" [RAMP] {spec.key.value:<16} factor='{f_id}' "
            f"min={fmin:.4f} max={fmax:.4f} mean={fmean:.4f} "
            f"clip<=0:{clip_lo:.3f} clip>=1:{clip_hi:.3f} "
            f"u_min={u_min:.2f} u_max={u_max:.2f}"
        )

        # Optional strict tripwire: flat factor means flat ramp
        if  abs(fmax - fmin) < 1e-4:
            raise ValueError(
                f"Surface '{spec.key.value}' factor '{f_id}' is nearly constant "
                f"(min={fmin}, max={fmax}). Ramp output will look flat."
            )
    except Exception:
        # Don’t let debug code hide real errors; re-raise if strict
        if ctx.cfg.get("strict_pipeline", False):
            raise

    # Map factor 0..1 -> ramp x-domain
    coords = u_min + (factor_2d * (u_max - u_min))

    # interp1d(2D_array) returns (H, W, 3)
    res = interp_func(np.clip(coords, u_min, u_max))

    # Validate output shape
    if res.ndim != 3 or res.shape[:2] != factor_2d.shape or res.shape[2] != 3:
        raise ValueError(
            f"Ramp '{spec.key.value}' returned unexpected shape {res.shape}; "
            f"expected (H,W,3) with H,W={factor_2d.shape}"
        )

    stats_once("ramp_provider humid_base", res)

    return res.astype("float32", copy=False)


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