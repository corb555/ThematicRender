from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

import numpy as np


#compositing_library.py

@dataclass(slots=True)
class OpMetadata:
    func: Callable
    # Attributes required on the _BlendSpec object (e.g., 'buffer', 'input_surfaces')
    required_attrs: List[str]
    # Keys required inside the spec.params dictionary (e.g., 'color')
    required_params: List[str]
    description: str

COMPOSITING_REGISTRY: Dict[str, OpMetadata] = {}

def register_op(name: str, required_attrs: List[str] = None, required_params: List[str] = None):
    def decorator(func):
        COMPOSITING_REGISTRY[name] = OpMetadata(
            func=func,
            required_attrs=required_attrs or [],
            required_params=required_params or [],
            description=func.__doc__ or ""
        )
        return func
    return decorator

class CompositingLibrary:
    """
    Library of atomic spatial operations.
    Inputs:
        surfaces/buffers: Guaranteed (H, W, 3)
        factor: Guaranteed (H, W, 1)
    """

    @staticmethod
    @register_op("create_buffer", required_attrs=["buffer", "input_surfaces"])
    def create_buffer(buffers, surfaces, factors, factor, spec, ctx):
        src = surfaces.get(spec.input_surfaces[0])
        _validate_spatial(src, spec.input_surfaces[0], ctx.target_shape)
        buffers[spec.buffer] = src.copy()

    @staticmethod
    @register_op("lerp_surfaces", required_attrs=["output_surface", "input_surfaces", "factor_nm"])
    def lerp_surfaces(buffers, surfaces, factors, factor, spec, ctx):
        p_a = surfaces.get(spec.input_surfaces[0])
        p_b = surfaces.get(spec.input_surfaces[1])
        _validate_spatial(p_a, spec.input_surfaces[0], ctx.target_shape)
        _validate_spatial(p_b, spec.input_surfaces[1], ctx.target_shape)

        # (H, W, 3) blended by (H, W, 1)
        surfaces[spec.output_surface] = p_a + factor * (p_b - p_a)

    @staticmethod
    @register_op("lerp", required_attrs=["buffer", "input_surfaces", "factor_nm"])
    def lerp_surface_to_buffer(buffers, surfaces, factors, factor, spec, ctx):
        target_rgb = surfaces.get(spec.input_surfaces[0])
        current = buffers.get(spec.buffer)

        _validate_spatial(target_rgb, spec.input_surfaces[0], ctx.target_shape)
        _validate_spatial(current, spec.buffer, ctx.target_shape)

        # In-place buffer update
        buffers[spec.buffer] = current + factor * (target_rgb - current)

    @staticmethod
    @register_op("multiply", required_attrs=["buffer", "factor_nm"])
    def multiply_op(buffers, surfaces, factors, factor, spec, ctx):
        current = buffers.get(spec.buffer)
        _validate_spatial(current, spec.buffer, ctx.target_shape)

        # (H, W, 3) * (H, W, 1)
        buffers[spec.buffer] = np.clip(current * factor, 0, 255)

    @staticmethod
    @register_op("alpha_over", required_attrs=["buffer", "input_surfaces", "factor_nm"])
    def alpha_over_op(buffers, surfaces, factors, factor, spec, ctx):
        under = buffers.get(spec.buffer)
        over = surfaces.get(spec.input_surfaces[0])

        _validate_spatial(under, spec.buffer, ctx.target_shape)
        _validate_spatial(over, spec.input_surfaces[0], ctx.target_shape)

        a = factor # (H, W, 1)
        buffers[spec.buffer] = (over * a) + (under * (1.0 - a))

    @staticmethod
    @register_op("lerp_buffers", required_attrs=["buffer", "merge_buffer", "factor_nm"])
    def lerp_buffers(buffers, surfaces, factors, factor, spec, ctx):
        under = buffers.get(spec.buffer)
        over = buffers.get(spec.merge_buffer)

        _validate_spatial(under, spec.buffer, ctx.target_shape)
        _validate_spatial(over, spec.merge_buffer, ctx.target_shape)

        if factor is None:
            raise ValueError(f"lerp_buffers op requires a factor (missing '{spec.factor_nm}')")

        buffers[spec.buffer] = under + factor * (over - under)

    @staticmethod
    @register_op("add_specular_highlights", required_attrs=["buffer", "factor_nm"], required_params=["color"])
    def add_specular(buffers, surfaces, factors, factor, spec, ctx):
        current = buffers.get(spec.buffer)
        _validate_spatial(current, spec.buffer, ctx.target_shape)

        color = np.array(spec.params["color"], dtype="float32").reshape(1, 1, 3)
        intensity = float(spec.params.get("intensity", 1.0))

        # factor is (H, W, 1), color is (1, 1, 3)
        reflection = (factor * color) * intensity
        buffers[spec.buffer] = np.clip(current + reflection, 0, 255)

    @staticmethod
    @register_op("write_output", required_attrs=["buffer"])
    def write_output(buffers, surfaces, factors, factor, spec, ctx):
        src_name = spec.buffer
        if src_name not in buffers:
            raise KeyError(f"Write Output failed: Buffer '{src_name}' not found.")
        buffers["__final_output__"] = buffers[src_name]

# --- Internal Validation ---

def _validate_spatial(arr: np.ndarray, label: str, target_hw: tuple):
    if arr is None:
        raise ValueError(f"Required surface/buffer '{label}' is None.")
    if arr.shape[:2] != target_hw:
        raise ValueError(f"Spatial Mismatch: '{label}' is {arr.shape[:2]}, expected {target_hw}")
    if arr.ndim != 3 or arr.shape[2] != 3:
        # We allow 4 for RGBA but prefer 3 for this standard pipeline
        if arr.shape[2] != 4:
            raise ValueError(f"Channel Mismatch: '{label}' is {arr.shape}, expected (H, W, 3)")
# ---  Utilities ---

def alpha_over(under_rgb: np.ndarray, over_rgb: np.ndarray, over_a: np.ndarray) -> np.ndarray:
    u = under_rgb.astype(np.float32, copy=False)
    o = over_rgb.astype(np.float32, copy=False)
    a = over_a[..., None]  # HxWx1
    return (o * a) + (u * (1.0 - a))


def _require_buffer(buffers, key, context, spec):
    if key not in buffers:
        raise RuntimeError(f"❌ {context}: Buffer '{key}' not initialized.")
    return buffers[key]


def _validate_surface(surface, key, context, spec, allow_black=False):
    if surface is None:
        raise ValueError(f"❌ {context}: Surface '{key}' is None.")
    if not allow_black and float(np.max(surface)) <= 1e-6:
        raise ValueError(f"❌ {context}: Surface '{key}' is pure black/empty.")
