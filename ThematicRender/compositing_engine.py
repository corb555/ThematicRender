
from typing import Dict, List, Any

import numpy as np

from ThematicRender.compositing_library import COMPOSITING_REGISTRY

#compositing_engine.py

class CompositingEngine:
    def __init__(self, tmr: Any):
        self.tmr = tmr
        self.target_shape = None # (H, W)

    def blend_window(
            self, surfaces: Dict[Any, np.ndarray], factors: Dict[str, np.ndarray],
            pipeline: List[Any]
    ) -> np.ndarray:
        """
        Orchestrates the compositing chain and enforces the (H, W, 3) spatial contract.
        """
        if not surfaces:
            raise ValueError("Compositing Engine: No surfaces provided to blend.")

        # 1. LOCK SPATIAL TRUTH
        first_surf = next(iter(surfaces.values()))
        self.target_shape = first_surf.shape[:2]
        h, w = self.target_shape

        active_surfaces = dict(surfaces)
        buffers: Dict[str, np.ndarray] = {}

        # 2. EXECUTE PIPELINE
        for i, spec in enumerate(pipeline):
            if not spec.enabled:
                continue

            op_meta = COMPOSITING_REGISTRY.get(spec.comp_op)
            if not op_meta:
                raise ValueError(f"Step {i}: Unknown comp_op '{spec.comp_op}'")

            self.tmr.start(f"    {spec.factor_nm or 'none'}:{spec.comp_op}")

            # 3. STANDARDIZE FACTOR (H, W, 1)
            factor = None
            if spec.factor_nm:
                factor = factors.get(spec.factor_nm)
                if factor is None:
                    # Provide context for missing factors
                    available = list(factors.keys())
                    raise KeyError(f"Step {i}: Factor '{spec.factor_nm}' missing. Available: {available}")

                # Enforce (H, W, 1) and apply Signal Shaping
                if factor.ndim == 2:
                    factor = factor[..., np.newaxis]

                # 1. SCALE
                if spec.scale != 1.0:
                    factor = factor * spec.scale

                # 2. BIAS
                if spec.bias != 0.0:
                    factor = np.clip(factor + spec.bias, 0.0, 1.0)

                # 3. CONTRAST
                if spec.contrast != 0.0:
                    gain = 1.0 + (spec.contrast * 10.0)
                    factor = np.clip((factor - 0.5) * gain + 0.5, 0.0, 1.0)

            # 4. DISPATCH TO LIBRARY
            try:
                op_meta.func(
                    buffers=buffers, surfaces=active_surfaces, factors=factors,
                    factor=factor, spec=spec, ctx=self
                )
            except Exception as e:
                # --- ENHANCED DEBUG PRINTOUT ---
                print(f"\n")
                print(f"❌ BLEND WINDOW ERROR: (check xx_describe.md for details)")
                print(f"Error: {e}")
                print(f"Pipeline Index:  {i}")
                print(f"Operation:   {spec.comp_op}")
                print(f"Description: {spec.desc}")
                print(f"-"*40)
                print(f"Factor:      {spec.factor_nm or 'None'}")
                print(f"Signal:      Scale={spec.scale} | Bias={spec.bias} | Contrast={spec.contrast}")

                # Format surface keys (handling SurfaceKey Enums)
                s_keys = ", ".join([str(s.value) if hasattr(s, 'value') else str(s) for s in spec.input_surfaces])
                print(f"Surfaces In: {s_keys or 'None'}")

                # Check for output/buffer targets
                target = getattr(spec, 'output_surface', getattr(spec, 'buffer', 'N/A'))
                print(f"Target:      {target.value if hasattr(target, 'value') else target}")

                # List currently initialized buffers
                print(f"Active Buffers: {list(buffers.keys())}")
                print(f"-"*40)
                raise e
            finally:
                self.tmr.end()

        # 5. FINALIZE AND TRANSPOSE
        # Extract result from standard keys using explicit None checks
        final_img = buffers.get("__final_output__")

        if final_img is None:
            # Fallback to canvas if no explicit write_output was called
            final_img = buffers.get("canvas")

        if final_img is None:
            # Provide a helpful error message listing what WAS created
            available = list(buffers.keys())
            raise ValueError(
                f"Pipeline produced no output. No '__final_output__' or 'canvas' buffer found. "
                f"Buffers available: {available}"
            )

        # We round and clip here to prevent data wrapping in the writer
        return np.round(final_img.transpose(2, 0, 1)).clip(0, 255).astype("uint8")