from typing import Dict, List, Any, Optional

import numpy as np

from Common.keys import SurfaceKey, _BlendSpec
from Render.compositing_library import COMPOSITING_REGISTRY


# compositing_engine.py
class CompositingEngine:
    """Orchestrates the sequential blending of surfaces and factors.

    This engine executes the 'Blend Pipeline' — a list of specified operations
    (Ops) that combine RGB color surfaces with alpha factors.
    """

    def __init__(self):
        """
        Args:
        """
        # self.tmr = timer
        self.target_shape = None  # (H, W) locked during first step

    def blend_window(
            self, surfaces: Dict[SurfaceKey, np.ndarray], factors: Dict[str, np.ndarray],
            pipeline: List[_BlendSpec]
    ) -> np.ndarray:
        """Executes the compositing pipeline for a single tile.

        Args:
            surfaces: Dictionary of synthesized RGB arrays (H, W, 3).
            factors: Dictionary of alpha masks (H, W, 1).
            pipeline: List of BlendSpecs defining the logic chain.

        Returns:
            A (3, H, W) uint8 array ready for the GeoTIFF writer.
        """
        if not surfaces:
            raise ValueError("Compositing Engine: No surfaces provided to blend.")

        # print(f"DEBUG [Compositor] Surfaces available: {list(surfaces.keys())}")
        # for key, srf in surfaces.items():
        #    if np.max(srf) > 0:
        #        print(f"DEBUG [Compositor] Surface {key} has signal (Max: {np.max(srf)})")
        #    else:
        #        print(f"DEBUG [Compositor] Surface {key} is EMPTY (All Zeros)")

        # 1. ESTABLISH SPATIAL GEOMETRY
        # All surfaces in this tile must share the same resolution.
        first_surf = next(iter(surfaces.values()))
        self.target_shape = first_surf.shape[:2]

        active_surfaces = dict(surfaces)
        buffers: Dict[str, np.ndarray] = {}

        # 2. EXECUTE PIPELINE STEPS
        for i, step in enumerate(pipeline):
            if not step.enabled:
                continue

            # Resolve the operator logic from the library registry
            operator = COMPOSITING_REGISTRY.get(step.comp_op)
            if not operator:
                raise ValueError(f"Step {i}: Unknown comp_op '{step.comp_op}'")

            # Instrumentation: Track timing per operation
            # self.tmr.start(f"    {step.factor_nm or 'none'}:{step.comp_op}")

            # 3. PREPARE THE SIGNAL - Fetch and apply signal shaping (Scale/Bias/Contrast)
            factor = self._condition_factor(step, factors, i)

            # 4. DISPATCH TO COMPOSITING LIBRARY
            try:
                operator.func(
                    buffers=buffers, surfaces=active_surfaces, factors=factors, factor=factor,
                    spec=step, ctx=self
                )
            except MemoryError as e:
                # Capture high-fidelity failure metadata
                self._log_pipeline_error(e, i, step, buffers)
                raise e

        # 5. FINALIZE FOR STORAGE
        # Extract the buffer designated by the 'write_output' op
        final_img = buffers.get("__final_output__")

        if final_img is None:
            print("blend image error")
            raise ValueError(
                "Pipeline produced no output. Ensure 'write_output' is enabled in biome.yml."
            )

        # ENGINE CONTRACT: Final conversion from (H, W, 3) float -> (3, H, W) uint8
        # We round and clip to prevent integer wrapping/overflow in the writer.
        return np.round(final_img.transpose(2, 0, 1)).clip(0, 255).astype("uint8")

    @staticmethod
    def _condition_factor(spec: _BlendSpec, factors: dict, step_idx: int) -> Optional[np.ndarray]:
        """Applies signal shaping math to the semantic factor."""
        if not spec.factor_nm:
            return None

        factor = factors.get(spec.factor_nm)
        if factor is None:
            available = list(factors.keys())
            raise KeyError(
                f"Step {step_idx}: Factor '{spec.factor_nm}' missing. Available: {available}"
            )

        # Enforce Firewall Standard (H, W, 1)
        if factor.ndim == 2:
            factor = factor[..., np.newaxis]

        # A. SCALE: Multiply intensity
        if spec.scale != 1.0:
            factor = factor * spec.scale

        # B. BIAS: Slide presence (and clip to protect range)
        if spec.bias != 0.0:
            factor = np.clip(factor + spec.bias, 0.0, 1.0)

        # C. CONTRAST: Sharpen gradients around the 0.5 pivot point
        if spec.contrast != 0.0:
            gain = 1.0 + (spec.contrast * 10.0)
            factor = np.clip((factor - 0.5) * gain + 0.5, 0.0, 1.0)

        return factor

    @staticmethod
    def _log_pipeline_error(e: Exception, index: int, spec: Any, buffers: dict) -> None:
        """
        Generates a high-fidelity diagnostic report for pipeline failures.
        """
        print(f"❌ BLEND PIPELINE ERROR")
        print(f"   (See xx_describe.md for full logic chain)")

        print(f"Error:        {e}")
        print(f"Step Index:   {index}")
        print(f"Operation:    {spec.comp_op}")
        print(f"Description:  {spec.desc}")
        print(f"-" * 40)

        # Signal telemetry
        print(f"Factor:       {spec.factor_nm or 'None'}")
        print(f"Signal:       Scale={spec.scale} | Bias={spec.bias} | Contrast={spec.contrast}")

        # Input Surface resolution
        srf_keys = ", ".join(
            [str(srf.value) if hasattr(srf, 'value') else str(srf) for srf in spec.input_surfaces]
        )
        print(f"Surfaces In:  {srf_keys or 'None'}")

        # Target resolution (Output Surface vs Buffer)
        target = getattr(spec, 'output_surface', getattr(spec, 'buffer', 'N/A'))
        target_val = target.value if hasattr(target, 'value') else target
        print(f"Target:       {target_val}")

        # Environment State
        print(f"Active Buffers: {list(buffers.keys())}")
        print("-" * 60 + "\n")
