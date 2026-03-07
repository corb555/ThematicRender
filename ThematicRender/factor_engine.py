from dataclasses import dataclass
import sys
from types import SimpleNamespace
from typing import Protocol, Callable, Mapping, Any, List

import numpy as np
from rasterio.windows import Window

from ThematicRender.keys import DriverKey
from ThematicRender.utils import print_once


# factor_engine.py

class FactorEngine:
    def __init__(
            self, cfg: Any, themes: Any, noise_registry: Any, factor_specs: List[Any],
            resources: Any, timer
    ):
        self.cfg = cfg
        self.themes = themes
        self.noise_registry = noise_registry
        self.specs = list(factor_specs)
        self.resources = resources
        self.tmr = timer

        # Resolve compute callables
        self._compiled = []
        from ThematicRender.factor_library import FACTOR_REGISTRY
        for spec in self.specs:
            fn = FACTOR_REGISTRY.get(spec.function_id)
            if fn is None:
                raise KeyError(f"Factor function_id '{spec.function_id}' not found.")
            self._compiled.append((spec, fn))

    def generate_factors(self, val_2d: dict, vld_2d: dict, window: Window, anchor_key: Any) -> dict:
        """
        Receives 2D sanitized arrays.
        Outputs (H, W, 1) for the storage layer.
        """
        factors = {}

        # 1. Determine master dimensions from the 2D anchor
        target_h, target_w = val_2d[anchor_key].shape[:2]
        self._debug_driver_stats(val_2d=val_2d, vld_2d=vld_2d, driver_key=DriverKey.DEM, name="DEM")

        # 2. Context setup  uses 2D dicts
        ctx = SimpleNamespace(
            cfg=self.cfg,
            themes=self.themes,
            noises=self.noise_registry,
            window=window, # Global window for noise coordinates
            val_2d=val_2d, # Sanitized 2D values
            vld_2d=vld_2d, # Sanitized 2D validity masks
            factors=factors,
            target_shape=(target_h, target_w),
            anchor_key=anchor_key,
            tmr=self.tmr
        )

        required_factors = self.resources.factor_inputs

        for spec, fn in self._compiled:
            if spec.name not in required_factors:
                continue

            try:
                # --- CALL LIBRARY (2D COMPUTE) ---
                # The library function 'fn'  operates in a pure 2D environment
                override_target = self.cfg.get_global("override_factor")
                if override_target == spec.name:
                    res = np.ones((target_h, target_w, 1), dtype="float32")
                else:
                    #  run the math
                    res = fn(val_2d, vld_2d, spec.name, ctx)

                if res is None:
                    raise ValueError("Returned None")

                f_max = float(res.max())
                """  if f_max > 1e-5:
                    print_once(f"found_{spec.name}",
                               f"✨ [STATS] Factor {spec.name: <10} | "
                               f"min: {res.min():.4f} | max: {f_max:.4f} | "
                               f"mean: {res.mean():.4f}")"""

                # We add the band dimension ONCE here before storing.
                if res.ndim == 2:
                    res = res[..., np.newaxis]

                # Validation remains as a safety check for the Engine boundaries
                if res.shape != (target_h, target_w, 1):
                    raise ValueError(f"Shape mismatch: Expected ({target_h}, {target_w}, 1), got {res.shape}")

                factors[spec.name] = res.astype("float32")

            except MemoryError as e:
                raise ValueError(f"\n❌ Factor Engine Error: [{spec.name}] {e}")

        return factors

    def _debug_driver_stats(
            self,
            *,
            val_2d: dict,
            vld_2d: dict,
            driver_key: Any,
            name: str,
    ) -> None:
        """Print one-time debug stats for a driver array (DEM, etc.)."""
        if driver_key not in val_2d:
            print_once(f"missing_{name}", f"⚠️  [STATS] {name}: driver missing from val_2d")
            return

        arr = val_2d[driver_key]
        vld = vld_2d.get(driver_key)

        # Handle vld that might be (H,W,1)
        if vld is not None and getattr(vld, "ndim", 0) == 3:
            vld = vld[..., 0]

        a_min = float(np.nanmin(arr))
        a_max = float(np.nanmax(arr))
        a_mean = float(np.nanmean(arr))

        msg = f"📦 [DRIVER] {name:<8} dtype={arr.dtype} shape={arr.shape} min={a_min:.2f} max={a_max:.2f} mean={a_mean:.2f}"

        if vld is not None:
            v_mean = float(np.mean(vld))
            v_zeros = float(np.mean(vld <= 0.0))
            msg += f" | valid_mean={v_mean:.3f} valid_zeros={v_zeros:.3f}"

        print_once(f"driver_stats_{name}", msg)


class FactorContext(Protocol):
    cfg: object


FactorFn = Callable[[dict, dict, str, FactorContext], np.ndarray]


@dataclass(frozen=True, slots=True)
class FactorRegistry:
    """Maps factor  ids to implementations."""
    fns: Mapping[str, FactorFn]

    def get(self, function_id: str) -> FactorFn:
        key = (function_id or "").strip()
        if not key:
            raise ValueError("Factor function_id is empty.")
        fn = self.fns.get(key)
        if fn is None:
            available = ", ".join(sorted(self.fns.keys()))
            raise KeyError(
                f"Unknown factor function_id '{function_id}' in Factor Specs. Available: "
                f"{available}"
            )
        return fn
