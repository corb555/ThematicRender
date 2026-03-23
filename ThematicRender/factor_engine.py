from dataclasses import dataclass
from types import SimpleNamespace
from typing import Protocol, Callable, Mapping, Any, List

import numpy as np
from rasterio.windows import Window

from ThematicRender.utils import print_once


# factor_engine.py

class FactorEngine:
    def __init__(
            self, cfg: Any, themes: Any, noise_registry: Any, factor_specs: List[Any],
            render_resources: Any, timer
    ):
        self.cfg = cfg
        self.themes = themes
        self.noise_registry = noise_registry
        self.specs = list(factor_specs)
        self.render_resources = render_resources
        self.tmr = timer

        # Resolve compute callables
        self._compiled = []
        from ThematicRender.factor_library import FACTOR_REGISTRY
        for spec in self.specs:
            fn = FACTOR_REGISTRY.get(spec.function_id)
            if fn is None:
                available = sorted(FACTOR_REGISTRY.keys())

                raise KeyError(
                    f"\n❌ Factor Engine Initialization Error:\n"
                    f"   Factor '{spec.name}' requested an unknown function_id: '"
                    f"{spec.function_id}'.\n"
                    f"   Check your FACTOR_SPECS in settings.py or biome.yml for typos.\n"
                    f"   Available Function IDs: {available}"
                )

            # print(f"Registered factor {spec.name}")
            self._compiled.append((spec, fn))

    def generate_factors(
            self, data_2d: dict, masks_2d: dict, window: Window, anchor_key: Any
    ) -> dict:
        """
        Generates  factors (alpha masks) from raw data.

        The pipeline definition controls which factors to call from the factor library.
        The engine acts as the orchestrator, preparing the shared context and
        executing the specified factor library functions in order.
        """
        factors = {}

        # Use the anchor driver to establish the master spatial resolution for this tile
        target_h, target_w = data_2d[anchor_key].shape[:2]

        # Retrieve the set of factors required by the current pipeline definition
        required_factors = self.render_resources.factor_inputs

        # Create a different context object with different parameter names
        lib_ctx = SimpleNamespace(
            cfg=self.cfg, themes=self.themes, noises=self.noise_registry, window=window,
            data_2d=data_2d, masks_2d=masks_2d, factors=factors, target_shape=(target_h, target_w),
            anchor_key=anchor_key, tmr=self.tmr
        )

        for spec, fn in self._compiled:
            if spec.name not in required_factors:
                continue

            # print(f"GENERATE factor {spec.name}")

            # --- Ensure drivers setting exists for this factor ---
            if spec.name not in self.cfg.logic:
                raise KeyError(
                    f"\n❌ Config error: Factor '{spec.name}' has no drivers entry in config."
                )

            lib_ctx.spec = spec

            try:
                # global override for debugging - replaces factor with all ones
                override_target = self.cfg.get_global("override_factor")
                if override_target == spec.name:
                    res = np.ones((target_h, target_w, 1), dtype="float32")
                else:
                    # INVOKE LIBRARY:  call the external function defined in factor_library
                    res = fn(data_2d, masks_2d, spec.name, lib_ctx)

                if res is None:
                    raise ValueError(f"Factor library function {spec.name} returned None")

                # THE STORAGE CONTRACT: Convert 2D compute results into 3D (H, W, 1) semantic masks
                if res.ndim == 2:
                    res = res[..., np.newaxis]

                # VALIDATION: Ensure the library output aligns with the master tile geometry
                if res.shape != (target_h, target_w, 1):
                    raise ValueError(
                        f"Shape mismatch: Expected ({target_h}, {target_w}, 1), got {res.shape}"
                    )

                # Store the standardized factor for use in the compositor or by downstream factors
                factors[spec.name] = res.astype("float32")

            except MemoryError as e:
                raise ValueError(f"\n❌ Factor Engine Error: [{spec.name}] {e}")

        return factors

    def _debug_driver_stats(
            self, *, data_2d: dict, masks_2d: dict, driver_key: Any, name: str, ) -> None:
        """Print one-time debug stats for a driver array (DEM, etc.)."""
        if driver_key not in data_2d:
            print_once(f"missing_{name}", f"⚠️  [STATS] {name}: driver missing from data_2d")
            return

        arr = data_2d[driver_key]
        mask = masks_2d.get(driver_key)

        # Handle mask that might be (H,W,1)
        if mask is not None and getattr(mask, "ndim", 0) == 3:
            mask = mask[..., 0]

        a_min = float(np.nanmin(arr))
        a_max = float(np.nanmax(arr))
        a_mean = float(np.nanmean(arr))

        msg = (f"📦 [DRIVER] {name:<8} dtype={arr.dtype} shape={arr.shape} min={a_min:.2f} max="
               f"{a_max:.2f} mean={a_mean:.2f}")

        if mask is not None:
            v_mean = float(np.mean(mask))
            v_zeros = float(np.mean(mask <= 0.0))
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
