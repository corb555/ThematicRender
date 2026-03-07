
from pathlib import Path
from typing import Dict, Any, Optional, Iterable,  Final

import numpy as np
from scipy.interpolate import interp1d

from ThematicRender.color_config import ColorConfig
from ThematicRender.color_ramp_hsv import get_ramp_from_yml
from ThematicRender.config_mgr import ConfigMgr
from ThematicRender.keys import RequiredResources, SurfaceKey, FileKey, DriverKey
from ThematicRender.noise_registry import NoiseRegistry
from ThematicRender.surface_library import SURFACE_PROVIDER_REGISTRY, MODIFIER_REGISTRY
from ThematicRender.ipc_blocks import Window

EXPECTED_BANDS = 3
OPAQUE_ALPHA: Final[int] = 255

def strip_alpha_or_fail(colors: np.ndarray, *, context: str) -> np.ndarray:
    """Normalize a ramp color table to RGB."""
    if colors.ndim != 2 or colors.shape[1] not in (3, 4):
        raise ValueError(f"{context}: expected colors (N,3) or (N,4), got {colors.shape}.")

    if colors.shape[1] == 3:
        return colors[:, :3]

    alpha_i = np.round(colors[:, 3]).astype("int64", copy=False)
    if np.any(alpha_i != OPAQUE_ALPHA):
        bad_idx = np.where(alpha_i != OPAQUE_ALPHA)[0][0]
        raise ValueError(
            f"{context}: non-opaque alpha at row {bad_idx} (val={alpha_i[bad_idx]}). "
            f"Surfaces must be RGB; move opacity into factors."
        )
    return colors[:, :3]

class SurfaceEngine:
    def __init__(self, cfg: ConfigMgr):
        self.cfg = cfg
        self.target_shape = None

        # Runtime cache for scipy interpolators
        self.surfaces: Dict[SurfaceKey, interp1d] = {}
        self.ramp_files: Dict[str, Path] = {}

        # Load registry from settings
        from ThematicRender.settings import SURFACE_SPECS
        self.spec_registry = {s.key: s for s in SURFACE_SPECS}

    def generate_surface_blocks(
            self, val_2d: dict, vld_2d: dict, factors_2d: dict,
            style_engine: Any, manifest: Iterable[SurfaceKey],
            noises: NoiseRegistry, window: Window, anchor_key: DriverKey
    ) -> Dict[SurfaceKey, np.ndarray]:
        """
        Synthesizes surface blocks following the 2D Firewall Contract.
        Returns a dict of (H, W, 3) float32 arrays.
        """
        # 1. Determine Master Geometry from the 2D anchor
        anchor_data = val_2d.get(anchor_key)
        if anchor_data is None:
            raise KeyError(f"Surface Engine: Anchor '{anchor_key}' not found in val_2d.")

        target_h, target_w = anchor_data.shape[:2]
        self.target_shape = (target_h, target_w)

        res = {}
        for skey in manifest:
            spec = self.spec_registry.get(skey)
            if not spec:
                continue

            provider_fn = SURFACE_PROVIDER_REGISTRY.get(spec.provider_id)
            if not provider_fn:
                raise ValueError(f"Unknown provider '{spec.provider_id}' for surface {skey}")

            try:
                # --- STAGE 1: CREATION ---
                # Providers receive clean 2D dicts.
                # Contract: returns (H, W, 3) float32.
                block = provider_fn(self, spec, val_2d, vld_2d, factors_2d, style_engine)

                if block.shape != (target_h, target_w, 3):
                    raise ValueError(
                        f"Contract Violation in {skey}: Expected ({target_h}, {target_w}, 3), "
                        f"got {block.shape}"
                    )
                res[skey] = block

            except Exception as e:
                print(f"\n❌ Surface Engine Error: [{skey.value}]")
                raise e

        # --- STAGE 2: MODIFICATION ---
        # Apply the chain of modifiers (Mottle, etc)
        res = self.apply_surface_modifiers(res, manifest, noises, window)

        return res

    def apply_surface_modifiers(
            self, surface_blocks: Dict[SurfaceKey, np.ndarray], manifest: Iterable[SurfaceKey],
            noises: NoiseRegistry, window: Window
    ) -> Dict[SurfaceKey, np.ndarray]:
        """Executes the modifier chain for each surface using SHM-safe noise lookup."""
        from ThematicRender.settings import SURFACE_MODIFIER_PROFILES

        for skey in manifest:
            spec = self.spec_registry.get(skey)
            if not spec or not spec.modifiers:
                continue

            for mod_cfg in spec.modifiers:
                mod_id = mod_cfg.get("id")
                profile_id = mod_cfg.get("profile_id")

                mod_fn = MODIFIER_REGISTRY.get(mod_id)
                profile = SURFACE_MODIFIER_PROFILES.get(profile_id)

                if not mod_fn or not profile:
                    continue

                # Coordinate Alignment: Logic handled in render_task compute_window logic
                # We sample noise at the specific size of the current surface block
                img_block = surface_blocks[skey]
                h_buf, w_buf = img_block.shape[:2]

                # Fetch Noise (Contract: window_noise returns H,W,1)
                noise_provider = noises.get(profile.noise_id)
                offset = hash(skey.value) % 1000
                noise_tile = noise_provider.window_noise(window, row_off=offset, col_off=offset)

                # Library math: (H,W,3) op (H,W,1)
                surface_blocks[skey] = mod_fn(img_block, noise_tile, profile)

        return surface_blocks

    def load_surface_ramps(self, resources: RequiredResources, output_dir: Optional[str] = None):
        """Initializes interpolators for all ramp-based surfaces."""
        out_dir = Path(output_dir) if output_dir else self._default_ramp_output_dir()
        out_dir.mkdir(parents=True, exist_ok=True)

        # Clear previous state
        self.ramp_files.clear()
        self.surfaces.clear()

        print("🔓 Initializing Surface Ramps...")

        # 1. Resolve ramps_yml path using new ConfigMgr path() accessor
        ramps_yml_path = self.cfg.path(FileKey.RAMPS_YML.value)

        # 2. Process Primary Pivot first (derived ramps depend on this)
        primary_path = None
        if resources.primary_surface:
            self._load_and_interpolate(resources.primary_surface, None, ramps_yml_path, out_dir)
            primary_path = self.ramp_files.get(resources.primary_surface.value)

        # 3. Load all required external surfaces
        for skey in resources.surface_inputs:
            if skey == resources.primary_surface:
                continue

            spec = self.spec_registry.get(skey)
            if spec is None:
                available = list(self.spec_registry.keys())
                raise ValueError(f"Missing SurfaceSpec for required input '{skey}'. Available: {available}")

            # Only 'ramp' providers need a scipy interpolator
            if spec.provider_id != "ramp":
                continue

            # Check if an explicit file exists in the resolved config paths
            # If not, it will be derived from the primary_path
            is_explicit = self.cfg.path(skey.value) is not None
            base_path = primary_path if not is_explicit else None

            self._load_and_interpolate(skey, base_path, ramps_yml_path, out_dir)

        return dict(self.ramp_files)

    def _load_and_interpolate(self, skey, base_path, ramps_yml_path, out_dir):
        """Helper to resolve a ramp file and build the scipy interpolator."""
        spec = self.spec_registry[skey]
        yaml_name = f"{skey.value}_color_ramp"

        mode, ramp_path = self._resolve_ramp_file(
            surface_key=skey,
            yaml_name=yaml_name,
            base_ramp_path=base_path,
            ramp_yml_path=ramps_yml_path,
            output_dir=out_dir
        )

        if ramp_path is None or not ramp_path.exists():
            raise FileNotFoundError(f"Ramp file for {skey.value} could not be resolved at {ramp_path}")

        print(f"   🔹 {skey.value.ljust(15)} <- {ramp_path.name}")

        # 1. Parse and build the scipy function
        z, c = ColorConfig.parse_ramp(str(ramp_path))
        c_rgb = strip_alpha_or_fail(c, context=f"surface ramp {skey.value}")

        self.surfaces[skey] = interp1d(z, c_rgb, axis=0, fill_value="extrapolate", kind="linear")
        self.ramp_files[skey.value] = ramp_path

    def _resolve_ramp_file(self, *, surface_key, yaml_name, base_ramp_path, ramp_yml_path, output_dir):
        """Resolves path using ConfigMgr or derives a new one using ramps_yml."""
        # Check ConfigMgr for an explicit file path provided by user
        explicit_path = self.cfg.path(surface_key.value)

        if explicit_path and explicit_path.exists():
            return "file", explicit_path

        # Otherwise, attempt to derive using the color_ramp_hsv logic
        if ramp_yml_path is None:
            raise ValueError(f"Cannot derive ramp for '{surface_key}': ramps_yml path not provided.")

        out_path = output_dir / f"gen_{yaml_name}.txt"
        try:
            mode, fname = get_ramp_from_yml(
                ramp_name=yaml_name,
                ramps_yml_settings=str(ramp_yml_path),
                base_ramp=str(base_ramp_path) if base_ramp_path else None,
                output_path=str(out_path)
            )
            return mode, Path(fname)
        except Exception as e:
            raise ValueError(f"Failed to derive ramp '{surface_key.value}': {e}")

    def _default_ramp_output_dir(self) -> Path:
        """Finds a safe place to dump derived text files."""
        out_path = self.cfg.path("output")
        if out_path:
            return out_path.parent
        return Path.cwd()