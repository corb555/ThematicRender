import hashlib
from pathlib import Path
from typing import Dict, Any, Optional, Iterable, Final

import numpy as np
from rasterio.windows import Window
from scipy.interpolate import interp1d

from Common.keys import RequiredResources, SurfaceKey, FileKey, DriverKey
from Render.color_config import ColorConfig
from Render.color_ramp_hsv import get_ramp_from_yml
from Render.noise_library import NoiseLibrary
from Render.render_config import RenderConfig
from Render.surface_library import SURFACE_PROVIDER_REGISTRY, MODIFIER_REGISTRY, SurfaceContext

# surface_engine.py
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
    def __init__(self, cfg: RenderConfig):
        self.cfg = cfg
        self.target_shape = None

        # Runtime cache for scipy interpolators
        self.surfaces: Dict[SurfaceKey, interp1d] = {}
        self.ramp_files: Dict[str, Path] = {}

        # Load registry from settings
        self.spec_registry = {s.key: s for s in cfg.surfaces}

        # --- DETERMINISTIC OFFSET CACHE ---
        # We calculate this once. Every worker process can use these to offset random effects
        self._offset_cache: Dict[SurfaceKey, int] = {}
        for skey in self.spec_registry.keys():
            seed_bytes = skey.encode('utf-8')
            stable_hash = hashlib.md5(seed_bytes).hexdigest()
            # Convert to an int and store it
            self._offset_cache[skey] = int(stable_hash[:8], 16) % 1000

    def generate_surface_blocks(
            self, data_2d: dict, masks_2d: dict, factors_2d: dict, style_engine: Any,
            surface_inputs: Iterable[SurfaceKey], noises: NoiseLibrary, window: Window,
            anchor_key: DriverKey
    ) -> Dict[SurfaceKey, np.ndarray]:
        """
        Synthesizes the required RGB surfaces for the current tile.
        The comp engine will use comp_ops and factors to combine these into the final result
        """
        # Establish master geometry from the anchor
        anchor_data = data_2d.get(anchor_key)
        if anchor_data is None:
            raise KeyError(f"Surface Engine: Anchor '{anchor_key}' not found in data_2d.")

        target_h, target_w = anchor_data.shape[:2]
        self.target_shape = (target_h, target_w)

        ctx = SurfaceContext(
            cfg=self.cfg, noises=noises, window=window, surfaces=self.surfaces,
            target_shape=self.target_shape
        )

        rendered_surfaces = {}
        for srf_key in surface_inputs:
            spec = self.spec_registry.get(srf_key)
            if spec is None:
                available = list(self.spec_registry.keys())
                raise KeyError(
                    f"Surface Engine: Required surface '{srf_key}' not found in registry. "
                    f"Check your SURFACE_SPECS definition. Available: {available}"
                )

            provider_fn = SURFACE_PROVIDER_REGISTRY.get(spec.provider_id)
            if not provider_fn:
                available = list(SURFACE_PROVIDER_REGISTRY.keys())
                raise ValueError(
                    f"Unknown provider '{spec.provider_id}' for surface {srf_key}. Available: "
                    f"{available}"
                )

            try:
                # --- STAGE 1: SYNTHESIS ---
                # Generate the base RGB block from the provider (Ramp/Theme/etc)
                block = provider_fn(ctx, spec, data_2d, masks_2d, factors_2d, style_engine)

                # --- STAGE 2: MODIFICATION ---
                # Apply procedural textures if defined in config
                if spec.modifiers:
                    block = self._apply_modifiers(srf_key, spec, block, noises, window)

                # Validation and storage
                if block.shape != (target_h, target_w, 3):
                    raise ValueError(f"Shape mismatch in {srf_key}")

                rendered_surfaces[srf_key] = block

            except Exception as e:
                print(f"\n❌ Surface Engine Error: [{srf_key}]")
                raise e

        return rendered_surfaces

    def _apply_modifiers(
            self, srf_key: SurfaceKey, spec: Any, img_block: np.ndarray, noises: NoiseLibrary,
            window: Window
    ) -> np.ndarray:
        """
        Applies a sequence of transformations to a single RGB block.
        """
        for mod_cfg in spec.modifiers:
            mod_id = mod_cfg.get("id")
            profile_id = mod_cfg.get("profile_id")

            mod_fn = MODIFIER_REGISTRY.get(mod_id)
            profile = self.cfg.modifiers.get(profile_id)

            if not mod_fn or not profile:
                continue

            # Identify noise source and fetch deterministic spatial offset
            noise_provider = noises.get(profile.noise_id)
            offset = self._offset_cache.get(srf_key, 0)

            # Sample noise using global coordinates (prevents tile seams)
            noise_tile = noise_provider.window_noise(window, row_off=offset, col_off=offset)

            # Transform the block
            img_block = mod_fn(img_block, noise_tile, profile)

        return img_block

    def load_surface_ramps(self, resources: RequiredResources, output_dir: Optional[str] = None):
        """Initializes interpolators for all ramp-based surfaces."""
        out_dir = Path(output_dir) if output_dir else self._default_ramp_output_dir()
        out_dir.mkdir(parents=True, exist_ok=True)

        # Clear previous state
        self.ramp_files.clear()
        self.surfaces.clear()

        # print("🔓 Initializing Surface Ramps...")

        ramps_yml_path = self.cfg.path(FileKey.RAMPS_YML)

        # 2. Process Primary Pivot first (derived ramps depend on this)
        primary_path = None
        if resources.primary_surface:
            self._load_and_interpolate(resources.primary_surface, None, ramps_yml_path, out_dir)
            primary_path = self.ramp_files.get(resources.primary_surface)

        # 3. Load all required external surfaces
        for skey in resources.surface_inputs:
            if skey == resources.primary_surface:
                continue

            spec = self.spec_registry.get(skey)
            if spec is None:
                available = list(self.spec_registry.keys())
                raise ValueError(
                    f"Missing SurfaceSpec for required input '{skey}'. Available: {available}"
                )

            # Only 'ramp' providers need a scipy interpolator
            if spec.provider_id != "ramp":
                continue

            # Check if an explicit file exists in the resolved config paths
            # If not, it will be derived from the primary_path
            is_explicit = self.cfg.path(skey) is not None
            base_path = primary_path if not is_explicit else None

            self._load_and_interpolate(skey, base_path, ramps_yml_path, out_dir)

        return dict(self.ramp_files)

    def _load_and_interpolate(self, skey, base_path, ramps_yml_path, out_dir):
        """Helper to resolve a ramp file and build the scipy interpolator."""
        yaml_name = f"{skey}_color_ramp"

        mode, ramp_path = self._resolve_ramp_file(
            skey=skey, yaml_name=yaml_name, base_ramp_path=base_path, ramp_yml_path=ramps_yml_path,
            output_dir=out_dir
        )

        if ramp_path is None or not ramp_path.exists():
            raise FileNotFoundError(
                f"Ramp file for {skey} could not be resolved at {ramp_path}"
            )

        # print(f"   🔹 {skey.ljust(15)} <- {ramp_path.name}")

        # 1. Parse and build the scipy function
        z, c = ColorConfig.parse_ramp(str(ramp_path))
        c_rgb = strip_alpha_or_fail(c, context=f"surface ramp {skey}")

        self.surfaces[skey] = interp1d(z, c_rgb, axis=0, fill_value="extrapolate", kind="linear")
        self.ramp_files[skey] = ramp_path

    def _resolve_ramp_file(
            self, *, skey, yaml_name, base_ramp_path, ramp_yml_path, output_dir
    ):
        """Resolves path using ConfigMgr or derives a new one using ramps_yml."""
        # Check ConfigMgr for an explicit file path provided by user
        explicit_path = self.cfg.path(skey)

        if explicit_path and explicit_path.exists():
            return "file", explicit_path

        # Otherwise, attempt to derive using the color_ramp_hsv logic
        if ramp_yml_path is None:
            raise ValueError(
                f"Cannot derive ramp for '{skey}': ramps_yml path not provided."
            )

        out_path = output_dir / f"gen_{yaml_name}.txt"
        try:
            mode, fname = get_ramp_from_yml(
                ramp_name=yaml_name, ramps_yml_settings=str(ramp_yml_path),
                base_ramp=str(base_ramp_path) if base_ramp_path else None, output_path=str(out_path)
            )
            return mode, Path(fname)
        except Exception as e:
            raise ValueError(f"Failed to derive ramp '{skey}': {e}")

    def _default_ramp_output_dir(self) -> Path:
        """Finds a safe place to dump derived text files."""
        out_path = self.cfg.path("output")
        if out_path:
            return out_path.parent
        return Path.cwd()
