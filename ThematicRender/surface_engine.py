from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional, Final, Dict, Iterable
import warnings

import numpy as np
from rasterio.windows import Window
from scipy.interpolate import interp1d

from ThematicRender.color_config import ColorConfig
from ThematicRender.color_ramp_hsv import get_ramp_from_yml
from ThematicRender.config_mgr import FileKey, SurfaceKey, ConfigMgr
from ThematicRender.keys import RequiredResources
from ThematicRender.noise_registry import NoiseRegistry
from ThematicRender.surface_library import SURFACE_PROVIDER_REGISTRY, MODIFIER_REGISTRY
from ThematicRender.utils import stats_once

# surface_engine.py

"""
Surface Registry: The map that connects a provider_id to its function.
Surface Block: The resulting (H, W, 3) RGB NumPy array produced by a provider.
Surface Variation (Mottle): A post-processing operation applied to a Surface Block to add visual 
grit/hue shifts before it reaches the blender.
SurfaceEngine: The class that manages the lifecycle of resources (interpolators) and the 
generation loop.
"""
@dataclass(frozen=True, slots=True)
class SurfacePlanItem:
    """Metadata for resolving/deriving a  ramp."""
    surface_key: SurfaceKey
    yaml_name: str

EXPECTED_BANDS = 3
ALPHA_COL = 3
ALPHA_OPAQUE = 255.0
ALPHA_TOL = 0.5  # allow tiny parse noise

OPAQUE_ALPHA: Final[int] = 255


def strip_alpha_or_fail(colors: np.ndarray, *, context: str) -> np.ndarray:
    """Normalize a ramp color table to RGB.

    Args:
        colors: Array shaped (N,3) or (N,4), integer-like in 0..255.
        context: Human-friendly label (file path, surface key, etc.) for errors.

    Returns:
        Array shaped (N,3) dtype float32 (or keep uint8 if you prefer).

    Raises:
        ValueError: If colors are not (N,3)/(N,4), or if RGBA alpha != 255.
    """
    if colors.ndim != 2 or colors.shape[1] not in (3, 4):
        raise ValueError(
            f"{context}: expected colors shaped (N,3) or (N,4), got {colors.shape}."
        )

    if colors.shape[1] == 3:
        return colors[:, :3]

    alpha = colors[:, 3]
    # allow float-ish inputs, but compare as integers after rounding
    alpha_i = np.round(alpha).astype("int64", copy=False)
    bad = np.where(alpha_i != OPAQUE_ALPHA)[0]
    if bad.size:
        i0 = int(bad[0])
        raise ValueError(
            f"{context}: RGBA ramp contains non-opaque alpha at row {i0} "
            f"(alpha={int(alpha_i[i0])}). Surfaces must be RGB-only; move opacity into factors."
        )

    return colors[:, :3]


class SurfaceEngine:
    def __init__(self, cfg:ConfigMgr):
        self.target_shape = None
        self.cfg = cfg
        # Map of SurfaceKey -> scipy interp1d function
        self.surfaces: Dict[SurfaceKey, interp1d] = {}

        # Map of surface_key.value -> Path on disk
        self.ramp_files: Dict[str, Path] = {}
        self.load_warnings: list[str] = []
        # Standard for RGB output; set to 4 if using RGBA palettes
        self.bands_count = 3

        # Load spec registry once at startup
        from ThematicRender.settings import SURFACE_SPECS
        self.spec_registry = {s.key: s for s in SURFACE_SPECS}

    def generate_surface_blocks(
            self, val_2d, vld_2d, factors_2d, style_engine, manifest, noises, window, anchor_key
    ) -> Dict[SurfaceKey, np.ndarray]:
        """
        Receives 2D values, 2D validity, and 2D factors.
        Outputs (H, W, 3) RGB surfaces.
        """
        target_h, target_w = val_2d[anchor_key].shape[:2]
        self.target_shape = (target_h, target_w)

        res = {}
        for skey in manifest:
            spec = self.spec_registry.get(skey)
            if not spec: continue

            provider_fn = SURFACE_PROVIDER_REGISTRY.get(spec.provider_id)

            try:
                # Providers  receive 2D data, making internal math safe
                block = provider_fn(self, spec, val_2d, vld_2d, factors_2d, style_engine)

                if block.shape != (target_h, target_w, 3):
                    raise ValueError(f"Shape mismatch: Expected ({target_h}, {target_w}, 3), got {block.shape}")

                res[skey] = block

            except Exception as e:
                print(f"\n❌ Surface Engine Error: [{skey.value}]")
                raise e

        # Pass 2D structures to modifiers
        res = self.apply_surface_modifiers(res, manifest, noises, window)
        stats_once("generate_surface_blocks humid_base", res[SurfaceKey.HUMID_BASE])
        return res


    def apply_surface_modifiers(
            self, surface_blocks: Dict[SurfaceKey, np.ndarray], manifest: Iterable[SurfaceKey],
            noises: "NoiseRegistry", window: "Window"
    ) -> Dict[SurfaceKey, np.ndarray]:
        """
        Engine Pass: Orchestrates the chain of modifiers for each surface.
        """
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

                # --- ENGINE TASK: COORDINATE ALIGNMENT ---
                # Determine halo padding for this specific block
                img_block = surface_blocks[skey]
                h_buf, w_buf = img_block.shape[:2]
                pad_r = (h_buf - int(window.height)) // 2
                pad_c = (w_buf - int(window.width)) // 2

                noise_window = Window(
                    col_off=window.col_off - pad_c,
                    row_off=window.row_off - pad_r,
                    width=w_buf,
                    height=h_buf
                )

                # Fetch noise (Enforcer pattern: ensure it's H,W,1 for library)
                noise_provider = noises.get(profile.noise_id)
                offset = hash(skey.value) % 1000
                noise_tile = noise_provider.window_noise(noise_window, row_off=offset, col_off=offset)

                # --- DELEGATE TO LIBRARY ---
                # Library receives (H,W,3) img and (H,W,1) noise
                surface_blocks[skey] = mod_fn(img_block, noise_tile, profile)

        return surface_blocks

    @warnings.deprecated("deprecated")
    def ZZapply_surface_mottle(
            self, surface_blocks: Dict[SurfaceKey, np.ndarray], manifest: Iterable[SurfaceKey],
            noises: "NoiseRegistry", window: "Window"
    ) -> Dict[SurfaceKey, np.ndarray]:
        """
        Post-processing pass: applies hue variations (mottle) to surfaces.
        """
        from ThematicRender.settings import SURFACE_MODIFIER_PROFILES

        for skey in manifest:
            spec = self.spec_registry.get(skey)
            if not spec or not spec.modifiers:
                continue

            profile = SURFACE_MODIFIER_PROFILES.get(spec.modifiers)
            img_block = surface_blocks[skey]

            # Master shape for this tile
            h_buf, w_buf = img_block.shape[:2]

            # Halo alignment logic
            pad_r = (h_buf - int(window.height)) // 2
            pad_c = (w_buf - int(window.width)) // 2

            noise_window = Window(
                col_off=window.col_off - pad_c,
                row_off=window.row_off - pad_r,
                width=w_buf,
                height=h_buf
            )

            noise_provider = noises.get(profile.noise_id)
            offset = hash(skey.value) % 1000

            # 2D Noise lookup
            noise = np.squeeze(noise_provider.window_noise(noise_window, row_off=offset, col_off=offset))

            # Shift is (3,) vector. noise is (H, W).
            # Broadcasting (H, W, 1) * (3,) results in (H, W, 3).
            shift = (noise[..., np.newaxis] * np.array(profile.shift_vector, dtype="float32")) * profile.intensity

            # In-place update with clipping, keeping shape (H, W, 3)
            surface_blocks[skey] = np.clip(img_block + shift, 0, 255)

        return surface_blocks

    def load_surface_ramps(self, resources: RequiredResources, output_dir: Optional[str] = None):
        """Initializes the interpolators for all ramp-based surfaces in the manifest."""
        out_dir = Path(output_dir) if output_dir else self._default_ramp_output_dir()
        out_dir.mkdir(parents=True, exist_ok=True)

        file_cfg = self.cfg.files
        ramps_yml_path = self._get_ramps_yml_path(file_cfg)

        self.ramp_files.clear()
        self.load_warnings.clear()
        self.surfaces.clear()

        # 1. Load the Primary Pivot first
        # Derived ramps  depend on this being loaded and available in ramp_files
        primary_path = None
        if resources.primary_surface:
            self._load_and_interpolate(resources.primary_surface, None, ramps_yml_path, out_dir)
            primary_path = self.ramp_files[resources.primary_surface.value]

        # 2. Load all other surfaces required by the pipeline
        for skey in resources.surface_inputs:
            if skey == resources.primary_surface:
                continue

            spec = self.spec_registry.get(skey)
            # If the surface is in 'surface_inputs', it is a REQUIREMENT.
            # If we don't have a spec for it, there is a configuration error.
            if spec is None:
                available = list(self.spec_registry.keys())
                raise ValueError(
                    f"❌ Surface Engine Error: Pipeline requires input surface '{skey}', "
                    f"but no SurfaceSpec was found in settings.py.\n"
                    f"Available specs: {available}"
                )

            # --- FUNCTIONAL DISPATCH ---
            # This specific function (load_surface_ramps) only handles 'ramp' types.
            # If the provider is 'style' (QML), we skip it here because
            # it doesn't need a scipy interpolator.
            if spec.provider_id != "ramp":
                continue

            # Check if this ramp is provided as a file or needs to be derived
            # If the user provided a file path in the config, it's explicit
            is_explicit = bool(self.cfg.files.get(skey.value))

            # Ramps with no explicit file are derived from the primary_path
            if spec is not None:
                base_path = primary_path if (spec.provider_id == "ramp" and not is_explicit) else None
            else:
                base_path = None

            self._load_and_interpolate(skey, base_path, ramps_yml_path, out_dir)

        return dict(self.ramp_files)

    def _load_and_interpolate(self, skey, base_path, ramps_yml_path, out_dir):
        """Helper to resolve a file and build the scipy interpolator."""
        # We must continue to load the file even if mode is 'dynamic' or 'hsv'.
        # Only skip if the provider_id is not 'ramp' (e.g. style overlays)
        spec = self.spec_registry[skey]
        if spec.provider_id != "ramp":
            return

        ramp_suffix = "_color_ramp"
        yaml_name = f"{skey.value}{ramp_suffix}"
        mode, ramp_path = self._resolve_ramp_file(
            surface_key=skey, yaml_name=yaml_name, base_ramp_path=base_path,
            ramp_yml_path=ramps_yml_path, output_dir=out_dir, files=self.cfg.files
        )
        print(f"Loading ramp {ramp_path} for {skey.value}")

        if ramp_path is None:
            raise FileNotFoundError(f"Ramp file for {skey.value} could not be resolved.")

        # 1. Parse the .txt file
        z, c = ColorConfig.parse_ramp(str(ramp_path))
        c = strip_alpha_or_fail(c, context=f"surface ramp {skey.value}")

        # 2. Build the interpolator
        self.surfaces[skey] = interp1d(
            z, c, axis=0, fill_value="extrapolate", kind="linear"
        )

        # 3. Save the path for derivation/reporting
        self.ramp_files[skey.value] = ramp_path

    # -------------------------------------------------------------------------
    # Path resolution / derivation
    # -------------------------------------------------------------------------
    def _resolve_ramp_file(
            self, *, surface_key: SurfaceKey, yaml_name: str, base_ramp_path: Path,
            ramp_yml_path: Path, output_dir: Path, files: Mapping[str, Any], ):
        """Resolve a  ramp path.

        Resolution order:
          1) Use explicit ramp file in cfg
          2) Create a derived ramp via ramps_yml

        Returns:
            Path if resolved; None for optional ramps that couldn't be loaded/derived.

        Raises:
            FileNotFoundError for missing VEGETATION (required).
        """
        # 1) Explicit file path.
        explicit_path = self._path_from_files(files, surface_key.value)
        context = (f"Resolve Ramp: Key: {surface_key}. Yaml RAMPS key: {yaml_name} Base Ramp: "
                   f"{base_ramp_path} Explicit path: '{explicit_path}'")
        if explicit_path is not None:
            # Explicit path provided
            if explicit_path.exists():
                return "file", explicit_path
            else:
                raise FileNotFoundError(
                    "\n".join(
                        [f"Ramp file path was provided but does not exist.\n{context}", ]
                    )
                )
        else:
            # Derive from ramps_yml
            if ramp_yml_path is None:
                raise ValueError(
                    "\n".join(
                        [f"Need to generate ramp but ramp_yml_path is None.\n{context}", ]
                    )
                )

            out_path = f"gen_{yaml_name}.txt"
            try:
                mode, fname = get_ramp_from_yml(
                    ramp_name=yaml_name, ramps_yml_settings=str(ramp_yml_path),
                    base_ramp=str(base_ramp_path), output_path=str(out_path), )
                return mode, fname

            except KeyError as e:
                raise ValueError(
                    "\n".join(
                        [f"Could not derive ramp '{surface_key.value}' from ramps_yml.",
                         f"Missing ramp definition key: '{yaml_name}'",
                         f"ramps_yml: {ramp_yml_path}", "Fallback:", "Fix:",
                         f"  - Add '{yaml_name}' to ramps_yml, or provide files['"
                         f"{surface_key.value}'].", f"Original error: {e!s}", ]
                    )
                )

            except Exception as e:
                raise ValueError(
                    "\n".join(
                        [f"Failed to derive ramp '{surface_key.value}' from ramps_yml.",
                         f"Requested derived ramp name: '{yaml_name}'",
                         f"ramps_yml: {ramp_yml_path}", f"Base ramp: {base_ramp_path}",
                         f"Output path: {out_path}", "Fallback:", "Fix:",
                         "  - Verify ramps_yml contents/format, or provide an explicit ramp "
                         "file.", f"Original error: {type(e).__name__}: {e}", ]
                    )
                )

    @staticmethod
    def _path_from_files(files: Mapping[str, Any], key: FileKey | str) -> Optional[Path]:
        """Fetch a path-like entry from cfg['files'] and normalize to Path.

        Supports both enums and strings to reduce fragility across transitions.
        """
        k = key.value if isinstance(key, FileKey) else str(key)
        raw = files.get(k)
        if not raw:
            return None
        return Path(str(raw)).expanduser()

    @staticmethod
    def _get_ramps_yml_path(files: Mapping[str, Any]) -> Optional[Path]:
        """Return ramps_yml path if present in cfg['files'] and exists."""
        raw = files.get(FileKey.RAMPS_YML.value)
        if not raw:
            raise ValueError(
                f"ERROR unable to get ramps yml FileKey '{FileKey.RAMPS_YML.value}'.\n"
                f"Available files:\n {files.keys()}"
            )
        p = Path(str(raw)).expanduser()
        if p.exists():
            return p
        else:
            raise ValueError(f"ERROR unable to open Ramps YML file {p}")

    def _default_ramp_output_dir(self) -> Path:
        """Choose output directory for derived ramps.

        Preference:
          1) directory of the output raster file
          2) current working directory
        """
        raw_out = self.cfg.files.get(FileKey.OUTPUT.value)
        if raw_out:
            try:
                return Path(str(raw_out)).expanduser().resolve().parent
            except Exception:
                pass
        return Path.cwd()
