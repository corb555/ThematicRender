from __future__ import annotations

from dataclasses import field, dataclass
from pathlib import Path
from typing import (Any, Mapping, Optional, Final, Tuple, Iterable, Set, Dict)
import warnings

import numpy as np
# config.py
from YMLEditor.yaml_reader import ConfigLoader

from ThematicRender.compositing_library import COMPOSITING_REGISTRY
from ThematicRender.keys import SurfaceKey, DriverKey, FactorKey, FileKey, NoiseProfile, \
    RequiredResources, _BlendSpec, SurfaceSpec, FactorSpec, PipelineRequirements
from ThematicRender.schema import RENDER_SCHEMA
from ThematicRender.settings import DriverSpec, DRIVER_SPECS, NOISE_PROFILES, \
    SURFACE_MODIFIER_PROFILES, SURFACE_SPECS, FACTOR_SPECS
from ThematicRender.utils import DTYPE_ALIASES, _onoff, GenMarkdown


@dataclass(slots=True)
class ConfigMgr:
    defs: dict[str, Any] = field(default_factory=dict)
    _pipeline_req: Optional[PipelineRequirements] = field(default=None, init=False, repr=False)

    # -------------------------
    # Construction & Validation
    # -------------------------
    @classmethod
    def load(cls, config_path: Path) -> "ConfigMgr":
        loader = ConfigLoader(RENDER_SCHEMA)
        defs = loader.read(config_file=Path(config_path))
        if not isinstance(defs, dict):
            raise ValueError("Config loader must return a dictionary.")
        return cls(defs=defs)

    def resolve_paths(self, prefix: str, output_arg: str) -> None:
        """
        Combines static 'files' and 'prefixed_files' from config,
        prepends prefix to 'prefixed_files'
        and sets the final 'output' path.
        """
        # 1. Get the base blocks
        static_files = self._get_block_optional(self.defs, "files")
        prefixed_defs = self._get_block_optional(self.defs, "prefixed_files")

        # 2. Build the final flat 'files' mapping
        resolved = {}

        # Add static files
        for key, filename in static_files.items():
            resolved[key] = str(filename)

        # Add prefixed files with prepended prefix
        for key, suffix in prefixed_defs.items():
            resolved[key] = f"{prefix}{suffix}"

        # 3. CLI Output override (priority)
        resolved["output"] = output_arg

        # 4. Save back into defs as the single source of truth for 'files'
        self.defs["files"] = resolved

    def validate_paths(self) -> None:
        """Verify that all input files exist."""
        # Check standard inputs defined in your DriverKeys or FileKeys
        # Only validate keys that aren't the 'output'
        for key, path_str in self.files.items():
            if key == "output":
                # Only check that the directory exists for output
                out_path = Path(path_str).expanduser()
                if not out_path.parent.exists():
                    raise FileNotFoundError(f"Output directory missing: {out_path.parent}")
                continue

            # For all others, treat as required input
            path = Path(path_str).expanduser()
            if not path.exists():
                # Note: You can make specific files optional by checking
                # against a list of optional keys if needed.
                raise FileNotFoundError(f"Missing input file: [{key}] -> {path}")

    def get(self, key: str, default: Any = None) -> Any:
        """
        Generic getter for top-level config values (like 'seed' or 'debug_mode').
        Used by BiomeProcessor to safely access settings.
        """
        return self.defs.get(key) #, default)

    def merge_files(self, resolved_files: Mapping[str, Path]) -> None:
        """ Adds files to the config files key """
        files = dict(self.files)  # copy mapping -> mutable dict
        files.update({str(k): str(v) for k, v in resolved_files.items()})
        self.defs["files"] = files

    def output_path(self, key: str) -> Path:
        """Return a Path object from the 'files' block. Expands user (~)."""
        raw = self.files.get(key, "")
        p = "" if raw is None else str(raw).strip()
        if not p:
            raise ValueError(f"Config is missing path for files['{key}'].")
        return Path(p).expanduser()

    def input_path(self, key: str, *, context: str) -> Path:
        """Return a Path and verify the file exists on disk."""
        path = self.output_path(key)
        if not path.exists():
            raise FileNotFoundError(f"Required input file missing for {context}: {path}")
        return path

    def optional_input_path(self, key: str, *, context: str) -> Optional[Path]:
        """Return Path if configured and exists; returns None if key is missing."""
        raw = self.files.get(key, "")
        if not raw:
            return None
        path = Path(str(raw).strip()).expanduser()
        if not path.exists():
            raise FileNotFoundError(
                f"Optional input file configured but missing for {context}: {path}"
            )
        return path

    def apply_creation_options(self, co: Optional[list[str]]) -> None:
        """Merge CLI --co flags into the YAML output settings."""
        if not co:
            return

        # Ensure 'output' block exists
        if "output" not in self.defs:
            self.defs["output"] = {}

        output = dict(self.defs["output"])
        creation = dict(output.get("creation_options", {}))

        for item in co:
            if "=" in item:
                k, v = item.split("=", 1)
                creation[k.strip().upper()] = v.strip()  # GDAL COs are usually uppercase

        output["creation_options"] = creation
        self.defs["output"] = output

    @property
    def files(self) -> Mapping[str, Any]:
        return self._get_block(self.defs, "files")

    @staticmethod
    def _as_mapping(v: Any, *, where: str) -> Mapping[str, Any]:
        if isinstance(v, Mapping):
            return v
        raise ValueError(f"Config '{where}' must be a mapping, got {type(v).__name__}.")

    @staticmethod
    def _get_block(defs: Mapping[str, Any], key: str) -> Mapping[str, Any]:
        if key not in defs:
            print(f"ERR defs is <<<{defs}>>>")
            raise ValueError(f"Config is missing required top-level block '{key}'.")
        return ConfigMgr._as_mapping(defs[key], where=key)

    @staticmethod
    def _get_block_optional(defs: Mapping[str, Any], key: str) -> Mapping[str, Any]:
        if key not in defs or defs[key] is None:
            return {}
        return ConfigMgr._as_mapping(defs[key], where=key)

    @staticmethod
    def _get_nested_block_optional(defs: Mapping[str, Any], dotted: str) -> Mapping[str, Any]:
        parts = dotted.split(".")
        cur: Any = defs
        path: list[str] = []
        for p in parts:
            path.append(p)
            cur_map = ConfigMgr._as_mapping(cur, where=".".join(path[:-1]) or "<root>")
            if p not in cur_map or cur_map[p] is None:
                return {}
            cur = cur_map[p]
        return ConfigMgr._as_mapping(cur, where=dotted)

    @property
    def enabled(self) -> Mapping[str, Any]:
        """Return the optional `enabled` block (or `{}` if missing)."""
        return ConfigMgr._get_block_optional(self.defs, "enabled")

    @property
    def factors(self) -> Mapping[str, Any]:
        """Return `enabled.factorss` mapping (or `{}` if missing)."""
        return ConfigMgr._get_nested_block_optional(self.defs, "enabled.factors")

    @property
    def surfaces_enabled(self) -> Mapping[str, Any]:
        """Return `enabled.surfaces` mapping (or `{}` if missing)."""
        return ConfigMgr._get_nested_block_optional(self.defs, "enabled.surfaces")

    def factor_on(self, name: str, default: bool = False) -> bool:
        return bool(self.factors.get(name, default))

    def surface_on(self, name: str, default: bool = True) -> bool:
        return bool(self.surfaces_enabled.get(name, default))

    def file(self, name: str, *, required: bool = False) -> str:
        v = self.files.get(name, "")
        s = "" if v is None else str(v)
        if required and not s:
            raise ValueError(f"Config is missing required files['{name}'].")
        return s

    def driver_spec(self, key: DriverKey) -> DriverSpec:
        base = DRIVER_SPECS.get(key)
        if base is None:
            raise KeyError(f"Missing DRIVER_SPECS entry for driver '{key.value}'")

        overrides = self.defs.get("driver_specs") or {}
        if not isinstance(overrides, Mapping):
            return base

        raw = overrides.get(key.value)
        if raw is None or not isinstance(raw, Mapping):
            return base

        dtype = base.dtype
        halo_px = base.halo_px

        if raw.get("dtype") is not None:
            dtype = _parse_dtype(raw["dtype"], where=f"driver_specs.{key.value}.dtype")

        if raw.get("halo_px") is not None:
            halo_px = max(0, int(raw["halo_px"]))

        return DriverSpec(dtype=np.dtype(dtype), halo_px=halo_px)

    @property
    def drivers(self) -> Mapping[str, Any]:
        """Return  drivers """
        return self._get_block(self.defs, "drivers")

    def driver(self, name: str) -> Mapping[str, Any]:
        """
        Returns driver parameters
        """
        drivers = self.drivers
        if name not in drivers:
            raise ValueError(
                f"'{name}' not found in Config drivers\nAvailable: {drivers.keys()}"
            )
        return self._as_mapping(drivers[name], where=f"drivers.{name}")

    def get_logic_params(self, key: str) -> Mapping[str, Any]:
        """
        Returns the math parameters for a specific logic key (e.g., 'dem' or 'playa').
        Pulls exclusively from DRIVER_LOGIC_PARAMS in settings.py.
        """
        from .settings import DRIVER_LOGIC_PARAMS

        params = DRIVER_LOGIC_PARAMS.get(key)
        if params is None:
            raise KeyError(f"Logic parameters for '{key}' not found in settings.py.")

        return params

    def driver_params(self, name: str) -> Mapping[str, Any]:
        """
        Returns driver parameters
        """
        from .settings import DRIVER_LOGIC_PARAMS

        params = DRIVER_LOGIC_PARAMS.get(name)

        if params is None:
            available = list(DRIVER_LOGIC_PARAMS.keys())
            raise KeyError(
                f"Logic parameters for '{name}' not found in settings.py. "
                f"Available: {available}"
            )

        return params

    # -------------------------
    # String format the config
    # -------------------------
    @warnings.deprecated("deprecated")
    def ZZformat(self) -> str:
        files = self.files
        drivers = self.drivers
        lines: list[str] = ["    Files:"]

        file_keys_in_order: list[str] = ([FileKey.RAMPS_YML.value] + [FileKey.OUTPUT.value])

        for k in file_keys_in_order:
            v = files.get(k)
            if v:
                lines.append(f"    🔹 {k}: {v}")

        def fmt_value(vv: Any) -> str:
            """Format config values compactly for display."""
            if isinstance(vv, float):
                return f"{vv:g}"
            return str(vv)

        def fmt_params(name: str) -> str:
            """Format  as '(k=v, k=v, ...)' or '' if none."""
            raw = drivers.get(name)
            if not isinstance(raw, Mapping) or not raw:
                return ""
            # Stable ordering for readable diffs
            items = ", ".join(f"{ky}={fmt_value(raw[ky])}" for ky in sorted(raw))
            return f" ({items})"

        lines.append("\n    Factors:")

        # Display factors / enabled
        for factor in FactorKey:
            factor_name = factor.value
            lines.append(
                f"    {_onoff(self.factor_on(factor_name))} {factor.name}{fmt_params(factor_name)}"
            )

        lines.append("\n    Surfaces:")

        for surface in SurfaceKey:
            surface_name = surface.value
            if (
                    surface == SurfaceKey.HUMID_VEGETATION or SurfaceKey.ARID_VEGETATION or
                    self.surface_on(
                surface_name
            )):
                enabled = True
            else:
                enabled = False
            suffix = f": {files.get(surface_name)}" if files.get(surface_name) else ""
            lines.append(f"    {_onoff(enabled)} {surface.name}{suffix}")

        lines.append("")
        lines.append("    Drivers:")

        file_keys_in_order: list[str] = (
                [DriverKey.DEM.value] + [k.value for k in DriverKey if k != DriverKey.DEM])

        for k in file_keys_in_order:
            v = files.get(k)
            # if v:
            lines.append(f"    🔹 {k}: {v}")

        lines.append("")
        return "\n".join(lines)


@dataclass(frozen=True, slots=True)
class FeatureSmoothingProfile:
    """
    Parameters for Raster Boundary Refinement and Generalization.

    This profile controls how discrete, low-resolution thematic classes are
    interpolated and smoothed to align with high-resolution grids. It
    effectively mitigates aliasing and "stair-step" resampling artifacts.

    Attributes:
        precedence (int):
            The topological priority of the feature class. When smoothing causes
            two features to overlap, the class with higher precedence 'claims' the
            contested pixels.
            Range: 0 (Background) to 10 (Critical Infrastructure/Water).
            Use: Set high for features with strict boundaries (e.g., Shorelines)
            to prevent them from being 'swallowed' by sprawling features like Rock.

        smoothing_radius (float):
            The standard deviation for Gaussian kernel smoothing.
            Range: 0.5 (Preserve local detail) to 5.0 (Broad generalization).
            Use: Increase to remove blocky artifacts from low-res source data.
            Decrease for features with complex, high-frequency perimeters like
            craggy outcrops.

        expansion_weight (float):
            The probability threshold that determines the new feature boundary.
            Range: 0.01 (Aggressive Buffer/Dilation) to 0.5 (Conservative/Eroded).
            Use: Lower values 'dilate' the feature, creating a safety
            buffer to prevent alpha-thinning. Increase for features that should
            remain compact and avoid 'bloating' into adjacent zones.
    """
    precedence: int
    smoothing_radius: float
    expansion_weight: float


THEME_SMOOTHING_PROFILES: Final[dict[str, FeatureSmoothingProfile]] = {
    "water": FeatureSmoothingProfile(precedence=5, smoothing_radius=3.0, expansion_weight=0.4),
    "rock": FeatureSmoothingProfile(precedence=4, smoothing_radius=4.0, expansion_weight=0.4),
    "volcanic": FeatureSmoothingProfile(precedence=3, smoothing_radius=3.0, expansion_weight=0.4),
    "glacier": FeatureSmoothingProfile(precedence=2, smoothing_radius=3.0, expansion_weight=0.2),
    "playa": FeatureSmoothingProfile(precedence=1, smoothing_radius=3.0, expansion_weight=0.3),
    "outwash": FeatureSmoothingProfile(precedence=1, smoothing_radius=3.0, expansion_weight=0.3),
    "_default_": FeatureSmoothingProfile(precedence=0, smoothing_radius=3.0, expansion_weight=0.3),
}


@dataclass(frozen=True, slots=True)
class EdgeProfile:
    """
    Parameters for the spatial blending of feature margins.

    This profile governs the 'Alpha Decay' — the mathematical transition from
    100% feature presence to 0% (background) based on Euclidean distance
    from the generalized boundary.

    Attributes:
        edge_blur_px (int):
            The radius of sub-pixel anti-aliasing.
            Range: 1 (Sharp/Technical) to 15 (Soft/Organic).
            Use: Fixes aliasing on high-resolution grids. Increase for volumetric
            features like glaciers or clouds; decrease for high-precision
            boundaries like waterlines or parcel edges.

        transition_width_px (int):
            The width of the internal 'Fringe' or 'Ramp' zone in pixels.
            Range: 0 (Binary contact) to 100+ (Regional gradient).
            Use: Simulates ecotones or transitional zones (e.g., the 'bleed'
            between a salt flat and soil). Set to 0 for geological contacts.

        falloff_rate (float):
            The exponential power (Gamma) of the transition curve.
            Range: 0.5 (Convex/Foggy) to 3.0 (Concave/Heavy).
            Use: Values > 1.0 maintain high opacity until the very edge,
            creating a 'Solid' feel. Values < 1.0 create a 'Lingering' effect,
            where the feature appears as a faint dusting into the background.

        max_opacity (float):
            The peak transparency limit for the feature class.
            Range: 0.0 (Invisible) to 1.0 (Fully Opaque).
            Use: Set < 1.0 for translucent layers like atmospheric haze,
            suspended sediment in water, or thin vegetation canopy to allow
            underlying terrain textures to remain visible.
    """
    edge_blur_px: int = 2
    transition_width_px: int = 0
    falloff_rate: float = 1.0
    max_opacity: float = 1.0

EDGE_PROFILES: Final[dict[str, EdgeProfile]] = {
    "glacier": EdgeProfile(
        edge_blur_px=2, transition_width_px=1, falloff_rate=0.8, max_opacity=0.8
    ),
    "rock": EdgeProfile(edge_blur_px=8, transition_width_px=2, falloff_rate=0.8, max_opacity=0.5),
    "volcanic": EdgeProfile(
        edge_blur_px=4, transition_width_px=20, falloff_rate=0.7, max_opacity=0.5
    ),
    "playa": EdgeProfile(edge_blur_px=4, transition_width_px=45, falloff_rate=0.3, max_opacity=0.4),
    "water": EdgeProfile(edge_blur_px=2, transition_width_px=0, falloff_rate=0.8, max_opacity=0.7),
    "outwash": EdgeProfile(
        edge_blur_px=2, transition_width_px=6, falloff_rate=0.8, max_opacity=0.7
    ),
}


@dataclass(frozen=True, slots=True)
class HueVariationProfile:
    """
    Parameters for applying noise-driven color mottle/perturbation to a surface.

    Attributes:
        intensity (float): Overall strength of the color shift (0.0 to 255.0).
        shift_vector (Tuple[float, float, float]): RGB weights defining the
            direction of the hue shift. e.g., (1.0, 0.5, -1.0) pushes peaks
            toward orange/yellow and valleys toward blue/cool.
        noise_id (str): The ID of the noise profile in NoiseRegistry to use
            for the mottle pattern.
    """
    intensity: float
    shift_vector: Tuple[float, float, float]
    noise_id: str


Slice2D = Tuple[slice, slice]


@dataclass(frozen=True)
class DriverBlock:
    value: np.ndarray
    valid: np.ndarray
    inner_slices: Optional[Slice2D] = None
    shm_name: Optional[str] = None  # Reference for IPC

    def to_metadata(self) -> dict:
        """Returns only the tiny info needed to rebuild this block in another process."""
        return {
            "shm_name": self.shm_name, "shape": self.value.shape, "dtype": str(self.value.dtype),
            "inner_slices": self.inner_slices,
        }

    @classmethod
    def from_metadata(cls, meta: dict):
        """Reconstructs the DriverBlock by mapping onto Shared Memory."""
        from multiprocessing.shared_memory import SharedMemory
        shm = SharedMemory(name=meta["shm_name"])
        # Create a zero-copy view of the shared memory
        val_view = np.ndarray(meta["shape"], dtype=meta["dtype"], buffer=shm.buf)
        # (Handle 'valid' array similarly...)
        return cls(value=val_view, valid=..., inner_slices=meta["inner_slices"])


def _parse_dtype(v: Any, *, where: str) -> np.dtype:
    """Parse dtype from config values."""
    if v is None:
        raise ValueError(f"{where}: dtype is None")

    if isinstance(v, np.dtype):
        return v

    if isinstance(v, type) and issubclass(v, np.generic):
        return np.dtype(v)

    if isinstance(v, str):
        key = v.strip().lower()
        if key in DTYPE_ALIASES:
            return np.dtype(DTYPE_ALIASES[key])
        raise ValueError(f"{where}: unknown dtype string '{v}'")

    raise ValueError(f"{where}: unsupported dtype {type(v).__name__}: {v!r}")

def _require_comp_ops(pipeline_list: list[_BlendSpec], required_ops: set[str]) -> None:
    enabled = [s for s in pipeline_list if getattr(s, "enabled", True)]
    enabled_ops = {getattr(s, "comp_op", None) or getattr(s, "action", None) for s in enabled}
    enabled_ops.discard(None)

    missing = required_ops - enabled_ops
    if missing:
        pretty_enabled = [
            f"{i}: comp_op={getattr(s, 'comp_op', None)!r} target={getattr(s, 'target', None)!r}"
            for i, s in enumerate(enabled)
        ]
        raise ValueError(
            "\n❌ PIPELINE CONFIG ERROR\n"
            f"Missing required pipeline steps: {sorted(missing)}\n"
            "Enabled steps:\n  - " + "\n  - ".join(pretty_enabled) + "\n"
                                                                     "Your pipeline must include an enabled comp_op='create_buffer' step "
                                                                     "before comp_op='write_output'.\n"
        )

# config_mgr.py

def derive_pipeline_requirements(
        pipeline: Iterable[Any],
        surface_specs: Iterable[Any],
        factor_specs: Iterable[Any]
) -> PipelineRequirements:
    """
    Scans the pipeline to find all required Factors and Surfaces,
    then recursively finds sub-dependencies (Surfaces -> Factors and Factors -> Factors).
    """
    req_factors: Set[str] = set()
    req_surfaces: Set[Any] = set()
    produced_surfaces: Set[Any] = set()

    ss_lookup = {ss.key: ss for ss in surface_specs}
    fs_lookup = {fs.name: fs for fs in factor_specs}

    active_steps = [s for s in pipeline if s.enabled]

    # --- PASS 1: HARVEST SEEDS FROM PIPELINE ---
    for step in active_steps:
        if hasattr(step, 'factor_nm') and step.factor_nm:
            req_factors.add(step.factor_nm)

        if hasattr(step, 'output_surface') and step.output_surface:
            produced_surfaces.add(step.output_surface)

        if hasattr(step, 'input_surfaces') and step.input_surfaces:
            for skey in step.input_surfaces:
                req_surfaces.add(skey)

    # --- PASS 2: RECURSIVE DISCOVERY ---
    # We loop until no more new factors or surfaces are found
    processed_surfaces = set()
    processed_factors = set()

    while True:
        # A. Find new dependencies from SURFACES
        new_surfaces = req_surfaces - processed_surfaces
        # B. Find new dependencies from FACTORS (Factor-on-Factor)
        new_factors = req_factors - processed_factors

        if not new_surfaces and not new_factors:
            break

        # Extract from Surfaces
        for skey in new_surfaces:
            spec = ss_lookup.get(skey)
            if spec:
                if spec.coord_factor: req_factors.add(spec.coord_factor)
                if spec.required_factors:
                    for f_req in spec.required_factors: req_factors.add(f_req)
            processed_surfaces.add(skey)

        # Extract from Factors (This fixes the 'water' gap)
        for fname in new_factors:
            spec = fs_lookup.get(fname)
            if spec:
                if spec.required_factors:
                    for f_req in spec.required_factors:
                        req_factors.add(f_req)
            processed_factors.add(fname)

    # Final inputs are only those NOT produced by the pipeline
    external_inputs = req_surfaces - produced_surfaces

    return PipelineRequirements(
        factor_names=req_factors,
        surface_inputs=external_inputs
    )

def derive_resources(
        *, cfg, pipeline: Iterable[_BlendSpec], factor_specs: Iterable[FactorSpec],
        surface_specs: Iterable[SurfaceSpec]
) -> RequiredResources:
    # materialize once so we can safely inspect it later
    pipeline_list = list(pipeline)
    _require_comp_ops(pipeline_list, {"create_buffer", "write_output"})


# 1. Identify Demand
    preq = derive_pipeline_requirements(pipeline, surface_specs, factor_specs)
    fs_lookup = {fs.name: fs for fs in factor_specs}
    ss_lookup = {ss.key: ss for ss in surface_specs}

    req_drivers: Set[DriverKey] = set()
    req_files: Set[FileKey] = {FileKey.RAMPS_YML}
    requested_noise_ids: Set[str] = set()

    # 2. Gather from Factors
    for name in preq.factor_names:
        fs = fs_lookup.get(name)
        if not fs:
            continue
        req_drivers.update(fs.drivers)
        if fs.required_noise:
            requested_noise_ids.add(fs.required_noise)

    # 3. Gather from Surfaces (Modifier Dependencies)
    for sk in preq.surface_inputs:
        ss = ss_lookup.get(sk)
        if not ss:
            continue
        if ss.driver:
            req_drivers.add(ss.driver)
        if ss.files:
            req_files.update(ss.files)

        if ss.modifiers:
            for mod_cfg in ss.modifiers:
                profile_id = mod_cfg.get("profile_id")
                if not profile_id:
                    continue
                v_profile = SURFACE_MODIFIER_PROFILES.get(profile_id)
                if v_profile is None:
                    available_vars = list(SURFACE_MODIFIER_PROFILES.keys())
                    raise ValueError(
                        f"\n❌ CONFIG ERROR: Surface '{sk.value}' requested modifier profile "
                        f"'{profile_id}', but it doesn't exist in SURFACE_MODIFIER_PROFILES.\n"
                        f"👉 Available IDs: {available_vars}"
                    )
                requested_noise_ids.add(v_profile.noise_id)

    # 4. Fulfill Noise Profiles
    noise_profiles: Dict[str, NoiseProfile] = {}
    for nid in requested_noise_ids:
        profile = NOISE_PROFILES.get(nid)
        if profile:
            noise_profiles[nid] = profile
        else:
            available_noises = list(NOISE_PROFILES.keys())
            raise ValueError(
                f"\n❌ FATAL: Pipeline requires noise profile '{nid}', but it's not defined "
                f"in the NOISE_PROFILES table in settings.py.\n"
                f"👉 Ensure the ID matches exactly.\n"
                f"👉 Available Noise IDs: {available_noises}"
            )

    # 6. DETERMINE THE ANCHOR (Geometry)
    explicit_anchor = cfg.get("anchor")
    if explicit_anchor:
        anchor_key = DriverKey(explicit_anchor)
    elif DriverKey.DEM in req_drivers:
        anchor_key = DriverKey.DEM
    elif req_drivers:
        anchor_key = sorted(list(req_drivers))[0]
    else:
        print("❌ Error: No drivers found in pipeline. ")
        res =  RequiredResources(
            drivers=req_drivers,
            files=req_files,
            anchor_key=None,
            noise_profiles=noise_profiles,
            factor_inputs=preq.factor_names,
            surface_inputs=preq.surface_inputs,
            primary_surface=None,
        )
        report = analyze_pipeline(
            cfg=cfg, resources=res, pipeline=pipeline,
            factor_specs=factor_specs, surface_specs=surface_specs
        )
        print(report)
        raise RuntimeError("❌ Error: No drivers found in pipeline. ")

    primary = None
    return RequiredResources(
        drivers=req_drivers,
        files=req_files,
        anchor_key=anchor_key,
        noise_profiles=noise_profiles,
        factor_inputs=preq.factor_names,
        surface_inputs=preq.surface_inputs,
        primary_surface=primary,
    )

def describe_tables(cfg, resources, pipeline, factor_specs, surface_specs) -> str:
    md = GenMarkdown()

    # --- Section: Summary ---
    md.header("Thematic Render: Pipeline Description", 1)
    md.bullet(f"{md.bold('Output Path:')} {cfg.files.get('output')}")
    md.bullet(f"{md.bold('Geometry Anchor:')} {resources.anchor_key.value}")
    md.text("")

    # --- Section 1: Blending Pipeline ---
    md.header("1. Blending Pipeline", 2)
    md.text("The sequence of compositing operations performed in memory.")
    md.tbl_hdr(
        "#", "Operation", "Factor", "Signal (C/B)", "Params", "Target/Surfaces", "Description"
        )

    for i, step in enumerate(pipeline):
        if not step.enabled: continue

        target = step.output_surface.value if hasattr(
            step, 'output_surface'
            ) and step.output_surface else "canvas"
        surfaces = ", ".join([s.value for s in step.input_surfaces]) if step.input_surfaces else ""

        # Format Signal Shaping (Contrast / Bias)
        signal = f"C: {step.contrast}<br>B: {step.bias}" if step.factor_nm else "N/A"

        md.tbl_row(
            i, md.bold(step.comp_op), step.factor_nm or "Initial", signal,
            md.format_dict(getattr(step, 'params', {})), f"{target} <- {surfaces}", step.desc
        )

    # --- Section 2: Surfaces ---
    md.header("2. Surface Definitions", 2)
    md.tbl_hdr("Surface Key", "Provider", "Coord Factor", "Modifiers", "Description")

    ss_lookup = {ss.key: ss for ss in surface_specs}
    for skey in resources.surface_inputs:
        ss = ss_lookup.get(skey)
        if not ss: continue

        # Clean list comprehension for modifiers
        mod_info = "None"
        if ss.modifiers:
            mod_info = "<br>".join([f"{m['id']}({m['profile_id']})" for m in ss.modifiers])

        md.tbl_row(skey.value, ss.provider_id, ss.coord_factor or "N/A", mod_info, ss.desc)

    # --- Section 3: Factor Logic ---
    md.header("3. Factor Logic", 2)
    md.text("Normalized 0..1 masks derived from physical drivers.")
    md.tbl_hdr("Factor Name", "Function", "Required Drivers", "Required Noise", "Description")

    fs_lookup = {fs.name: fs for fs in factor_specs}
    for fname in sorted(list(resources.factor_inputs)):
        fs = fs_lookup.get(fname)
        if not fs: continue

        md.tbl_row(
            fname, fs.function_id, ", ".join([d.value for d in fs.drivers]),
            fs.required_noise or "None", fs.desc
        )

    # --- Section 4: Noise Profiles ---
    md.header("4. Active Noise Profiles", 2)
    md.text("Parameters for multi-scale Gaussian jitter and mottle.")
    md.tbl_hdr("ID", "Sigmas (Scales)", "Weights", "Stretch (V,H)", "Description")

    for nid, prof in resources.noise_profiles.items():
        md.tbl_row(
            prof.id, prof.sigmas, prof.weights, prof.stretch, prof.desc
        )

    # --- Section 5: Surface Modifier Profiles ---
    md.header("5. Surface Modifier Profiles", 2)
    md.tbl_hdr("Profile ID", "Intensity", "Shift Vector (R,G,B)", "Noise Source", "Description")

    # Collect unique profile IDs referenced in the new dictionary format
    active_profile_ids = set()
    for skey in resources.surface_inputs:
        ss = ss_lookup.get(skey)
        if ss and ss.modifiers:
            active_profile_ids.update([m["profile_id"] for m in ss.modifiers])

    for pid in sorted(list(active_profile_ids)):
        vprof = SURFACE_MODIFIER_PROFILES.get(pid)
        if not vprof: continue
        md.tbl_row(pid, vprof.intensity, vprof.shift_vector, vprof.noise_id, vprof.desc)

    # --- Section 6: Drivers ---
    md.header("6. Physical Drivers", 2)
    md.text("Source files required for this build.")
    md.tbl_hdr("Key", "File Path", "Halo", "Description")
    for dkey in sorted(list(resources.drivers)):
        dspec = cfg.driver_spec(dkey)
        path = cfg.files.get(dkey.value, "NOT PROVIDED")
        md.tbl_row(dkey.value, path, f"{dspec.halo_px}px", "")

    return md.render()

def analyze_pipeline(cfg, resources, pipeline, factor_specs, surface_specs) -> str:
    from ThematicRender.settings import SURFACE_MODIFIER_PROFILES
    warnings = []
    step_with_warnings = set()  # Track indices: {0, 5, 10}

    # Helper to add a warning and tag the step simultaneously
    def add_step_warning(idx, msg):
        warnings.append(msg)
        step_with_warnings.add(idx)

    md = GenMarkdown()

    # 1. Prepare Lookups
    fs_lookup = {fs.name: fs for fs in factor_specs}
    ss_lookup = {ss.key: ss for ss in surface_specs}

    warnings = [] # Store logic warnings here

    ss_lookup = {ss.key: ss for ss in surface_specs}
    fs_lookup = {fs.name: fs for fs in factor_specs}

    # Track what WILL be available at each step
    sim_buffers = set()
    sim_surfaces = set(resources.surface_inputs) # Initially loaded surfaces

    # --- CHECK FOR WARNINGS ---
    for i, step in enumerate(pipeline):
        if not step.enabled: continue

        meta = COMPOSITING_REGISTRY.get(step.comp_op)
        if meta is None:
            add_step_warning(i, f" **Step {i}:** Unknown operation `{step.comp_op}`.")
            continue

        # A. Check Inputs (Do the required sources exist yet?)
        # Surfaces
        if "input_surfaces" in meta.required_attrs:
            for skey in (step.input_surfaces or []):
                if skey not in sim_surfaces:
                    add_step_warning(i, f" **Step {i} ({step.comp_op}):** Missing required attribute `{attr}`.")

        # Buffers (For ops like lerp, lerp_buffers, multiply, write_output)
        if "buffer" in meta.required_attrs and step.comp_op != "create_buffer":
            if step.buffer not in sim_buffers:
                add_step_warning(i, f" **Step {i}:** Requires buffer `{step.buffer}`, but it hasn't been initialized.")
        if step.comp_op == "lerp_buffers":
            if step.merge_buffer not in sim_buffers:
                add_step_warning(i,f" **Step {i}:** Merge buffer `{step.merge_buffer}` but there is no enabled `create_buffer` step.")

        # B. Check Attributes (Are the fields populated?)
        for attr in meta.required_attrs:
            val = getattr(step, attr, None)
            if val is None or (hasattr(val, "__len__") and len(val) == 0):
                add_step_warning(i,f" **Step {i} ({step.comp_op}):** Missing required attribute `{attr}`.")

        # C. Register Outputs (Update the simulation state)
        if step.comp_op == "create_buffer":
            sim_buffers.add(step.buffer)

        if hasattr(step, 'output_surface') and step.output_surface:
            sim_surfaces.add(step.output_surface)

            # 2. Check internal params dict
            for p_key in meta.required_params:
                if p_key not in step.params:
                    add_step_warning(i,
                        f" **Step {i} ({step.comp_op}):** Missing required parameter `{p_key}` in `params` dict."
                    )

        # Warning: High Contrast Clipping
        if abs(step.contrast) > 1.5:
            warnings.append(f"⚠️ **Step {i} ({step.comp_op}):** High contrast ({step.contrast}) may be clipping your factor signal to binary black/white.")

        # Warning: Texture Mismatch in Blends
        if step.comp_op == "lerp_surfaces" and len(step.input_surfaces) == 2:
            s1 = ss_lookup.get(step.input_surfaces[0])
            s2 = ss_lookup.get(step.input_surfaces[1])

        # Warning: Weak Signals
        if step.factor_nm and step.scale < 0.1:
            warnings.append(f" **Step {i}:** Factor scale is extremely low ({step.scale}). Signal is likely invisible.")

    # --- FACTOR SEQUENCE LINTER ---
    # We check if dependencies appear BEFORE the factor that needs them
    processed_in_order = []
    for spec in factor_specs:
        if spec.name in resources.factor_inputs:
            for dep in spec.required_factors:
                if dep not in processed_in_order:
                    add_step_warning("Global",
                                     f"⚠️ **Factor Sequence Error:** `{spec.name}` needs `{dep}`, "
                                     f"but `{dep}` is defined LATER in the `FACTOR_SPECS` list. "
                                     f"Move `{dep}` above `{spec.name}` in settings.py."
                                     )
        processed_in_order.append(spec.name)

    # Driver specific warnings
    for dkey in resources.drivers:
        dspec = cfg.driver_spec(dkey)
        if dspec.smoothing_radius and dspec.smoothing_radius > 15:
            warnings.append(f"⚠️ **Driver `{dkey.value}`:** Very high smoothing radius ({dspec.smoothing_radius}) might erase small features.")

    # Tracking for "First Sighting" logic to minimize repetition
    seen_factors = set()
    seen_surfaces = set()

    # --- Header Summary ---
    md.header("Thematic Render: Execution Flow", 1)
    md.bullet(f"{md.bold('Output:')} `{cfg.files.get('output')}`")
    if resources.anchor_key is not None:
        md.bullet(f"{md.bold('Anchor:')} `{resources.anchor_key.value}` (Defines master geometry)")
    else:
        md.bullet(f"{md.bold('Anchor:')} `*UNDEFINED*` Needed for master geometry!")
        warnings.append(f"⚠️No Anchor Key.  Cannot determine geometry")
    md.text("---")

    # Render Warnings at the top
    md.header("Pipeline Warnings", 3)
    if warnings:
        for w in warnings:
            md.bullet(f"⚠️ {w}")
    else:
        md.text("none")

    md.text("---")

    #  --- DISPLAY PIPELINE DESCRIPTION ---
    md.header("1. Enabled Pipeline Steps:", 2)
    for i, step in enumerate(pipeline):
        if not step.enabled:
            continue

        # 1. Determine Target (Logic-Aware)
        meta = COMPOSITING_REGISTRY.get(step.comp_op)

        # If the operation is registered to output a surface (like lerp_surfaces)
        if meta and "output_surface" in meta.required_attrs:
            target = step.output_surface.value if hasattr(step.output_surface, 'value') else str(step.output_surface)
        # If the operation targets an image buffer (like lerp, create_buffer, multiply)
        elif getattr(step, 'buffer', None):
            target = step.buffer
        else:
            target = "canvas"

        # Check the tracker for this specific index
        warning_icon = "⚠️ " if i in step_with_warnings else ""

        # Header with the warning icon
        md.header(f"Step {i}) {warning_icon}{step.desc}", 3)
        md.bullet(f"{md.bold('Operation:')} {step.comp_op}")

        # Special case for buffer-to-buffer clarity
        if step.comp_op == "lerp_buffers":
            md.bullet(f"{md.bold('Logic:')} Blend buffer `{step.merge_buffer}` into `{step.buffer}`")
        else:
            md.bullet(f"{md.bold('Target:')} `{target}`")

        # A. Factor Usage
        if step.factor_nm:
            fname = step.factor_nm
            signal = f"Scale: {step.scale} | Bias: {step.bias} | Contrast: {step.contrast} "

            if fname not in seen_factors:
                seen_factors.add(fname)
                fs = fs_lookup.get(fname)
                if fs:
                    md.bullet(f"{md.bold('Factor:')} `{fname}` * ")
                    md.text(f"  * *Logic:* `{fs.function_id}` using `{', '.join([d.value for d in fs.drivers])}` ")
                    if fs.required_noise:
                        md.text(f"  * *Noise Source:* `{fs.required_noise}`")
                else:
                    md.bullet(f"{md.bold('Factor:')} `{fname}` {md.italic('(No Spec Found)')} ")
            else:
                md.bullet(f"{md.bold('Factor:')} `{fname}` {md.italic(' ')} ")

            md.text(f"  * *Signal Shaping:* {signal}")

        # B. Inbound Surface Usage (Updated for 'input_surfaces')
        # Check for input_surfaces instead of surface_keys
        if hasattr(step, 'input_surfaces') and step.input_surfaces:
            md.bullet(f"{md.bold('Inbound Surfaces:')}")
            for skey in step.input_surfaces:
                if skey not in seen_surfaces:
                    seen_surfaces.add(skey)
                    ss = ss_lookup.get(skey)
                    if ss:
                        mods = ", ".join([f"{m['id']}({m['profile_id']})" for m in ss.modifiers]) if ss.modifiers else "None"
                        md.text(f"  * `{skey.value}` *")
                        md.text(f"    * *Provider:* `{ss.provider_id}`")
                        if ss.coord_factor: md.text(f"    * *Sampling Factor:* `{ss.coord_factor}`")
                        md.text(f"    * *Modifiers:* {mods}")
                    else:
                        md.text(f"  * `{skey.value}` {md.italic('(Computed Buffer/Surface)')}")
                else:
                    md.text(f"  * `{skey.value}` {md.italic('(Referenced)')}")

        # C. Inbound Buffer Usage (For lerp_buffers)
        if step.comp_op == "lerp_buffers":
            md.bullet(f"{md.bold('Inbound Buffers:')}")
            md.text(f"  * Base: `{step.buffer}`")
            md.text(f"  * Source: `{step.merge_buffer}`")

        md.text("") # Spacer between steps

    # --- Section 2: Resource Appendix ---
    md.header("2. Global Resources", 2)
    md.text("Shared drivers and noise profiles.")

    # Drivers Table
    md.header("Physical Drivers", 3)
    md.tbl_hdr("Key", "Halo", "Cleanup Logic", "File Path")
    for dkey in sorted(list(resources.drivers)):
        dspec = cfg.driver_spec(dkey)
        path = cfg.files.get(dkey.value, "N/A")
        cleanup = f"{dspec.cleanup_type} (r={dspec.smoothing_radius})" if dspec.cleanup_type else "None"
        md.tbl_row(f"`{dkey.value}`", f"{dspec.halo_px}px", cleanup, f"`{path}`")

    # Noise Table
    md.header("Noise Profiles", 3)
    md.tbl_hdr("ID", "Sigmas (Scales)", "Weights", "Description")
    for nid, prof in resources.noise_profiles.items():
        md.tbl_row(f"`{nid}`", prof.sigmas, prof.weights, prof.desc)

    # Modifier Profiles
    md.header("Surface Modifier Profiles", 3)
    md.tbl_hdr("Profile ID", "Intensity", "Shift (RGB)", "Noise Source")
    active_profile_ids = set()
    for skey in resources.surface_inputs:
        ss = ss_lookup.get(skey)
        if ss and ss.modifiers:
            active_profile_ids.update([m["profile_id"] for m in ss.modifiers])

    for pid in sorted(list(active_profile_ids)):
        vprof = SURFACE_MODIFIER_PROFILES.get(pid)
        if vprof:
            md.tbl_row(f"`{pid}`", vprof.intensity, vprof.shift_vector, f"`{vprof.noise_id}`")

    return md.render()