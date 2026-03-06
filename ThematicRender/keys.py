from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Callable, Optional, Tuple, Set, FrozenSet, List, Dict, Protocol

import numpy as np


def _zeros(shape_hw: tuple[int, int]) -> np.ndarray:
    """Return HxWx1 float32 filled with 0.0."""
    h, w = shape_hw
    return np.zeros((h, w, 1), dtype=np.float32)


def _ones(shape_hw: tuple[int, int]) -> np.ndarray:
    """Return HxWx1 float32 filled with 1.0."""
    h, w = shape_hw
    return np.ones((h, w, 1), dtype=np.float32)


class SurfaceKey(StrEnum):
    # Arid Surfaces
    ARID_BASE = "arid_base"  # Arid area, no forest
    ARID_RED_BASE = "arid_red_base"
    ARID_COMPOSITE = "arid_composite"  # Arid with red areas mixed in
    ARID_VEGETATION = "arid_vegetation"

    # Humid Surfaces
    HUMID_BASE = "humid_base"  # Humid area, no forest
    HUMID_VEGETATION = "humid_vegetation"  # Humid Vegetation added to base
    HUMID_COMPOSITE = "humid_composite"

    SNOW = "snow"
    THEME_OVERLAY = "theme_overlay"


class DriverKey(StrEnum):
    """Keys for driver raster inputs stored under `cfg['files']`.

    These are the file keys that point to raster datasets used to compute factors
    (e.g., slope, precipitation, hillshade).
    """
    WATER_PROXIMITY = "water_prox"
    DEM = "dem"
    PRECIP = "precip"
    LITH = "lith"
    HILLSHADE = "hillshade"
    FOREST = "forest"
    THEME = "theme"


class FactorKey(StrEnum):
    """
    Each factor may require drivers  and/or surfaces.
    """
    DEM = "dem"
    BASE = "base"
    PRECIP = "precip"
    FOREST = "forest"
    LITH = "lith"
    SNOW = "snow"
    HILLSHADE = "hillshade"
    THEME = "theme"


class FileKey(StrEnum):
    """Non-driver file keys stored under `cfg['files']`."""

    OUTPUT = "output"
    RAMPS_YML = "ramps_yml"
    THEME_QML = "theme_qml"


@dataclass(frozen=True, slots=True)
class DriverSpec:
    dtype: np.dtype
    halo_px: int
    cleanup_type: Optional[str] = None      # 'categorical' or 'continuous'
    smoothing_radius: Optional[float] = None


@dataclass(frozen=True, slots=True)
class PipelineRequirements:
    factor_names: Set[str]
    surface_inputs: Set[Any]  # SurfaceKey


@dataclass(frozen=True, slots=True)
class SurfaceSpec:
    key: Any  # SurfaceKey
    provider_id: str
    desc: str
    files: FrozenSet[Any] = None  # FileKey
    driver: Optional[Any] = None  # DriverKey
    required_factors: Tuple[str, ...] = field(default_factory=tuple)
    coord_factor: Optional[str] = None
    modifiers: Optional[List] = None


class ConfigView(Protocol):
    def factor_on(self, name: str, default: bool = False) -> bool: ...


@dataclass(frozen=True, slots=True)
class FactorSpec:
    name: str
    function_id: str
    default_factory: Callable[[tuple[int, int]], np.ndarray]
    drivers: FrozenSet[DriverKey] = frozenset()
    required_factors: Tuple[str, ...] = field(default_factory=tuple)
    files: FrozenSet[FileKey] = frozenset()
    required_noise: Optional[str] = None
    desc: str = ""


@dataclass(frozen=True, slots=True)
class _BlendSpec:
    comp_op: str  # Entry in CompositingOps registry
    desc: str = ""  # Human-readable for the GUI/Report
    enabled: bool = True

    # Spatial Inputs (The 'Wires' in a node graph)
    factor_nm: Optional[str] = None  # The logic field (e.g. water_depth_f)
    mask_nm: Optional[str] = None  # The boundary field (e.g. water_f)

    # Palette/Surface Inputs
    input_surfaces: List[SurfaceKey] = field(default_factory=list)
    output_surface: Optional[SurfaceKey] = None

    # Routing
    buffer: str = "canvas"  # default buffer
    merge_buffer: Optional[str] = None

    # Global Signal Processing
    scale: float = 1.0
    contrast: float = 0.0
    bias: float = 0.0

    # Operation-Specific Parameters
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class NoiseProfile:
    id: str
    sigmas: Tuple[float, ...]
    weights: Tuple[float, ...]
    stretch: Tuple[float, float] = (1.0, 1.0)
    seed_offset: int = 0
    desc: str = ""


@dataclass(frozen=True, slots=True)
class SurfaceModifierProfile:
    """
    Parameters for applying noise-driven color mottle/perturbation to a surface.

    Attributes:
        intensity: Overall strength of the color shift (0.0 to 255.0).
        shift_vector: RGB weights defining the direction of the hue shift.
            e.g., (1.0, 0.8, -0.5) pushes peaks toward orange/yellow
            and valleys toward blue/cool.
        noise_id: The ID of the noise profile in NoiseRegistry to use
            for the mottle pattern (e.g., "biome" or "fine_mottle").
    """
    intensity: float
    shift_vector: Tuple[float, float, float]
    noise_id: str
    desc: str = ""


@dataclass(frozen=True, slots=True)
class RequiredResources:
    """The master manifest produced by scanning the pipeline."""
    # Physical Drivers
    drivers: Set[DriverKey]
    files: Set[FileKey]
    factor_inputs: Set[str]

    # The Geometry Master
    anchor_key: DriverKey

    # Procedural Resources
    noise_profiles: Dict[str, NoiseProfile]

    # Surface Management
    # The set of SurfaceKeys actually required by the BLEND_PIPELINE
    surface_inputs: Set[SurfaceKey]

    # The surface key used as the base for HSV-shifted derivations
    primary_surface: Optional[SurfaceKey]


@dataclass(frozen=True, slots=True)
class ResolvedManifest:
    resources: RequiredResources
    file_map: Dict[str, str]  # Key -> Path string
    factor_details: List[FactorSpec]
    surface_details: List[SurfaceSpec]
    pipeline: List[_BlendSpec]
