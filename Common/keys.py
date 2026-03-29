# keys.py

from dataclasses import dataclass, field, replace
from enum import StrEnum
from pathlib import Path
from typing import Any, Optional, Tuple, Set, FrozenSet, List, Dict, Protocol

DEFAULT_BUFFER = "canvas"

DriverKey = str
SurfaceKey = str
FactorKey = str


class FileKey(StrEnum):
    """Non-driver file keys stored under `cfg['files']`."""

    OUTPUT = "output"
    RAMPS_YML = "ramps_yml"
    THEME_QML = "theme_qml"


@dataclass(frozen=True, slots=True)
class DriverRndrSpec:
    halo_px: int
    cleanup_type: Optional[str] = None
    smoothing_radius: Optional[float] = None
    dtype: Any = "float32"


@dataclass(frozen=True, slots=True)
class DriverHWSpec:
    halo_px: int
    dtype: Any


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
    drivers: FrozenSet[DriverKey] = frozenset()
    required_factors: Tuple[str, ...] = field(default_factory=tuple)
    files: FrozenSet[FileKey] = frozenset()
    required_noise: Optional[str] = None
    desc: str = ""
    params: Dict[str, Any] = field(default_factory=dict)


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
    buffer: str = DEFAULT_BUFFER  # default buffer
    merge_buffer: Optional[str] = None

    # Global Signal Processing
    scale: float = 1.0
    contrast: float = 0.0
    bias: float = 0.0

    # Operation-Specific Parameters
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class NoiseSpec:
    id: str
    sigmas: Tuple[float, ...]
    weights: Tuple[float, ...]
    stretch: Tuple[float, float] = (1.0, 1.0)
    seed_offset: int = 0
    desc: str = ""


@dataclass(frozen=True, slots=True)
class SurfaceModifierSpec:
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


@dataclass(frozen=False, slots=True)
class RequiredResources:
    """The master manifest produced by scanning the pipeline."""
    # Physical Drivers
    drivers: Dict[DriverKey, Path]
    files: Set[FileKey]
    factor_inputs: Set[str]

    # The Geometry Master
    anchor_key: DriverKey

    # Procedural Resources
    noise_profiles: Dict[str, NoiseSpec]

    # Surface Management
    surface_inputs: Set[SurfaceKey]
    primary_surface: Optional[SurfaceKey]

    # --- THE HASHES ---
    # Initialized as empty strings, populated by TaskResolver
    geography_hash: str = ""
    logic_hash: str = ""
    style_hash: str = ""
    topology_hash: str = ""


    def with_hashes(
            self, geography_hash: str, hashes: dict
    ) -> 'RequiredResources':
        """
        Returns a copy of the resources with updated content hashes.
        Uses dataclasses.replace to handle the object update cleanly.
        """
        return replace(
            self, geography_hash=geography_hash, logic_hash=hashes["logic"],
            style_hash=hashes["style"], topology_hash=hashes["topology"]
        )


@dataclass(frozen=True, slots=True)
class ResolvedManifest:
    resources: RequiredResources
    file_map: Dict[str, str]  # Key -> Path string
    factor_details: List[FactorSpec]
    surface_details: List[SurfaceSpec]
    pipeline: List[_BlendSpec]


@dataclass(frozen=True, slots=True)
class ThemeSmoothingSpec:
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


@dataclass(frozen=True, slots=True)
class _GatedStepSpec:
    driver_key: Any
    factor_key: Any
    default_fill: float
    lerp_low: float
    noise_id: str = ""
