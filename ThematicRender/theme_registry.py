from dataclasses import dataclass
from typing import Dict, Optional, Any, Mapping

import numpy as np
from scipy.ndimage import gaussian_filter, binary_fill_holes, median_filter

from ThematicRender.qml_palette import QmlPalette, _parse_color_attr
from ThematicRender.render_config import RenderConfig

MEDIAN_FILTER_SIZE = 3
EPSILON = 1e-6

BACKGROUND_THEME_ID = 0
LUT_SIZE = 256
CLAIM_THRESHOLD = 0.2

# theme_registry.py

@dataclass(frozen=True, slots=True)
class ThemeRuntimeSpec:

    label: str
    theme_id: int
    rgb: tuple[int, int, int]

    max_opacity: float = 1.0
    blur_px: float = 0.0
    noise_amp: float = 0.0
    noise_id: str = "geology"
    contrast: float = 1.0

    smoothing_radius: float = 0.0

    surface_noise_id: Optional[str] = None
    surface_intensity: float = 0.0
    surface_shift_vector: tuple[float, float, float] = (0.0, 0.0, 0.0)

    enabled: bool = True


@dataclass(slots=True)
class ThemeTileContext:
    """Per-tile shared theme analysis.

    Args:
        theme_ids: Raw or smoothed theme ID raster for the tile.
        present_ids: Set of theme IDs present in this tile.
        active_specs: Active runtime specs that are both configured and present.
        masks_by_id: Binary float masks keyed by theme ID.
    """

    theme_ids: np.ndarray
    present_ids: set[int]
    active_specs: list[ThemeRuntimeSpec]
    masks_by_id: Dict[int, np.ndarray]


class ThemeRegistry:
    """Registry for categorical theme metadata and runtime rendering specs."""

    def __init__(self, cfg: Any):
        """Initialize the registry.

        Args:
            cfg: Render configuration.
        """
        self.cfg = cfg

        # QML-derived metadata.
        self._name_to_id: Dict[str, int] = {}
        self._id_to_color: Dict[int, tuple[int, int, int]] = {}

        # Worker-local heavy state.
        self.qml_palette: Optional[Any] = None
        self.lut_rgb: Optional[np.ndarray] = None

        # Normalized runtime specs.
        self._runtime_specs_by_label: Dict[str, ThemeRuntimeSpec] = {}
        self._runtime_specs_by_id: Dict[int, ThemeRuntimeSpec] = {}

    @property
    def name_to_id(self) -> Dict[str, int]:
        """Return mapping from label -> theme ID."""
        return self._name_to_id

    @property
    def runtime_specs_by_label(self) -> Dict[str, ThemeRuntimeSpec]:
        """Return active/inactive runtime specs keyed by label."""
        return self._runtime_specs_by_label

    @property
    def runtime_specs_by_id(self) -> Dict[int, ThemeRuntimeSpec]:
        """Return runtime specs keyed by theme ID."""
        return self._runtime_specs_by_id

    def load_metadata(self, render_cfg: Any) -> None:

        qml_path = render_cfg.path("theme_qml")
        if not qml_path or not qml_path.exists():
            raise FileNotFoundError(f"Theme QML not found: {qml_path}")

        self.qml_palette = QmlPalette.load(qml_path)

        self._name_to_id.clear()
        self._id_to_color.clear()
        self._runtime_specs_by_label.clear()
        self._runtime_specs_by_id.clear()

        self._name_to_id.update(self.qml_palette.value_by_label)

        for value_str, entry in self.qml_palette.entries_by_value.items():
            rgb = _parse_color_attr(entry.color_hex)
            if rgb is None:
                continue
            theme_id = int(value_str)
            self._id_to_color[theme_id] = rgb

        self._build_runtime_specs(render_cfg)

    def _build_runtime_specs(self, render_cfg: Any) -> None:

        categories_cfg = self._extract_theme_category_config(render_cfg)
        smoothing_cfg = self._extract_smoothing_config(render_cfg)
        modifiers_cfg = getattr(render_cfg, "modifiers", {}) or {}

        # DEBUG
        thr = getattr(render_cfg, "theme_render", {}) or {}
        ct = thr.get("categories")

        for label, cat_cfg in categories_cfg.items():
            if label not in self._name_to_id:
                raise ValueError(
                    f"Theme '{label}' is configured but not found in the QML palette."
                )

            theme_id = self._name_to_id[label]
            rgb = self._id_to_color.get(theme_id, (0, 0, 0))
            smoothing_radius = float(
                smoothing_cfg.get(label, smoothing_cfg.get("_default_", {})).get(
                    "smoothing_radius", 0.0
                )
            )

            modifier = modifiers_cfg.get(label)
            surface_noise_id = getattr(modifier, "noise_id", None) if modifier else None
            surface_intensity = float(getattr(modifier, "intensity", 0.0)) if modifier else 0.0
            surface_shift_vector = tuple(
                getattr(modifier, "shift_vector", (0.0, 0.0, 0.0))
            ) if modifier else (0.0, 0.0, 0.0)

            spec = ThemeRuntimeSpec(
                label=label,
                theme_id=theme_id,
                rgb=rgb,
                max_opacity=float(cat_cfg.get("max_opacity", 1.0)),
                blur_px=float(cat_cfg.get("blur_px", 0.0)),
                noise_amp=float(cat_cfg.get("noise_amp", 0.0)),
                noise_id=str(cat_cfg.get("noise_id", "geology")),
                contrast=float(cat_cfg.get("contrast", 1.0)),
                smoothing_radius=smoothing_radius,
                surface_noise_id=surface_noise_id,
                surface_intensity=surface_intensity,
                surface_shift_vector=tuple(float(v) for v in surface_shift_vector),
                enabled=bool(cat_cfg.get("enabled", True)),
            )

            self._runtime_specs_by_label[label] = spec
            self._runtime_specs_by_id[theme_id] = spec

    def _extract_theme_category_config(self, render_cfg: Any) -> Dict[str, Mapping[str, Any]]:

        theme_render = getattr(render_cfg, "theme_render", None)
        if theme_render and getattr(theme_render, "get", None):
            categories = theme_render.get("categories", {})
            if categories:
                return dict(categories)


        logic = getattr(render_cfg, "logic", {}) or {}
        return {
            label: params
            for label, params in logic.items()
            if label in self._name_to_id
        }

    @staticmethod
    def _extract_smoothing_config(render_cfg: Any) -> Dict[str, Mapping[str, Any]]:

        all_specs = getattr(render_cfg, "theme_smoothing_specs", {}) or {}
        if "theme_smoothing" in all_specs:
            return dict(all_specs["theme_smoothing"])
        return {}

    def load_theme_style(self) -> None:
        """Build dense RGB LUT in worker process."""
        if self.lut_rgb is not None:
            return

        lut = np.zeros((LUT_SIZE, 3), dtype=np.uint8)
        for theme_id, rgb in self._id_to_color.items():
            if not 0 <= theme_id < LUT_SIZE:
                raise ValueError(f"Theme ID {theme_id} is outside LUT range 0-{LUT_SIZE - 1}.")
            lut[theme_id] = rgb

        self.lut_rgb = lut

    def build_tile_context(self, theme_ids: np.ndarray) -> ThemeTileContext:

        present_ids = set(np.unique(theme_ids).tolist())
        active_specs: list[ThemeRuntimeSpec] = []
        masks_by_id: Dict[int, np.ndarray] = {}

        for theme_id in present_ids:
            if theme_id == BACKGROUND_THEME_ID:
                continue

            spec = self._runtime_specs_by_id.get(theme_id)
            if spec is None or not spec.enabled:
                continue

            active_specs.append(spec)
            masks_by_id[theme_id] = (theme_ids == theme_id).astype(np.float32)

        active_specs.sort(key=lambda item: item.theme_id)
        return ThemeTileContext(
            theme_ids=theme_ids,
            present_ids=present_ids,
            active_specs=active_specs,
            masks_by_id=masks_by_id,
        )

    def get_theme_surface(
        self,
        theme_ids: np.ndarray,
        ctx: Any,
        tile_ctx: Optional[ThemeTileContext] = None,
    ) -> np.ndarray:

        if self.lut_rgb is None:
            self.load_theme_style()

        if tile_ctx is None:
            tile_ctx = self.build_tile_context(theme_ids)

        indices = theme_ids.astype(np.uint8)
        rgb_float = self.lut_rgb[indices].astype(np.float32)

        noise_cache: Dict[str, np.ndarray] = {}

        for spec in tile_ctx.active_specs:
            if not spec.surface_noise_id or spec.surface_intensity <= 0.0:
                continue

            noise = noise_cache.get(spec.surface_noise_id)
            if noise is None:
                noise_provider = ctx.noises.get(spec.surface_noise_id)
                if noise_provider is None:
                    raise KeyError(
                        f"Missing noise provider '{spec.surface_noise_id}' "
                        f"for theme '{spec.label}'."
                    )
                noise = np.squeeze(noise_provider.window_noise(ctx.window)).astype(np.float32)
                noise_cache[spec.surface_noise_id] = noise

            centered_noise = noise - 0.5
            shift = (
                centered_noise[..., np.newaxis]
                * np.asarray(spec.surface_shift_vector, dtype=np.float32)
                * spec.surface_intensity
            )
            mask_3d = tile_ctx.masks_by_id[spec.theme_id][..., np.newaxis]
            rgb_float += shift * mask_3d

        rgb_float[theme_ids == BACKGROUND_THEME_ID] = 0.0
        return np.clip(rgb_float, 0.0, 255.0)

    def get_smoothed_ids(self, theme_ids_2d: np.ndarray) -> np.ndarray:

        if theme_ids_2d is None or not np.any(theme_ids_2d):
            return theme_ids_2d

        return self.get_smoothed_theme(
            theme_ids_2d=theme_ids_2d,
            specs_by_id=self._runtime_specs_by_id,
        )

    @staticmethod
    def get_smoothed_theme(
        theme_ids_2d: np.ndarray,
        specs_by_id: Mapping[int, ThemeRuntimeSpec],
    ) -> np.ndarray:
        """Smooth categorical theme IDs with hole removal and blur-threshold cleanup."""
        if theme_ids_2d is None or not np.any(theme_ids_2d):
            return theme_ids_2d

        cleaned = median_filter(theme_ids_2d, size=MEDIAN_FILTER_SIZE)
        out = np.full_like(cleaned, BACKGROUND_THEME_ID)

        present_ids = [int(v) for v in np.unique(cleaned) if int(v) != BACKGROUND_THEME_ID]
        if not present_ids:
            return cleaned

        support_fields: list[np.ndarray] = []
        support_ids: list[int] = []

        for theme_id in present_ids:
            spec = specs_by_id.get(theme_id)
            if spec is None or not spec.enabled:
                continue

            sigma = float(spec.smoothing_radius)
            mask = cleaned == theme_id

            # 1. Fill enclosed holes inside the category
            mask = binary_fill_holes(mask)

            if sigma <= 0.0:
                support = mask.astype(np.float32)
            else:
                support = gaussian_filter(mask.astype(np.float32), sigma=sigma)

            support_fields.append(support)
            support_ids.append(theme_id)

        if not support_fields:
            return cleaned

        stacked = np.stack(support_fields, axis=0)
        winner_index = np.argmax(stacked, axis=0)
        winner_support = np.max(stacked, axis=0)

        winner_ids = np.asarray(support_ids, dtype=cleaned.dtype)
        claim_mask = winner_support >= CLAIM_THRESHOLD

        out[claim_mask] = winner_ids[winner_index[claim_mask]]
        return out

def refine_organic_signal_b(
    mask: np.ndarray,
    *,
    spec: ThemeRuntimeSpec,
    ctx: Any,
) -> np.ndarray:
    """
    Refine a theme mask using ThemeRuntimeSpec parameters.
        NOTE: A and B are identical except A uses the A pattern for passing parameters and
    B uses the B pattern
    """
    signal = mask.astype(np.float32)

    if spec.blur_px > 0.0:
        signal = gaussian_filter(signal, sigma=spec.blur_px)

    if spec.noise_amp > 0.0:
        noise_provider = ctx.noises.get(spec.noise_id)
        if noise_provider is None:
            raise KeyError(
                f"Missing noise provider '{spec.noise_id}' for theme '{spec.label}'."
            )

        noise = np.squeeze(noise_provider.window_noise(ctx.window)).astype(np.float32)

        # Example noise modulation: centered around 1.0
        signal *= 1.0 + ((noise - 0.5) * 2.0 * spec.noise_amp)

    signal = np.clip(signal, 0.0, 1.0)

    if spec.contrast != 1.0:
        signal = np.power(signal, spec.contrast)

    return np.clip(signal * spec.max_opacity, 0.0, 1.0)

def refine_organic_signal_a(mask, blur_px, noise_amp, noise_id, contrast, max_opacity, ctx, name):
    """
    NOTE: A and B are identical except A uses the A pattern for passing parameters and
    B uses the B pattern
    Transforms a clinical GIS mask into a naturalized artistic factor.

    This is the core 'Artistic Brush' of the engine. It supports two modes:
    1. CRISP Mode (Default): Uses contrast to create sharp, mottled rock patches.
    2. SILKY Mode (Power): Uses exponential curves for fluid-like transitions (Water).
    """
    # Isolate strictly 2D plane
    signal = np.squeeze(mask).astype(np.float32)
    params = ctx.cfg.get_logic(name)

    # 1. INITIAL MELT: Soften the upscaled driver geometry
    if blur_px > 0:
        signal = gaussian_filter(signal, sigma=blur_px)

    # 2. SIGNAL SHAPING: Resolve the transition curve
    power_val = float(params.get("power_exponent", 0.0))
    if power_val > 0:
        # SILKY PATH: Creates the 'Glint' look with long, smooth tails
        signal = np.power(signal, 1.0 / max(power_val, 0.1))
    elif contrast != 1.0:
        # CRISP PATH: Sharps the edge to create distinct mineral islands
        signal = np.clip((signal - 0.5) * contrast + 0.5, 0.0, 1.0)

    # 3. PROCEDURAL TEXTURE: Inject organic variation (Sand-Swept / Grain)
    if noise_id:
        noise_provider = ctx.noises.get(noise_id)
        noise = np.squeeze(noise_provider.window_noise(ctx.window))

        # Math creates a visibility multiplier between (1.0 - noise_amp) and 1.0
        variation = (1.0 - noise_amp) + (noise * noise_amp)
        signal = signal * variation

    # 4. FINAL STANDARDIZATION
    return np.clip(signal, 0.0, 1.0) * max_opacity
