from typing import Dict, Optional, Any

import numpy as np
from scipy.ndimage import gaussian_filter, binary_fill_holes, median_filter

from ThematicRender.qml_palette import QmlPalette, _parse_color_attr
from ThematicRender.render_config import RenderConfig


# theme_registry.py
class ThemeRegistry:
    """Manages the translation of a categorical (theme) GIS raster into RGB surfaces.

    A categorical raster is a spatial dataset where pixel values represent discrete
    classifications (e.g., 1=Water, 5=Forest, 12=Urban) rather than continuous
    measurements. Because the raster contains discrete category IDs, they cannot
    be mathematically interpolated or blended directly.

    The QML file (QGIS Layer Style) serves as the explicit definition for these
    categories. It defines the mapping between category IDs, text labels, and
    intended RGB colors. This registry parses the QML to build high-speed Look-Up
    Tables (LUTs) that allow the engine to "paint" the categorical data.

    This class provides two primary services:
    1. Surface Synthesis: Translates category IDs into separate floating-point RGB color
       buffers using the LUT derived from the QML palette.
    2. Smoothing: Provides a 'Melt and Grow' algorithm to resolve aliasing
       (stairsteps) in low-resolution source data based on explicit precedence rules.
    """

    def __init__(self, cfg: RenderConfig):
        self.cfg = cfg

        # 1. Metadata (Populated in Main, sent to Workers via Pickling)
        self._label_to_id: Dict[str, int] = {}
        self._id_to_color: Dict[int, tuple] = {}

        # 2. Heavy Objects (Initialized per process)
        self.qml_palette: Optional[Any] = None
        self.lut_rgb: Optional[np.ndarray] = None

    @property
    def label_to_id(self) -> Dict[str, int]:
        """Provides the mapping even if qml_palette isn't loaded (e.g. in Main)."""
        return self._label_to_id

    def load_metadata(self, render_cfg: RenderConfig) -> None:
        """
        Parses the QML from the CURRENT job config to extract metadata.
        """
        # Use the path from the manifest's config
        qml_path = render_cfg.path("theme_qml")

        if not qml_path or not qml_path.exists():
            raise FileNotFoundError(f"Theme QML not found: {qml_path}")

        # print(f"🎨 [ThemeRegistry] Loading colors from {qml_path.name}")
        self.qml_palette = QmlPalette.load(qml_path)

        # Clear old data if this is a new job
        self._label_to_id.clear()
        self._id_to_color.clear()

        self._label_to_id.update(self.qml_palette.value_by_label)

        for v_str, entry in self.qml_palette.entries_by_value.items():
            rgb = _parse_color_attr(entry.color_hex)
            if rgb:
                val = int(v_str)
                self._id_to_color[
                    val] = rgb  # DIAGNOSTIC: Ensure we found the water  # if val == 4:  #
                # print(f"DEBUG [ThemeRegistry] Found Water (ID 4): {rgb}")

        # print(f"🎨 [ThemeRegistry] Loaded {len(self._id_to_color)} color mappings.")  #
        # EVIDENCE TRACE 1:  # print(f"TRACE [Main]: QML Path used: {render_cfg.path(
        # 'theme_qml')}")  # print(f"TRACE [Main]: Dict Content for Water (ID 4): {
        # self._id_to_color.get(4)}")  # print(f"TRACE [Main]: Total labels in dict: {len(
        # self._label_to_id)}")

    def load_theme_style(self) -> None:
        """
        Builds the physical LUT from pre-loaded metadata.
        Call this in the WORKER process init.
        """
        if self.lut_rgb is not None:
            return

        # Initialize the dense NumPy array (256 categories, 3 RGB bands)
        lut = np.zeros((256, 3), dtype=np.uint8)

        # Fill LUT from the metadata inherited via pickling
        for val, rgb in self._id_to_color.items():
            if 0 <= val < 256:
                lut[val] = rgb
            else:
                raise ValueError(f"BAD LUT. Val={val}")

        self.lut_rgb = lut  # print(f"🔍 [ID:{id(self)}] load_theme_style COMPLETE. LUT[4] value:
        # {self.lut_rgb[4]}")

    def get_theme_surface(self, theme_ids: np.ndarray, ctx: Any) -> np.ndarray:
        if self.lut_rgb is None:
            self.load_theme_style()

        # 1. Base Painting (from QML colors)
        indices = theme_ids.astype(np.uint8)
        rgb_float = self.lut_rgb[indices].astype("float32")

        # 2. Apply per-category mottling
        for label, target_id in self.label_to_id.items():
            target_id = self.label_to_id.get(label)
            if target_id is None or target_id not in theme_ids:
                continue

            # Get the modifier profile from the config
            profile = ctx.cfg.modifiers.get(label)  # Looks for 'rock' or 'glacier' in modifiers
            if not profile:
                continue

            # Generate noise for this specific tile

            noise_provider = ctx.noises.get(profile.noise_id)
            noise = np.squeeze(noise_provider.window_noise(ctx.window))
            centered_noise = noise - 0.5

            # Create the category mask
            mask = (theme_ids == target_id)[..., np.newaxis]  # (H, W, 1)

            # Calculate the shift for this category only
            shift = (centered_noise[..., np.newaxis] * np.array(
                profile.shift_vector
                )) * profile.intensity

            # Apply only where the category exists
            rgb_float += (shift * mask)

        # 3. Cleanup and Standardize
        rgb_float[theme_ids == 0] = 0
        return np.clip(rgb_float, 0, 255)

    def get_smoothed_ids(
            self, theme_ids_2d: np.ndarray, smoothing_specs: Dict[str, Any]
    ) -> np.ndarray:
        """
        Resolves aliasing and precedence using an explicit set of smoothing rules.

        Args:
            theme_ids_2d: The raw categorical raster.
            smoothing_specs: A dictionary of label -> ThemeSmoothingSpec definitions.
        """
        if theme_ids_2d is None or not np.any(theme_ids_2d):
            return theme_ids_2d

        return self.get_smooth_theme(
            theme_ids_2d, self.label_to_id, smoothing_specs
        )

    @staticmethod
    def get_smooth_theme(theme_2d, label_to_id, smoothing_profiles):
        """
        The 'Melt and Grow' Algorithm.
        Processes categories in order of precedence to resolve spatial collisions.
        """
        theme = median_filter(theme_2d, size=3)
        present_ids = np.unique(theme)

        smoothed = theme.copy()
        background_mask = (theme == 0)
        all_labels = list(label_to_id.keys())

        # Determine processing order based on explicit precedence settings
        def get_prof(lbl):
            return smoothing_profiles.get(lbl, smoothing_profiles.get("_default_"))

        # Low precedence categories are processed (and potentially overwritten) first
        order = sorted(all_labels, key=lambda l: get_prof(l).precedence if get_prof(l) else 0)

        for label in order:
            val = label_to_id.get(label)
            if val not in present_ids or val == 0:
                continue

            prof = get_prof(label)
            if not prof: continue

            # Create a soft probability ramp (Melt)
            mask = (theme == val)
            mask = binary_fill_holes(mask)
            melted = gaussian_filter(mask.astype(np.float32), sigma=prof.smoothing_radius)

            # Resolve which lower-precedence pixels can be stolen by this category
            can_overwrite = np.zeros_like(background_mask, dtype=bool)
            for other_label in all_labels:
                other_val = label_to_id.get(other_label)
                if other_val is None or other_val == val: continue
                if get_prof(other_label).precedence < prof.precedence:
                    can_overwrite |= (smoothed == other_val)

            # Expand the category (Grow) based on the melted threshold
            grow_mask = (melted > prof.expansion_weight) & (background_mask | can_overwrite)
            smoothed[grow_mask] = val

        return smoothed


def refine_organic_signal(mask, blur_px, noise_amp, noise_id, contrast, max_opacity, ctx, name):
    """
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
