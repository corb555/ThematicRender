from typing import Dict, Optional, Any

import numpy as np

from ThematicRender.config_mgr import THEME_SMOOTHING_PROFILES
from ThematicRender.keys import DriverKey
from ThematicRender.qml_palette import QmlPalette

#theme_registry.py

class ThemeRegistry:
    """
    Thematic Engine: Maps categorical IDs to spatial RGB surfaces.
    CONTRACT: get_theme_surface returns (H, W, 3) float32 or None.
    """

    def __init__(self, cfg: Any):
        self.cfg = cfg
        self.qml_palette: Optional[Any] = None
        self.lut_rgb: Optional[np.ndarray] = None  # (256, 3) uint8

    def load_theme_style(self, context: str = "Initialization") -> None:
        """Loads the QML style and builds the RGB LUT."""
        # REMOVE the 'if not self.cfg.factor_on' check.
        # If we are here, the pipeline needs it.
        try:
            qml_path = self.cfg.input_path("theme_qml", context=f"{context} (THEME QML style)")
            self.qml_palette = QmlPalette.load(qml_path)
            self.lut_rgb = self.qml_palette.build_lut_rgb()
        except Exception as e:
            print(f"⚠️ Theme Registry: Could not load QML ({e}). Style provider may fail.")


    def get_theme_surface(self, val_2d: dict, context: str) -> Optional[np.ndarray]:
        #  Look up 2D array directly from dict
        idx = val_2d.get(DriverKey.THEME)

        #  idx is  guaranteed (H, W) by the render_task firewall
        h, w = idx.shape[0], idx.shape[1]

        # LUT lookup
        rgb_u8 = self.lut_rgb[idx.astype(np.uint8)]
        rgb_u8[idx == 0] = 0

        return rgb_u8.astype("float32", copy=False)

    @property
    def label_to_val(self) -> Dict[str, int]:
        """Returns normalized label-to-ID mapping from the QML palette."""
        if not self.qml_palette:
            return {}
        return self.qml_palette.value_by_label

    def get_smoothed_ids(self, theme_ids_2d: np.ndarray) -> np.ndarray:
        """
        Pre-processor: Converts blocky low-res IDs into generalized,
        smooth-boundary IDs using THEME_SMOOTHING_PROFILES.
        """
        if theme_ids_2d is None or not np.any(theme_ids_2d):
            return theme_ids_2d

        # The math logic lives in the FactorLibrary to keep the Registry clean
        from .factor_library import FactorLibrary

        return FactorLibrary.get_smooth_theme(
            theme_ids_2d,
            self.label_to_val,
            THEME_SMOOTHING_PROFILES
        )
