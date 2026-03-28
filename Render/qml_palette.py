from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Iterable, Tuple, Final, Mapping, Optional
import xml.etree.ElementTree as ET

import numpy as np

RGB = Tuple[int, int, int]
RGBA = Tuple[int, int, int, int]

DEFAULT_LUT_SIZE = 256
DEFAULT_ALPHA = 255
TAG_PALETTE_ENTRY = "paletteEntry"


class QmlPaletteError(ValueError):
    """Raised when a QML style is missing required palette content."""


_LABEL_NORM_RE: Final[re.Pattern[str]] = re.compile(r"\s+")


def _norm_label(s: str) -> str:
    s2 = (s or "").strip().lower()
    s2 = _LABEL_NORM_RE.sub(" ", s2)
    return s2


@dataclass(frozen=True, slots=True)
class QmlPaletteEntry:
    value: int
    label: str
    color_hex: str  # "#rrggbb"
    alpha: int  # 0..255


@dataclass(frozen=True, slots=True)
class QmlPalette:
    """QGIS paletted raster style: value -> (label,color,alpha) and label -> value."""
    entries_by_value: Mapping[int, QmlPaletteEntry]
    value_by_label: Mapping[str, int]  # normalized label -> value

    @classmethod
    def load(cls, path: Path) -> "QmlPalette":
        """Load a QGIS QML paletted raster style.

        Args:
            path: Path to a .qml file exported from QGIS for a paletted raster.

        Returns:
            Parsed `QmlPalette`.

        Raises:
            QmlPaletteError: If the QML does not contain a paletted raster palette.
        """
        return load_qml_palette(path)

    def value_for_label(self, label: str) -> int:
        """Return palette value for a label (case/whitespace-insensitive)."""
        key = _norm_label(label)
        if key not in self.value_by_label:
            available = ", ".join(sorted(self.value_by_label.keys()))
            raise KeyError(f"QML label '{label}' not found. Available: {available}")
        return int(self.value_by_label[key])

    def values_for_labels(self, labels: set[str]) -> set[int]:
        """Return palette values for a set of labels."""
        return {self.value_for_label(lbl) for lbl in labels}

    def build_lut_rgb(
            self, *, size: int = DEFAULT_LUT_SIZE, fill_rgb: RGB = (0, 0, 0), ) -> np.ndarray:
        """Build dense RGB LUT for uint8 index rasters.

        Returns:
            (size, 3) uint8 LUT.
        """
        if size <= 0:
            raise ValueError(f"LUT size must be > 0, got {size}")

        lut = np.empty((size, 3), dtype=np.uint8)
        lut[:, :] = np.asarray(fill_rgb, dtype=np.uint8)

        for v, entry in self.entries_by_value.items():
            if 0 <= int(v) < size:
                rgb = _parse_color_attr(entry.color_hex)
                if rgb is not None:
                    lut[int(v), :] = np.asarray(rgb, dtype=np.uint8)

        return lut

    def build_lut_rgba(
            self, *, size: int = DEFAULT_LUT_SIZE,
            fill_rgba: RGBA = (0, 0, 0, 255), ) -> np.ndarray:
        """Build dense RGBA LUT for uint8 index rasters (QGIS-consistent alpha).

        Returns:
            (size, 4) uint8 LUT.
        """
        if size <= 0:
            raise ValueError(f"LUT size must be > 0, got {size}")

        lut = np.empty((size, 4), dtype=np.uint8)
        lut[:, :] = np.asarray(fill_rgba, dtype=np.uint8)

        for v, entry in self.entries_by_value.items():
            if 0 <= int(v) < size:
                rgb = _parse_color_attr(entry.color_hex)
                if rgb is None:
                    continue
                a = int(np.clip(int(entry.alpha), 0, 255))
                lut[int(v), :3] = np.asarray(rgb, dtype=np.uint8)
                lut[int(v), 3] = np.uint8(a)

        return lut


def load_qml_palette(path: Path) -> QmlPalette:
    """Parse a QGIS QML raster palette (paletted renderer) into a lookup object."""
    root = ET.parse(str(path)).getroot()

    # Find <colorPalette> anywhere under <rasterrenderer type="paletted">
    color_palette = root.find(".//rasterrenderer[@type='paletted']/colorPalette")
    if color_palette is None:
        raise ValueError("QML does not contain paletted rasterrenderer/colorPalette.")

    by_val: dict[int, QmlPaletteEntry] = {}
    by_label: dict[str, int] = {}

    for pe in color_palette.findall("paletteEntry"):
        v_raw = pe.get("value")
        c_raw = pe.get("color")
        a_raw = pe.get("alpha", "255")
        l_raw = pe.get("label", "")

        if v_raw is None or c_raw is None:
            continue

        v = int(v_raw)
        a = int(a_raw)
        label = str(l_raw)

        entry = QmlPaletteEntry(value=v, label=label, color_hex=str(c_raw), alpha=a)
        by_val[v] = entry

        norm = _norm_label(label)
        # 1. Skip if label is empty
        if not norm:
            continue

        # 2. Skip if the label is just the number itself (QGIS placeholder)
        # e.g. label="10" value="10"
        if norm == str(v):
            continue

        if norm:
            # If duplicate labels exist, last one wins (rare but sane)
            by_label[norm] = v

    return QmlPalette(entries_by_value=by_val, value_by_label=by_label)


def _validate_is_paletted_raster(root: ET.Element, qml_path: Path) -> None:
    """Fail-fast: ensure this QML is a paletted raster renderer."""
    rr = root.find(".//rasterrenderer")
    if rr is None:
        raise QmlPaletteError(f"QML has no <rasterrenderer>: {qml_path}")

    rr_type = (rr.attrib.get("type") or "").strip().lower()
    if rr_type != "paletted":
        raise QmlPaletteError(
            f"QML rasterrenderer type must be 'paletted', got '{rr_type or '<missing>'}': "
            f"{qml_path}"
        )

    cp = rr.find(".//colorPalette")
    if cp is None:
        raise QmlPaletteError(f"QML paletted renderer missing <colorPalette>: {qml_path}")


def _iter_palette_entries(root: ET.Element) -> Iterable[ET.Element]:
    """Yield <paletteEntry> elements from QML (tag may be namespaced)."""
    for elem in root.iter():
        tag = elem.tag.split("}")[-1]  # handle namespaces
        if tag == TAG_PALETTE_ENTRY:
            yield elem


def _parse_int_attr(elem: ET.Element, attr: str) -> Optional[int]:
    """Parse an integer attribute like value="123"."""
    raw = (elem.attrib.get(attr) or "").strip()
    if not raw:
        return None
    try:
        return int(float(raw))
    except ValueError:
        return None


def _parse_alpha_attr(raw: Optional[str]) -> int:
    """Parse alpha, defaulting to 255."""
    s = (raw or "").strip()
    if not s:
        return DEFAULT_ALPHA
    try:
        return int(np.clip(int(float(s)), 0, 255))
    except ValueError:
        return DEFAULT_ALPHA


def _parse_color_attr(raw: str) -> Optional[RGB]:
    """Parse QML color strings.

    Supports:
      - '#RRGGBB'
      - 'R,G,B' or 'R,G,B,A'  (we ignore A here because alpha is separate in QML)

    Args:
        raw: Color attribute string.

    Returns:
        (r,g,b) or None.
    """
    s = (raw or "").strip()
    if not s:
        return None

    if s.startswith("#"):
        hx = s[1:]
        if len(hx) != 6:
            return None
        try:
            return int(hx[0:2], 16), int(hx[2:4], 16), int(hx[4:6], 16)
        except ValueError:
            return None

    parts = [p.strip() for p in s.split(",")]
    if len(parts) >= 3:
        try:
            r, g, b = (int(float(parts[0])), int(float(parts[1])), int(float(parts[2])))
            return int(np.clip(r, 0, 255)), int(np.clip(g, 0, 255)), int(np.clip(b, 0, 255))
        except ValueError:
            return None

    return None
