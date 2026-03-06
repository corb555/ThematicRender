from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import Set, Optional, Dict, Tuple
import warnings

import numpy as np
import rasterio
from rasterio.windows import Window

from ThematicRender.config_mgr import ConfigMgr, DriverKey
from ThematicRender.ipc_blocks import BlockPool, DriverBlockView, DriverBlockRef


# raster_manager.py
@dataclass(frozen=True, slots=True)
class WindowRead:
    """A possibly-expanded read window and slices to crop back to the requested window."""
    read_window: Window
    inner_slices: Tuple[slice, slice]  # slices into the read array for the original window


def _expand_window_for_halo(window: Window, *, halo_px: int, width: int, height: int) -> WindowRead:
    if halo_px <= 0:
        return WindowRead(read_window=window, inner_slices=(slice(None), slice(None)))

    col_off = int(window.col_off)
    row_off = int(window.row_off)
    w = int(window.width)
    h = int(window.height)

    left = max(0, col_off - halo_px)
    top = max(0, row_off - halo_px)
    right = min(width, col_off + w + halo_px)
    bottom = min(height, row_off + h + halo_px)

    read_w = Window(left, top, right - left, bottom - top)

    inner_row0 = row_off - top
    inner_row1 = inner_row0 + h
    inner_col0 = col_off - left
    inner_col1 = inner_col0 + w

    return WindowRead(
        read_window=read_w,
        inner_slices=(slice(inner_row0, inner_row1), slice(inner_col0, inner_col1)), )


class RasterManager:
    def __init__(
            self, cfg: ConfigMgr, required_drivers: Set[DriverKey], anchor_key: DriverKey
    ):
        self.cfg = cfg
        self.required_drivers = required_drivers
        self.anchor_key = anchor_key  # Decoupled from hardcoded DEM
        self.sources: Dict[DriverKey, rasterio.DatasetReader] = {}
        self._stack = ExitStack()

    def __enter__(self):
        """Open the drivers required by the pipeline spec."""
        for dkey in self.required_drivers:
            path = self.cfg.files.get(dkey.value)
            if not path:
                # If it's a required driver but has no path, we fail hard.
                raise FileNotFoundError(f"Required driver '{dkey.value}' path missing in config. Add it to the files: or "
                                        f"prefixed_files: section in the config file.")

            p = Path(path).expanduser()
            if not p.exists():
                raise FileNotFoundError(f"Driver file not found: {p}")

            try:
                self.sources[dkey] = self._stack.enter_context(rasterio.open(p))
                status = " (ANCHOR)" if dkey == self.anchor_key else ""
                print(f"Opened {dkey.value}{status}: {p.name}")
            except Exception as e:
                raise ValueError(f"Failed to open driver {dkey.value} at {p}: {e}")

        # Final validation: The anchor MUST be open
        if self.anchor_key not in self.sources:
            raise RuntimeError(f"Anchor driver '{self.anchor_key.value}' was not opened.")

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._stack.close()

    @property
    def anchor_src(self) -> rasterio.DatasetReader:
        """The source that defines dimensions, CRS, and Transform for the output."""
        return self.sources[self.anchor_key]

    def read_window(
            self, window: Window, *, pool_map: Dict["DriverKey", BlockPool], ) -> Dict[
        "DriverKey", DriverBlockView]:
        max_halo = 0
        for dkey in self.sources:
            spec = self.cfg.driver_spec(dkey)
            max_halo = max(max_halo, spec.halo_px)

        out: Dict["DriverKey", DriverBlockView] = {}
        for key, src in self.sources.items():
            pool = pool_map[key]
            ref = self.read_driver_block_ref(
                key, src, window, halo_override=max_halo, pool=pool
            )
            out[key] = pool.view(ref)
        return out

    def read_driver_block_ref(
            self, key: "DriverKey", src: rasterio.DatasetReader, window: Window, *,
            halo_override: Optional[int] = None, pool: BlockPool, ) -> DriverBlockRef:
        spec = self.cfg.driver_spec(key)
        halo = halo_override if halo_override is not None else spec.halo_px

        win_read = _expand_window_for_halo(
            window, halo_px=halo, width=src.width, height=src.height
        )

        # Read value band with boundless fill
        fill = src.nodata if src.nodata is not None else 0
        raw = src.read(1, window=win_read.read_window, boundless=True, fill_value=fill)

        # Standardize value dtype (keep thematic uint8)
        val = raw.astype("float32", copy=False) if spec.dtype != np.uint8 else raw

        h, w = val.shape

        # Build valid mask (HxWx1)
        if src.count in (2, 4):
            alpha_raw = src.read(
                src.count, window=win_read.read_window, boundless=True, fill_value=0
            )
            valid = (alpha_raw.astype("float32", copy=False) / 255.0)[..., np.newaxis]
        else:
            valid = np.ones((h, w, 1), dtype="float32")

        # Combine alpha with nodata mask if applicable
        if src.nodata is not None:
            nodata_mask = (raw != src.nodata).astype("float32")[..., np.newaxis]
            valid *= nodata_mask

        # Padding semantics for fixed-size pool slots
        pad_value = float(fill) if spec.dtype != np.uint8 else int(fill)

        slot_id = pool.acquire()
        try:
            return pool.write(
                slot_id, value=val, valid=valid, inner_slices=win_read.inner_slices,
                pad_value=pad_value, pad_valid=0.0, )
        except Exception:
            pool.release(slot_id)
            raise
