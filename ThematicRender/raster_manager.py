import rasterio
import numpy as np
from typing import Dict, Optional, Set, Tuple, Any
from contextlib import ExitStack
from dataclasses import dataclass

from ThematicRender.keys import DriverKey
from ThematicRender.config_mgr import ConfigMgr
from ThematicRender.ipc_blocks import DriverBlockRef, BlockPool, Window

@dataclass(frozen=True, slots=True)
class WindowRead:
    """A possibly-expanded read window and slices to crop back to the requested window."""
    read_window: Window
    inner_slices: Tuple[slice, slice]

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
        inner_slices=(slice(inner_row0, inner_row1), slice(inner_col0, inner_col1))
    )

class RasterManager:
    def __init__(self, cfg: ConfigMgr, required_drivers: Set[DriverKey], anchor_key: DriverKey):
        self.cfg = cfg
        self.required_drivers = required_drivers
        self.anchor_key = anchor_key
        self.sources: Dict[DriverKey, rasterio.DatasetReader] = {}
        self._stack = ExitStack()

    def __enter__(self):
        """Open the drivers using the resolved paths in ConfigMgr."""
        print("🔓 Opening Input Drivers...")
        for dkey in self.required_drivers:
            # Use the new ConfigMgr.path() accessor
            p = self.cfg.path(dkey.value)

            if not p:
                raise FileNotFoundError(f"Required driver '{dkey.value}' path missing in config.")
            if not p.exists():
                raise FileNotFoundError(f"Driver file not found: {p}")

            try:
                self.sources[dkey] = self._stack.enter_context(rasterio.open(p))
                status = "(ANCHOR)" if dkey == self.anchor_key else "        "
                print(f"   🔹 {status} {dkey.value.ljust(12)} -> {p.name}")
            except Exception as e:
                raise ValueError(f"Failed to open driver {dkey.value} at {p}: {e}")

        if self.anchor_key not in self.sources:
            raise RuntimeError(f"Anchor driver '{self.anchor_key.value}' failed to open.")

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._stack.close()

    @property
    def anchor_src(self) -> rasterio.DatasetReader:
        """The source that defines master dimensions and CRS."""
        return self.sources[self.anchor_key]

    def read_driver_block_ref(
            self, key: "DriverKey", src: rasterio.DatasetReader, window: Window, *,
            halo_override: Optional[int] = None, pool: Any) -> DriverBlockRef:

        dspec = self.cfg.get_spec(key)
        halo = halo_override if halo_override is not None else dspec.halo_px

        # 1. Coordinate Expansion
        win_read = _expand_window_for_halo(
            window, halo_px=halo, width=src.width, height=src.height
        )

        # 2. Value Read
        fill = src.nodata if src.nodata is not None else 0
        raw = src.read(1, window=win_read.read_window, boundless=True, fill_value=fill)

        # 3. Dtype Standardization
        val = raw.astype("float32", copy=False) if dspec.dtype != np.uint8 else raw
        h, w = val.shape

        # 4. Validity Mask Construction (Strictly 1, H, W)
        if src.count in (2, 4):
            # Read alpha band (2 for grayscale+alpha, 4 for RGBA)
            alpha_raw = src.read(src.count, window=win_read.read_window, boundless=True, fill_value=0)
            valid = (alpha_raw.astype("float32", copy=False) / 255.0)[np.newaxis, ...]
        else:
            valid = np.ones((1, h, w), dtype="float32")

        # 5. Combine with NoData masking (Strictly 1, H, W)
        if src.nodata is not None:
            # [np.newaxis, ...] creates (1, H, W)
            nodata_mask = (raw != src.nodata).astype("float32")[np.newaxis, ...]
            valid *= nodata_mask

        # 6. Write to Shared Memory Pool
        pad_value = float(fill) if dspec.dtype != np.uint8 else int(fill)
        slot_id = pool.acquire()

        try:
            return pool.write(
                slot_id=slot_id,
                value=val,     # (H, W) -> will be coerced to (1, H, W) by pool.write
                valid=valid,   # (1, H, W)
                inner_slices=win_read.inner_slices,
                pad_value=pad_value,
                pad_valid=0.0
            )
        except Exception:
            pool.release(slot_id)
            raise