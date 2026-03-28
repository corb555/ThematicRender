from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Any, List, Dict, Tuple

import numpy as np
# io_manager.py
import rasterio
from rasterio.windows import Window
from Common.ipc_packets import DriverBlockRef, WKR_TIMEOUT
from Common.keys import DriverKey, DriverRndrSpec
from Pipeline.render_config import JobManifest
from Pipeline.shared_memory import SlotRegistry


# io_manager.py
class IOSystem:
    @staticmethod
    def initialize_physical_output(out_path: Path, profile: dict) -> List[Window]:

        print(f"🏗️ [IOSystem] Initializing physical output: {out_path.name}")

        # Open in 'w' mode and close immediately to commit the header to disk.
        with rasterio.open(out_path, "w", **profile) as _:
            pass

        # Re-open in 'r' mode to see how the blocks are laid out.
        with rasterio.open(out_path, "r") as reader_dst:
            # We assume band 1 is representative of the whole file grid
            win_list = [window for _, window in reader_dst.block_windows(1)]

        if not win_list:
            raise ValueError(f"❌ Geometry Error: Could not determine windows for {out_path}")

        print(f"📏 [IOSystem] Output grid calculated: {len(win_list)} tiles.")
        return win_list

    @staticmethod
    def get_tile_geometry_refs(
            manifest: 'JobManifest', window: Window, registry: 'SlotRegistry'
    ) -> Dict['DriverKey', DriverBlockRef]:
        """
        Calculates the exact Shared Memory slices and offsets for a specific tile.

        This centralizes the 'Halo Math' so the Dispatcher doesn't have to
        calculate geometry internally.
        """
        max_halo = manifest.render_cfg.get_max_halo()
        refs = {}

        try:
            # We use a context manager for IOManager to safely probe the source drivers
            with IOManager(
                    manifest.render_cfg, manifest.resources.drivers, manifest.resources.anchor_key
            ) as io:
                for dkey in manifest.resources.drivers:
                    # Get or allocate from registry (handled by Dispatcher, but used here for ID)
                    slot_id, _ = registry.get_or_allocate(dkey, window)

                    # Use IOManager's utility to find the exact pixel slices
                    # including halo buffers

                    geom = io.get_geometry_metadata(dkey, window, max_halo)

                    refs[dkey] = DriverBlockRef(
                        slot_id=slot_id, data_h_w=geom.full_h_w, inner_slices=geom.inner_slices
                    )
        except Exception as e:
            raise FileNotFoundError(f"Dkey: {dkey} {e}")

        return refs

    @staticmethod
    def ensure_build_directory(path: Path):
        """Utility to ensure the output directory exists before rendering."""
        if not path.parent.exists():
            path.parent.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def get_anchor_windows(manifest: 'JobManifest', anchor_key: 'DriverKey') -> List[Window]:
        """
        Open the anchor dataset briefly to extract its internal block structure.
        This defines the tile grid for a full (non-preview) render.
        """
        path = manifest.resources.drivers[anchor_key]

        with rasterio.open(path) as src:
            # Use the internal block windows of the anchor to ensure
            # we align perfectly with the source data storage on disk.
            win_list = [window for _, window in src.block_windows(1)]

        if not win_list:
            raise ValueError(f"❌ Geometry Error: Could not determine windows for anchor {path}")

        print(f"📏 [IOSystem] Full-Render grid calculated: {len(win_list)} tiles.")
        return win_list


@dataclass(frozen=True, slots=True)
class WindowRead:
    read_window: Window
    inner_slices: Tuple[slice, slice]
    full_h_w: Tuple[int, int]  # The dimensions of the read_window


def _expand_window_for_halo(
        window: Window, *, halo_px: int, width: int, height: int
) -> WindowRead:
    """Calculate an expanded coordinate window and inner crop slices."""
    col_off, row_off = int(window.col_off), int(window.row_off)
    w, h = int(window.width), int(window.height)

    if halo_px <= 0:
        return WindowRead(
            read_window=window, inner_slices=(slice(None), slice(None)), full_h_w=(h, w), )

    left = max(0, col_off - halo_px)
    top = max(0, row_off - halo_px)
    right = min(width, col_off + w + halo_px)
    bottom = min(height, row_off + h + halo_px)

    read_w = Window(left, top, right - left, bottom - top)
    full_h_w = (int(read_w.height), int(read_w.width))

    inner_row0 = row_off - top
    inner_row1 = inner_row0 + h
    inner_col0 = col_off - left
    inner_col1 = inner_col0 + w

    return WindowRead(
        read_window=read_w,
        inner_slices=(slice(inner_row0, inner_row1), slice(inner_col0, inner_col1)),
        full_h_w=full_h_w, )


class IOManager:
    def __init__(self, render_cfg: Any, drivers: Dict[Any, Path], anchor_key: Any):
        self.render_cfg = render_cfg
        self.drivers = drivers
        self.anchor_key = anchor_key
        self.sources: Dict[Any, rasterio.DatasetReader] = {}
        self._stack = ExitStack()

    def __enter__(self):
        for dkey, path in self.drivers.items():
            try:
                self.sources[dkey] = self._stack.enter_context(rasterio.open(path))
            except Exception as e:
                raise IOError(f"Error for '{dkey}' path:'{path}'.     {str(e)}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._stack.close()

    def read_into_buffer(
            self, key: Any, window: Window, halo: int, out_data: np.ndarray,  # (Bands, H, W)
            out_mask: np.ndarray  # (Bands, H, W)
    ) -> None:
        src = self.sources[key]
        geom = get_read_geometry(window, halo, src.width, src.height)

        # 1. Capture the actual dimensions of this specific read
        # For edge tiles, these will be smaller than the 256px slot size
        h, w = int(geom.read_window.height), int(geom.read_window.width)

        # 2. Direct Data Read
        # We slice out_data[0, :h, :w] to ensure the destination
        # matches the read_window EXACTLY.
        fill = src.nodata if src.nodata is not None else 0
        src.read(
            1, window=geom.read_window, boundless=True, fill_value=fill, out=out_data[0, :h, :w]
        )

        # 3. Direct Mask Read/Generation
        if src.count in (2, 4):
            # Read Alpha channel directly into the SHM mask slice
            src.read(
                src.count, window=geom.read_window, boundless=True, fill_value=0,
                out=out_mask[0, :h, :w]  # <--- Slice here too
            )
            # Rescale 0-255 to 0.0-1.0 in-place (only on the valid slice)
            out_mask[0, :h, :w] /= 255.0
        else:
            # Initialize the valid portion of the mask to 1.0 (fully opaque)
            out_mask[0, :h, :w].fill(1.0)

        # 4. Apply NoData Overlay (In-Place)
        if src.nodata is not None:
            # We compare the data already in SHM to the nodata value
            # Only perform this on the :h, :w slice to prevent errors
            target_data = out_data[0, :h, :w]
            out_mask[0, :h, :w] *= (target_data != src.nodata)

    @property
    def anchor_src(self) -> rasterio.DatasetReader:
        """Returns the primary dataset used for spatial reference."""
        return self.sources[self.anchor_key]

    def read_driver_block_ref(
            self, key: Any, src: rasterio.DatasetReader, window: Window, *,
            halo_override: Optional[int] = None, pool: Any
    ) -> DriverBlockRef:
        # 1. SPATIAL SETUP
        drv_rndr_spec: DriverRndrSpec = self.render_cfg.get_spec(key)
        halo = halo_override if halo_override is not None else drv_rndr_spec.halo_px

        win_read = _expand_window_for_halo(
            window, halo_px=halo, width=src.width, height=src.height
        )

        # 2. DISK I/O (Data)
        fill = src.nodata if src.nodata is not None else 0
        raw = src.read(1, window=win_read.read_window, boundless=True, fill_value=fill)

        # 3. DTYPE STANDARDIZATION
        # Promote to float32 for math processing unless it is 8-bit categorical data
        if drv_rndr_spec.dtype != np.uint8:
            data = raw.astype("float32", copy=False)
        else:
            data = raw
        h, w = data.shape

        # 4. MASK CONSTRUCTION (Presence Sensing)
        # Determine valid pixels using Alpha bands (2nd or 4th band) or NoData values
        if src.count in (2, 4):
            # Extract standard Alpha channel
            alpha_raw = src.read(
                src.count, window=win_read.read_window, boundless=True, fill_value=0
            )
            mask = (alpha_raw.astype("float32", copy=False) / 255.0)[np.newaxis, ...]
        else:
            # Default to solid mask (1.0)
            mask = np.ones((1, h, w), dtype="float32")

        # Overlay NoData mask if defined in the GeoTIFF metadata
        if src.nodata is not None:
            nodata_mask = (raw != src.nodata).astype("float32")[np.newaxis, ...]
            mask *= nodata_mask

        # 5. STORAGE HANDOFF (Shared Memory)
        # Acquire a binary slot from the pool (Blocks if pool is exhausted)
        try:
            slot_id = pool.acquire(timeout=WKR_TIMEOUT)
        except Exception:
            # This captures queue.Empty if using a multiprocessing.Queue internally
            raise RuntimeError(
                f"SHM Pool Exhausted for driver '{key}'. "
            )

        try:
            # Commit the 2D local arrays into the 4D Shared Memory buffer
            # Return the lightweight reference for the worker processes
            return pool.write(
                slot_id=slot_id, data=data, mask=mask, inner_slices=win_read.inner_slices,
                # Metadata used for debugging/padding
                pad_data=(float(fill) if drv_rndr_spec.dtype != np.uint8 else int(fill)),
                pad_mask=0.0
            )
        except Exception:
            # Restore slot availability if the write operation fails
            pool.release(slot_id)
            raise

    def get_geometry_metadata(self, key, window, halo):
        src = self.sources[key]
        return get_read_geometry(window, halo, src.width, src.height)


def get_read_geometry(window: Window, halo_px: int, src_w: int, src_h: int) -> WindowRead:
    """Pure math to determine what to read and how to crop it."""
    if halo_px <= 0:
        return WindowRead(
            window, (slice(None), slice(None)), (int(window.height), int(window.width))
        )

    col_off, row_off = int(window.col_off), int(window.row_off)
    w, h = int(window.width), int(window.height)

    left = max(0, col_off - halo_px)
    top = max(0, row_off - halo_px)
    right = min(src_w, col_off + w + halo_px)
    bottom = min(src_h, row_off + h + halo_px)

    read_w = Window(left, top, right - left, bottom - top)

    inner_row0 = row_off - top
    inner_row1 = inner_row0 + h
    inner_col0 = col_off - left
    inner_col1 = inner_col0 + w

    return WindowRead(
        read_window=read_w,
        inner_slices=(slice(inner_row0, inner_row1), slice(inner_col0, inner_col1)),
        full_h_w=(int(read_w.height), int(read_w.width))
    )
