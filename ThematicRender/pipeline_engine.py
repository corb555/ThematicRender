import os
from pathlib import Path
import pickle
import traceback
from typing import Optional, TypeVar, Dict, Tuple

import numpy as np
import rasterio
from rasterio.windows import Window
from tqdm import tqdm

from ThematicRender.config_mgr import ConfigMgr, derive_resources
from ThematicRender.factor_engine import FactorEngine
from ThematicRender.ipc_blocks import PoolSpec
from ThematicRender.keys import DriverKey, SurfaceKey
from ThematicRender.noise_registry import NoiseRegistry
from ThematicRender.pipeline_tasks import read_task, render_task, write_task, WriterContext, \
    WorkerContext, ReaderContext
from ThematicRender.raster_manager import RasterManager, BlockPool
from ThematicRender.settings import (FACTOR_SPECS, SURFACE_SPECS)
from ThematicRender.compositing_engine import CompositingEngine
from ThematicRender.surface_engine import SurfaceEngine
from ThematicRender.theme_registry import ThemeRegistry
from ThematicRender.utils import TimerStats, ERR_PREFIX

T = TypeVar("T")  # DriverBlock or rasterio DatasetReader, etc.

from rasterio.windows import union


# pipeline_engine.py
class PipelineEngine:
    def __init__(
            self, cfg: ConfigMgr, pipeline, percent, row, col, singlethread: bool = True
    ) -> None:
        self.pool_map = None
        self.singlethread = singlethread
        self.cfg = cfg
        self.preview = True
        self.tmr = TimerStats()
        self.surfaces = SurfaceEngine(cfg)
        self.themes = ThemeRegistry(cfg)
        self.compositor = CompositingEngine(self.tmr)
        self.resources = derive_resources(
            cfg=cfg, pipeline=pipeline, factor_specs=FACTOR_SPECS, surface_specs=SURFACE_SPECS
        )
        self.noise_registry = NoiseRegistry(cfg, self.resources.noise_profiles)
        self.factors = FactorEngine(
            cfg=self.cfg, themes=self.themes, noise_registry=self.noise_registry,
            factor_specs=FACTOR_SPECS, resources=self.resources, timer=self.tmr
        )
        self.pipeline = pipeline

        # PREVIEW CONFIG
        if percent is not None:
            self.percent = float(percent)
            self.row = float(row)
            self.col = float(col)
        else:
            self.percent = 0.0

    def process_rasters(self) -> None:
        out_path = self.cfg.output_path("output")

        with RasterManager(self.cfg, self.resources.drivers, self.resources.anchor_key) as io:
            profile = self._build_output_profile(io)

            write_offset_row = 0
            write_offset_col = 0
            envelope = None

            # PREVIEW MODE: Relative Focal Point logic
            if self.percent is not None and self.percent != 0 and self.percent != 1.0:
                # 1. Calculate relative focal point (default to center if None)
                rel_x = self.col if self.col is not None else 0.5
                rel_y = self.row if self.row is not None else 0.5

                # 2. Derive the envelope based on relative position
                envelope = self._calculate_preview_window(
                    io.anchor_src,
                    percent=self.percent,
                    rel_x=rel_x,
                    rel_y=rel_y
                )

                if envelope is None:
                    print("Preview area is empty.")
                    return

                profile.update({
                    "width": int(envelope.width),
                    "height": int(envelope.height),
                    "transform": io.anchor_src.window_transform(envelope),
                })

                write_offset_row = int(envelope.row_off)
                write_offset_col = int(envelope.col_off)

            with rasterio.open(out_path, "w", **profile) as dst:
                self.pool_map = self._create_pool_map(io=io, dst=dst)

                # Windows must match dst tiling so pool sizing matches
                dst_windows = [w for _, w in dst.block_windows(1)]

                # Always process in GLOBAL coords
                if envelope is not None:
                    win_list = [Window(
                        col_off=int(w.col_off) + write_offset_col,
                        row_off=int(w.row_off) + write_offset_row, width=int(w.width),
                        height=int(w.height), ) for w in dst_windows]
                    print(f"🔶 PREVIEW MODE: Resizing to {profile['width']}x{profile['height']}")
                else:
                    win_list = dst_windows

                # Build contexts
                reader_ctx = ReaderContext(io=io, pool_map=self.pool_map)

                worker_ctx = WorkerContext(
                    cfg=self.cfg,
                    pool_map=self.pool_map, factors_engine=self.factors,
                    surfaces_engine=self.surfaces, themes=self.themes, compositor=self.compositor,
                    pipeline=self.pipeline, anchor_key=self.resources.anchor_key,
                    surface_inputs=self.resources.surface_inputs, resources=self.resources,
                    noise_registry=self.noise_registry, )

                writer_ctx = WriterContext(
                    dst=dst, pool_map=self.pool_map, write_offset_row=write_offset_row,
                    write_offset_col=write_offset_col
                )

                """  for  ctx in  [reader_ctx, worker_ctx, writer_ctx]:
                    try:
                        pickle.dumps(ctx)
                        print("✅ multiprocess-safe!")
                    except Exception as e:
                        print(f">> Pickling Error: {e}")"""

                if self.singlethread:
                    # SINGLE-THREAD MAIN PROCESSING
                    for seq, window in enumerate(tqdm(win_list, desc="Rendering")):
                        try:
                            work_packet = read_task(seq=seq, window=window, ctx=reader_ctx)
                            result_packet = render_task(packet=work_packet, ctx=worker_ctx)
                            write_task(packet=result_packet, ctx=writer_ctx)
                        except Exception:
                            print(f"\n❌ Pipeline Error for Global Window {window}")
                            traceback.print_exc()
                            raise
                else:
                    raise NotImplementedError("Multiprocessor Support Not Available")

    def _create_pool_map(self, *, io: "RasterManager", dst: "rasterio.DatasetWriter") -> Dict[
        "DriverKey", BlockPool]:
        """Create per-driver BlockPools sized for (anchor block size + unified max halo)."""
        # Anchor-driven block size (height, width)
        try:
            block_h, block_w = dst.block_shapes[0]
        except Exception as e:
            raise RuntimeError(f"Failed to read dst.block_shapes[0]: {e}")

        # Unified halo across all opened drivers (must match RasterManager.read_window logic)
        max_halo = 0
        for dkey in io.sources.keys():
            spec = self.cfg.driver_spec(dkey)
            max_halo = max(max_halo, int(spec.halo_px))

        # Fixed slot shape = anchor block expanded by halo on all sides
        pool_h = int(block_h + 2 * max_halo)
        pool_w = int(block_w + 2 * max_halo)

        # Slot count: enough to cover “read + in-flight compute + write”
        cpu_n = os.cpu_count() or 4
        slots = max(8, min(64, 2 * cpu_n))

        pool_map: Dict["DriverKey", BlockPool] = {}

        for dkey in io.sources.keys():
            dspec = self.cfg.driver_spec(dkey)

            # Match your read behavior: thematic stays uint8, everything else float32
            value_dtype = np.uint8 if dspec.dtype == np.uint8 else np.float32

            spec = PoolSpec(
                value_shape=(pool_h, pool_w), value_dtype=np.dtype(value_dtype),
                valid_shape=(pool_h, pool_w, 1), valid_dtype=np.dtype(np.float32), )
            pool_map[dkey] = BlockPool(spec=spec, slots=slots)

        return pool_map

    @staticmethod
    def _calculate_preview_window(src, percent: float, rel_x: float, rel_y: float) -> Window:
        """
        Calculates a global Window based on normalized focal points (0.0-1.0).
        Handles non-tiled source files gracefully.
        """
        full_w, full_h = src.width, src.height

        # 1. Determine target size based on percent
        target_w = int(full_w * percent)
        target_h = int(full_h * percent)

        # 2. Determine focal point in pixels
        focal_x = int(full_w * rel_x)
        focal_y = int(full_h * rel_y)

        # 3. Calculate top-left corner
        col_off = focal_x - (target_w // 2)
        row_off = focal_y - (target_h // 2)

        # 4. Snap to Block Size (Safety Logic)
        # We only snap to the file's blocks if they are smaller than the full image
        # Otherwise, we snap to a standard 256 boundary to help the processing engine.
        b_h, b_w = src.block_shapes[0]
        snap_w = b_w if b_w < full_w else 256
        snap_h = b_h if b_h < full_h else 256

        col_off = (max(0, col_off) // snap_w) * snap_w
        row_off = (max(0, row_off) // snap_h) * snap_h

        # 5. Final Clamp: Ensure the window is within the raster bounds
        # We use (full - target) to ensure the window doesn't hang off the right/bottom
        col_off = max(0, min(col_off, full_w - target_w))
        row_off = max(0, min(row_off, full_h - target_h))

        # 6. Final Window Creation
        # Re-verify width/height to handle extreme edges
        actual_w = min(target_w, full_w - col_off)
        actual_h = min(target_h, full_h - row_off)

        return Window(col_off, row_off, actual_w, actual_h)

    # ------------------------------------------------
    # Helpers
    # ------------------------------------------------
    @staticmethod
    def preview_list(dst, all_windows, percent: int, row: int = 0, col: int = 0):
        """
        Returns all windows that intersect the requested preview block.
        """
        # 1. Calculate the pixel boundaries of the preview area
        percent = float(percent)
        row = int(row)
        col = int(col)
        block_h = int(dst.height * (percent / 100.0))
        block_w = int(dst.width * (percent / 100.0))

        min_row, max_row = row * block_h, (row + 1) * block_h
        min_col, max_col = col * block_w, (col + 1) * block_w

        # 2. Filter using INTERSECTION logic
        # A window is included if any part of it overlaps the [min, max] range
        win_list = []
        for w in all_windows:
            # Check vertical overlap
            v_overlap = (w.row_off < max_row) and ((w.row_off + w.height) > min_row)
            # Check horizontal overlap
            h_overlap = (w.col_off < max_col) and ((w.col_off + w.width) > min_col)

            if v_overlap and h_overlap:
                win_list.append(w)

        # 3. Handle empty results
        if not win_list:
            context = (f"Target Area: Rows[{int(min_row)}-{int(max_row)}], Cols[{int(min_col)}-"
                       f"{int(max_col)}]")
            if min_row >= dst.height or min_col >= dst.width:
                print(f"⚠️  Preview block [{row}, {col}] is entirely outside the map bounds.")
            else:
                print(
                    f"⚠️  No tiles found intersecting {context}. (Map size: {dst.width}x"
                    f"{dst.height})"
                )
            return [], None

        # 4. Calculate the bounding envelope
        envelope = win_list[0]
        for w in win_list[1:]:
            envelope = union(envelope, w)

        print(f"🔶 Preview block [{row}, {col}] selected ({len(win_list)} tiles intersecting)")
        return win_list, envelope

    @staticmethod
    def _build_output_profile(io: RasterManager) -> dict:
        """Metadata derived strictly from the Anchor driver."""
        anchor = io.anchor_src
        return {
            "driver": "GTiff", "height": anchor.height, "width": anchor.width, "count": 3,
            "dtype": "uint8", "crs": anchor.crs, "transform": anchor.transform, "tiled": True,
            "blockxsize": 256, "blockysize": 256, "compress": "deflate", "predictor": 2,
            "nodata": None,
        }

    def _inner_slices_from_anchor(self, driver_blocks: dict) -> Tuple[slice, slice]:
        """Returns the pre-calculated cropping slices for the anchor driver."""
        anchor_blk = self.get_driver(driver_blocks, self.resources.anchor_key, "inner_slices")
        return anchor_blk.inner_slices or (slice(None), slice(None))

    @staticmethod
    def _slice_surfaces(
            surfaces: Dict["SurfaceKey", np.ndarray], slices: Tuple[slice, slice]
    ) -> Dict["SurfaceKey", np.ndarray]:
        rs, cs = slices
        return {k: v[rs, cs, :] for k, v in surfaces.items()}

    @staticmethod
    def _slice_factors(factors: dict, slices: tuple) -> dict:
        """
        Slices all factor arrays from (320, 320, 1) to (256, 256, 1).
        """
        cropped = {}
        for k, v in factors.items():
            if isinstance(v, np.ndarray):
                if v.ndim == 3:
                    # Explicitly slice H and W, keep Channel
                    cropped[k] = v[slices[0], slices[1], :]
                elif v.ndim == 2:
                    cropped[k] = v[slices]
                else:
                    cropped[k] = v
            else:
                cropped[k] = v
        return cropped

    @staticmethod
    def get_driver(
            drivers: dict[DriverKey, Optional[T]], driver_key: DriverKey, context: str, ) -> T:
        """Return a required driver or raise with a helpful message."""
        driver = drivers.get(driver_key)
        if driver is None:
            raise RuntimeError(
                f"{ERR_PREFIX} Driver '{driver_key.value}' is missing. Required for {context}"
            )
        return driver

    def load_ramps(self) -> None:
        """Load ramps and merge paths back into config for validation."""
        ramp_files = self.surfaces.load_surface_ramps(self.resources)
        self.cfg.merge_files({k: Path(v) for k, v in ramp_files.items()})

        for msg in getattr(self.surfaces, "load_warnings", []):
            print(f"⚠️  {msg}")
