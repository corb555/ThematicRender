# pipeline_engine.py
import os
from pathlib import Path
import time
import traceback
from typing import Dict, Any

from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context, Process, Queue
import multiprocessing as mp
from contextlib import ExitStack

import numpy as np
import rasterio
from rasterio.windows import Window
from tqdm import tqdm

from ThematicRender.compositing_engine import CompositingEngine
from ThematicRender.config_mgr import ConfigMgr, derive_resources
from ThematicRender.factor_engine import FactorEngine
from ThematicRender.ipc_blocks import  PoolSpec, SharedMemoryPool
from ThematicRender.keys import DriverKey
from ThematicRender.noise_registry import NoiseRegistry
from ThematicRender.pipeline_tasks import ReaderContext, WorkerContext, WriterContext, read_task, \
    render_task, write_task, writer_worker_loop, render_worker_task, render_worker_loop
from ThematicRender.raster_manager import RasterManager
from ThematicRender.settings import FACTOR_SPECS, SURFACE_SPECS
from ThematicRender.surface_engine import SurfaceEngine
from ThematicRender.theme_registry import ThemeRegistry
from ThematicRender.utils import TimerStats

# pipeline_engine.py
class PipelineEngine:
    def __init__(
            self, cfg: ConfigMgr, pipeline: list, percent: Any, row: Any, col: Any,
            multitthread: bool
    ) -> None:
        self.pool_map = None
        self.multitthread = multitthread
        self.cfg = cfg
        self.tmr = TimerStats()

        # 1. Initialize Engines
        self.surfaces = SurfaceEngine(cfg)
        self.themes = ThemeRegistry(cfg)
        self.compositor = CompositingEngine(self.tmr)

        # 2. Map Resources
        self.resources = derive_resources(
            cfg=cfg, pipeline=pipeline, factor_specs=FACTOR_SPECS, surface_specs=SURFACE_SPECS
        )

        # 3. Initialize Shared Resources
        self.noise_registry = NoiseRegistry(cfg, self.resources.noise_profiles)
        self.factors = FactorEngine(
            cfg=self.cfg, themes=self.themes, noise_registry=self.noise_registry,
            factor_specs=FACTOR_SPECS, resources=self.resources, timer=self.tmr)
        self.pipeline = pipeline

        # 4. Handle Preview Params
        p = float(percent) if percent is not None else 0.0
        self.percent = p if 0.0 < p < 1.0 else 0.0
        self.row = float(row) if row is not None else 0.5
        self.col = float(col) if col is not None else 0.5

    def process_rasters(self) -> None:
        # Simple stats accumulator
        stats = {
            "read": 0.0, "render": 0.0, "write": 0.0,
            "flush": 0.0, "init" : 0.0, "cleanup": 0.0, "count": 0
        }
        total_start = time.perf_counter()

        with ExitStack() as stack:
            out_path = self.cfg.path("output")

            # 1. Define Pool Specs (256x256 tiles, 3 bands for RGB)
            out_spec = PoolSpec(
                value_shape=(3, 256, 256), # RGB
                value_dtype=np.dtype(np.uint8),
                valid_shape=(1, 256, 256), # Alpha/Mask
                valid_dtype=np.dtype(np.float32)
            )
            # Initialize the pool for the output buffers
            output_pool = SharedMemoryPool(out_spec, slots=16, prefix="tr_output")
            stack.callback(output_pool.cleanup) # Ensure cleanup is registered immediately

            with RasterManager(self.cfg, self.resources.drivers, self.resources.anchor_key) as io:
                # 2. PREVIEW LOGIC FIRST
                # (Calculates the final dimensions and profile BEFORE creating the file)
                write_offset_row = 0
                write_offset_col = 0
                envelope = None
                profile = self._build_output_profile(io)

                if 0.0 < self.percent < 1.0:
                    envelope = self._calculate_preview_window(
                        io.anchor_src, percent=self.percent, rel_x=self.col, rel_y=self.row
                    )
                    if envelope is not None:
                        profile.update({
                            "width": int(envelope.width),
                            "height": int(envelope.height),
                            "transform": io.anchor_src.window_transform(envelope),
                        })
                        write_offset_row = int(envelope.row_off)
                        write_offset_col = int(envelope.col_off)

                # 3. INITIALIZE THE FILE AND CLOSE IT IMMEDIATELY
                # This satisfies GDAL's "format recognized" check for the later r+ open
                print(f"🏗️ Initializing output: {out_path.name}")
                with rasterio.open(out_path, "w", **profile) as init_dst:
                    pass

                # 4. PREPARE THE WINDOWS & POOL MAP
                # We open in "r" briefly to inspect the block structure
                with rasterio.open(out_path, "r") as reader_dst:
                    dst_windows = [w for _, w in reader_dst.block_windows(1)]
                    self.pool_map = self._create_pool_map(io=io, dst=reader_dst)
                    for pool in self.pool_map.values():
                        stack.callback(pool.cleanup)

                # 5. TRANSLATE WINDOWS
                if envelope is not None:
                    win_list = [Window(
                        col_off=int(w.col_off) + write_offset_col,
                        row_off=int(w.row_off) + write_offset_row,
                        width=int(w.width),
                        height=int(w.height)
                    ) for w in dst_windows]
                else:
                    win_list = dst_windows

                # 6. BUILD CONTEXTS
                reader_ctx = ReaderContext(io=io, pool_map=self.pool_map)
                worker_ctx = WorkerContext(
                    cfg=self.cfg,
                    pool_map=self.pool_map,
                    factors_engine=self.factors,
                    surfaces_engine=self.surfaces,
                    themes=self.themes,
                    compositor=self.compositor,
                    pipeline=self.pipeline,
                    anchor_key=self.resources.anchor_key,
                    surface_inputs=self.resources.surface_inputs,
                    resources=self.resources,
                    noise_registry=self.noise_registry,
                    out_pool=output_pool # Connect the output pool here
                )

                writer_ctx = WriterContext(
                    output_path=out_path,
                    output_profile=profile,
                    pool_map=self.pool_map,
                    write_offset_row=write_offset_row,
                    write_offset_col=write_offset_col,
                    out_pool=output_pool # Connect the output pool here
                )
                init_duration = time.perf_counter() - total_start
                stats["init"] = init_duration

                # 7. PROCESSING
                if not self.multitthread:
                    # ---  SINGLE THREADED ---
                    progress_bar = tqdm(win_list, desc="Rendering (ST)")
                    for seq, window in enumerate(progress_bar):
                        work_packet = read_task(seq=seq, window=window, ctx=reader_ctx)
                        stats["read"] += work_packet.read_duration

                        result_packet = render_task(packet=work_packet, ctx=worker_ctx)
                        stats["render"] += result_packet.render_duration

                        stats["write"] += write_task(packet=result_packet, ctx=writer_ctx)
                        stats["count"] += 1

                    f_start = time.perf_counter()
                    writer_ctx.close()
                    stats["write"] += (time.perf_counter() - f_start)
                else:
                    # --- PHASE 3: MULTIPROCESSING ---
                    print(f" Launching Pipeline [Multiprocessor: {os.cpu_count()-1} render cores]")

                    # 1. Get the Spawn Context
                    ctx_mp = mp.get_context('spawn')

                    # 2. Setup Shared Queues
                    work_queue = ctx_mp.Queue(maxsize=4) # Small buffer for backpressure
                    result_queue = ctx_mp.Queue()

                    # 3. Start Dedicated Writer
                    writer_p = ctx_mp.Process(
                        target=writer_worker_loop,
                        args=(result_queue, writer_ctx)
                    )
                    writer_p.start()

                    # 4. Start Render Worker Pool
                    # We save 1 core for the Reader/Main and 1 for the Writer
                    num_workers = 3 #max(1, os.cpu_count() - 2)
                    workers = []
                    for i in range(num_workers):
                        p = ctx_mp.Process(
                            target=render_worker_loop,
                            args=(work_queue, result_queue, worker_ctx)
                        )
                        p.start()
                        workers.append(p)

                    # 5. READER LOOP (Main Process)
                    try:
                        progress_bar = tqdm(win_list, desc="Processing")
                        for seq, window in enumerate(progress_bar):
                            # This will naturally block if SHM slots are full
                            work_packet = read_task(seq=seq, window=window, ctx=reader_ctx)

                            # Push to workers
                            work_queue.put(work_packet)

                    finally:
                        print("🛑 Shutting down workers...")
                        # Send None to every worker to stop them
                        for _ in workers: work_queue.put(None)
                        for p in workers: p.join()

                        # Send None to writer
                        result_queue.put(None)
                        writer_p.join()

        # This is where the deferred write time actually happens
        writer_ctx.close()
        flush_duration = 0 #time.perf_counter() - flush_start
        stats["flush"] = flush_duration
        stats["write"] += flush_duration
        print(f"\nDisk Flush:      {flush_duration:.2f}s")

        total_elapsed = time.perf_counter() - total_start
        self._print_stats_report(stats, total_elapsed)

    @staticmethod
    def _print_stats_report(stats, total_elapsed):
        n = stats["count"] or 1
        sum_parts = stats['init'] + stats['read'] + stats['render'] + stats['write'] + stats['cleanup']
        unknown = total_elapsed - sum_parts

        print("="*40)
        print(f"Init:         {stats['init']:7.2f}s")
        print(f"Read:         {stats['read']:7.2f}s")
        print(f"Render:       {stats['render']:7.2f}s")
        print(f"Write:        {stats['write']:7.2f}s")
        print(f"Cleanup:      {stats['cleanup']:7.2f}s")
        print(f"Unknown:      {unknown:7.2f}s ")
        print("-" * 40)
        print(f"Wall Time:    {total_elapsed:7.2f}s")

        # Efficiency Ratio logic:
        # Only Read/Render/Write can be parallelized in Phase 3.
        # Init and Cleanup stay serial.
        parallel_potential = stats['read'] + stats['render'] + stats['write']
        efficiency = parallel_potential / (total_elapsed - stats['init'] - stats['cleanup'])
        print(f"Efficiency:   {efficiency:7.2f}x")
        print("="*40 + "\n")

    def _create_pool_map(self, io, dst) -> Dict[DriverKey, SharedMemoryPool]:
        block_h, block_w = dst.block_shapes[0]

        # Determine universal slot size (Block + Max Halo)
        max_halo = max([self.cfg.get_spec(k).halo_px for k in io.sources.keys()])
        pool_h, pool_w = block_h + 2 * max_halo, block_w + 2 * max_halo

        slots = 16 # Adjust based on RAM
        pool_map = {}

        for dkey in io.sources.keys():
            dspec = self.cfg.get_spec(dkey)
            val_dtype = np.uint8 if dspec.dtype == np.uint8 else np.float32

            spec = PoolSpec(
                value_shape=(pool_h, pool_w), value_dtype=np.dtype(val_dtype),
                valid_shape=(pool_h, pool_w, 1), valid_dtype=np.dtype(np.float32)
            )
            # Use a unique prefix per driver to avoid name collisions in the OS
            pool_map[dkey] = SharedMemoryPool(spec, slots, prefix=f"tr_{dkey.value}")

        return pool_map

    @staticmethod
    def _calculate_preview_window(src, percent: float, rel_x: float, rel_y: float) -> Window:
        """Calculates a global Window based on normalized focal points (0.0-1.0)."""
        full_w, full_h = src.width, src.height
        target_w, target_h = int(full_w * percent), int(full_h * percent)

        # Calculate top-left corner from focal point
        col_off = int(full_w * rel_x) - (target_w // 2)
        row_off = int(full_h * rel_y) - (target_h // 2)

        # Snap to 256 for processing efficiency
        col_off = (max(0, col_off) // 256) * 256
        row_off = (max(0, row_off) // 256) * 256

        # Clamp to bounds
        col_off = max(0, min(col_off, full_w - target_w))
        row_off = max(0, min(row_off, full_h - target_h))

        return Window(
            col_off, row_off, min(target_w, full_w - col_off), min(target_h, full_h - row_off)
            )

    @staticmethod
    def _build_output_profile(io: RasterManager) -> dict:
        anchor = io.anchor_src
        return {
            "driver": "GTiff", "height": anchor.height, "width": anchor.width, "count": 3,
            "dtype": "uint8", "crs": anchor.crs, "transform": anchor.transform, "tiled": True,
            "blockxsize": 256, "blockysize": 256, "compress": "deflate", "predictor": 2,
            "nodata": None,
        }

    def load_ramps(self) -> None:
        """Load ramps and update config paths."""
        ramp_files = self.surfaces.load_surface_ramps(self.resources)
        # Note: In Phase 3, this will be handled before ConfigMgr is locked
        # For now, we update the existing files dict
        self.cfg.files.update({k: Path(v) for k, v in ramp_files.items()})

def on_worker_done(future):
    try:
        future.result()  # This will re-raise any exception that happened in the worker
    except Exception as e:
        print(f"🔥 WORKER CRASHED: {e}")
        traceback.print_exc()
