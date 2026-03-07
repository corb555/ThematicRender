import time
import traceback

import numpy as np
import rasterio
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, Tuple,  Set

from rasterio.windows import Window
from scipy.ndimage import gaussian_filter
from pathlib import Path

from ThematicRender.config_mgr import ConfigMgr
from ThematicRender.ipc_blocks import (WorkPacket, ResultPacket, rect_from_window, window_from_rect,
                                       DriverBlockRef)
from ThematicRender.keys import DriverKey

# -----------------------------------------------------------------------------
# Context objects (The "Manifests")
# -----------------------------------------------------------------------------

#pipeline_tasks.py
@dataclass(slots=True)
class ReaderContext:
    io: Any # RasterManager
    pool_map: Dict[DriverKey, Any]

@dataclass(slots=True)
class WorkerContext:
    cfg: ConfigMgr
    pool_map: Dict[DriverKey, Any]
    factors_engine: Any
    surfaces_engine: Any
    themes: Any
    compositor: Any
    pipeline: Any
    anchor_key: DriverKey
    surface_inputs: Set[Any]
    resources: Any
    noise_registry: Any
    out_pool: Optional[Any] = None
    _initialized: bool = False

    @property
    def initialized(self):
        return self._initialized

@dataclass(slots=True)
class WriterContext:
    output_path: Path
    output_profile: Dict[str, Any]
    pool_map: Dict[DriverKey, Any]
    out_pool: Optional[Any] = None
    write_offset_row: int = 0
    write_offset_col: int = 0

    # Internal local handle (NOT PICKLED)
    _dst: Any = field(default=None, init=False, repr=False)

    def get_dst(self):
        """Lazy-load the file handle locally in the writer process."""
        if self._dst is None:
            # We open in r+ because the file was created empty by the main engine
            self._dst = rasterio.open(self.output_path, "r+")
        return self._dst

    def close(self):
        """Cleanup handle."""
        if self._dst:
            self._dst.close()
            self._dst = None

def init_render_task(ctx: WorkerContext) -> None:
    """Performs one-time initialization of engines inside a worker process."""
    if ctx._initialized:
        return

    # Load QML -> LUT
    ctx.themes.load_theme_style(context="Render Process Init")

    # Interpolate Color Ramps
    ctx.surfaces_engine.load_surface_ramps(ctx.resources)

    ctx._initialized = True

# -----------------------------------------------------------------------------
# Tasks (The Functional Compute Units)
# -----------------------------------------------------------------------------
def read_task(*, seq: int, window: Window, ctx: ReaderContext) -> WorkPacket:
    """Reads all required drivers from disk into SHM. Runs on Main Process."""
    t_start = time.perf_counter()

    # 1. Calculate max halo for coordinate expansion
    max_halo = 0
    for dkey in ctx.io.sources:
        spec = ctx.io.cfg.get_spec(dkey)
        max_halo = max(max_halo, spec.halo_px)

    # 2. Iterate through all required drivers and read into their respective pools
    refs = {}
    for key, src in ctx.io.sources.items():
        # ctx.io is the RasterManager
        # ctx.pool_map[key] is the SharedMemoryPool for this specific driver
        refs[key] = ctx.io.read_driver_block_ref(
            key=key,
            src=src,
            window=window,
            halo_override=max_halo,
            pool=ctx.pool_map[key]
        )

    # 3. Return the WorkPacket with metadata for the Render task
    return WorkPacket(
        seq=seq,
        window_rect=rect_from_window(window),
        refs=refs,
        read_duration=time.perf_counter() - t_start
    )

def render_task(*, packet: WorkPacket, ctx: WorkerContext) -> ResultPacket:
    """The 2D Compute Firewall. Runs on Worker Process."""
    t_start = time.perf_counter()
    if not ctx._initialized:
        init_render_task(ctx)

    # 1. GEOMETRY TRUTH
    inner_window = window_from_rect(packet.window_rect)
    h, w = int(inner_window.height), int(inner_window.width)

    # 2. HYDRATION & FIREWALL (Pool returns B, H, W)
    raw_blocks = {k: ctx.pool_map[k].view(ref) for k, ref in packet.refs.items()}

    # Extract Anchor Metadata (needed for padding/halos)
    anchor_blk_ref = packet.refs[ctx.anchor_key]
    anchor_blk_view = raw_blocks[ctx.anchor_key]

    # Squeeze to 2D for Engines (Always take Band 0)
    val_2d = {k: np.squeeze(blk.value[0]) for k, blk in raw_blocks.items()}
    vld_2d = {k: np.squeeze(blk.valid[0]) for k, blk in raw_blocks.items()}

    # 3. COMPUTE WINDOW (Halo Space calculation)
    # We need the padding info from the anchor's inner_slices to define the compute window
    r_pad = anchor_blk_ref.inner_slices[0].start if anchor_blk_ref.inner_slices else 0
    c_pad = anchor_blk_ref.inner_slices[1].start if anchor_blk_ref.inner_slices else 0

    compute_h, compute_w = val_2d[ctx.anchor_key].shape[:2]
    compute_window = rasterio.windows.Window(
        col_off=inner_window.col_off - c_pad,
        row_off=inner_window.row_off - r_pad,
        width=compute_w, height=compute_h
    )

    # 4. GEOMETRY CLEANUP (Config Driven)
    for dkey in val_2d.keys():
        dspec = ctx.cfg.get_spec(dkey)
        if not dspec.cleanup_type: continue

        if dspec.cleanup_type == "categorical":
            val_2d[dkey] = ctx.themes.get_smoothed_ids(val_2d[dkey])
        elif dspec.cleanup_type == "continuous":
            radius = dspec.smoothing_radius or 4.0
            data = val_2d[dkey].astype(np.float32)
            # Apply 2D filter
            val_2d[dkey] = gaussian_filter(data, sigma=radius)

    # 5. GENERATE FACTORS (2D Zone)
    raw_factors = ctx.factors_engine.generate_factors(val_2d, vld_2d, compute_window, ctx.anchor_key)
    factors_2d = {k: np.squeeze(f) for k, f in raw_factors.items()}

    # 6. GENERATE SURFACES (2D Zone)
    surface_blocks = ctx.surfaces_engine.generate_surface_blocks(
        val_2d=val_2d, vld_2d=vld_2d, factors_2d=factors_2d, style_engine=ctx.themes,
        manifest=ctx.surface_inputs, noises=ctx.factors_engine.noise_registry,
        window=compute_window, anchor_key=ctx.anchor_key
    )

    # 7. CROP & BLEND (Back to inner window dimensions)
    slices = anchor_blk_ref.inner_slices or (slice(None), slice(None))
    surfaces_in = _slice_collection(surface_blocks, slices)
    factors_in = _slice_collection(factors_2d, slices)

    img_block = ctx.compositor.blend_window(surfaces_in, factors_in, ctx.pipeline)

    # 8. PACKAGING
    if ctx.out_pool is None:
        # Single-thread pass-through
        return ResultPacket(
            seq=packet.seq,
            window_rect=packet.window_rect,
            refs=packet.refs,
            img_block=img_block,
            read_duration=packet.read_duration, # Pass along the read metric
            render_duration=time.perf_counter() - t_start
        )
    else:
        # Phase 2b/3 SHM path
        out_slot = ctx.out_pool.acquire()
        try:
            # write() handles the conversion of (3, H, W) into (Slot, Band, H, W)
            out_ref = ctx.out_pool.write(
                out_slot,
                value=img_block,
                valid=np.ones((1, h, w), dtype=np.float32),
                inner_slices=None
            )
            return ResultPacket(
                seq=packet.seq,
                window_rect=packet.window_rect,
                refs=packet.refs,
                out_ref=out_ref,
                read_duration=packet.read_duration, # Pass along the read metric
                render_duration=time.perf_counter() - t_start
            )
        except Exception as e:
            ctx.out_pool.release(out_slot)
            raise e

def write_task(*, packet: ResultPacket, ctx: WriterContext) -> float:
    """Writes to disk and releases all SHM slots."""

    # 1. COORDINATE TRANSLATION
    # window = Global GIS coordinates
    # local_window = Relative coordinates inside the output file
    t_start = time.perf_counter()

    window = window_from_rect(packet.window_rect)
    local_window = rasterio.windows.Window(
        col_off=int(window.col_off) - int(ctx.write_offset_col),
        row_off=int(window.row_off) - int(ctx.write_offset_row),
        width=int(window.width),
        height=int(window.height)
    )

    # 2. RESOLVE DATA
    if packet.img_block is not None:
        # Single-thread pass-through path
        data = packet.img_block
    elif packet.out_ref is not None:
        # Pull from SHM Output Pool (Standardized 4D -> 3D View)
        view = ctx.out_pool.view(packet.out_ref)

        # IMPORTANT: Crop to the actual valid data height/width.
        # This handles the "Edge Case" where the final tile in a row
        # is smaller than the standard 256x256 buffer.
        h, w = packet.out_ref.value_hw
        data = view.value[:, :h, :w]
    else:
        raise ValueError("ResultPacket is empty: contains neither img_block nor out_ref.")

    # 3. WRITE (Rasterio expects B, H, W)
    dst = ctx.get_dst()
    dst.write(data, window=local_window)

    # 4. RESOURCE RELEASE
    # This returns the slot indices to the "Available" Queue
    if packet.out_ref:
        ctx.out_pool.release(packet.out_ref.slot_id)

    # Release all input buffers (DEM, Lithology, etc.) used for this  tile
    for dkey, ref in packet.refs.items():
        ctx.pool_map[dkey].release(ref.slot_id)

    return time.perf_counter() - t_start

def render_worker_task(packet, worker_ctx, result_queue):
    """Note: Multiprocessing.Queue doesn't like being in the middle of arguments
    sometimes, but this should work."""
    try:
        # Re-initialize the worker's SHM handles if needed
        # (Usually handled by your ctx._initialized check)
        result = render_task(packet=packet, ctx=worker_ctx)
        result_queue.put(result)
    except Exception as e:
        print(f"Render Error: {e}")
        traceback.print_exc()

def render_worker_loop(work_queue, result_queue, worker_ctx):
    # 1. THE HANDSHAKE
    # Check the Output Pool signature
    if worker_ctx.out_pool:
        if not worker_ctx.out_pool.verify_connection():
            return # Exit if the session is invalid

    # Check all input pools
    for pool in worker_ctx.pool_map.values():
        # Force re-attachment of the numpy views for the new process
        pool._v_cache = None
        pool._m_cache = None
        if not pool.verify_connection():
            return

    # 2. INITIALIZE ENGINE (Once per core)
    if not worker_ctx._initialized:
        init_render_task(worker_ctx)

    while True:
        packet = work_queue.get()
        if packet is None: # Sentinel to exit
            break

        try:
            # Use your existing render_task logic
            result = render_task(packet=packet, ctx=worker_ctx)
            result_queue.put(result)
        except Exception as e:
            print(f"🔥 Worker Render Error: {e}")
            traceback.print_exc()


def writer_worker_loop(result_queue, writer_ctx):
    """The dedicated I/O process aggregates all telemetry."""
    stats = {
        "read": 0.0,
        "render": 0.0,
        "write": 0.0,
        "count": 0,
    }

    # Track when the writer actually started its loop
    proc_start = time.perf_counter()

    try:
        while True:
            packet = result_queue.get()
            if packet is None:
                break

            # 1. Measure the Write
            write_start = time.perf_counter()
            write_task(packet=packet, ctx=writer_ctx)
            write_duration = time.perf_counter() - write_start

            # 2. Accumulate from Packet
            stats["read"] += packet.read_duration
            stats["render"] += packet.render_duration
            stats["write"] += write_duration
            stats["count"] += 1

    finally:
        # 3. Final Flush
        f_start = time.perf_counter()
        writer_ctx.close()
        flush_duration = time.perf_counter() - f_start
        stats["write"] += flush_duration

        # 4. Total Wall Time (from the writer's perspective)
        # Note: Adding init_duration makes it comparable to the total run
        total_elapsed = (time.perf_counter() - proc_start)

        # 5. Print the Final Report
        _print_mp_report(stats, total_elapsed, flush_duration)


def _print_mp_report(stats, total_elapsed, flush_duration):
    n = stats["count"] or 1
    print("\n" + "="*40)
    print(f"🏁 MP RENDER REPORT ({n} tiles)")
    print("-" * 40)
    print(f"Read :          {stats['read']:7.2f}s")
    print(f"Render :        {stats['render']:7.2f}s")
    print(f"Write :         {stats['write']:7.2f}s (inc. {flush_duration:.2f}s flush)")
    print("-" * 40)
    print(f"Wall Time:      {total_elapsed:7.2f}s")

    # The Parallelism Power:
    # Sum of all work / Wall time
    sum_work = stats['read'] + stats['render'] + stats['write']
    print(f"Efficiency:   {sum_work / total_elapsed:7.2f}x speedup")
    print("="*40 + "\n")

def _slice_collection(collection: Dict[Any, np.ndarray], slices: Tuple[slice, slice]):
    sy, sx = slices
    return {k: v[sy, sx, ...] for k, v in collection.items()}