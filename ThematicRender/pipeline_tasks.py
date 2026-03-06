from __future__ import annotations

import dataclasses
from dataclasses import dataclass
import multiprocessing as mp
from pathlib import Path
import time
from typing import Tuple, Any, Dict, Optional

import numpy as np
import rasterio
from rasterio.windows import Window
from scipy.ndimage import gaussian_filter

from ThematicRender.config_mgr import ConfigMgr
from ThematicRender.ipc_blocks import (WorkPacket, ResultPacket, rect_from_window, window_from_rect)
from ThematicRender.keys import DriverKey
from ThematicRender.raster_manager import RasterManager
from ThematicRender.utils import stats_once


# pipeline_tasks.py
# -----------------------------------------------------------------------------
# Context objects
# -----------------------------------------------------------------------------

@dataclass(slots=True)
class ReaderContext:
    io: RasterManager
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
    surface_inputs: dict
    resources: Any
    noise_registry: Any
    _initialized: bool = False
    out_pool: Optional[Any] = None

    @property
    def initialized(self):
        return self._initialized


@dataclass(slots=True)
class WriterContext:
    dst: Any  # rasterio dataset writer
    pool_map: Dict[DriverKey, Any]
    write_offset_row: int = 0
    write_offset_col: int = 0
    out_pool: Optional[Any] = None  # Phase-2 output pool (optional)


def init_render_task(ctx: WorkerContext) -> None:
    """
    Performs one-time initialization of engines (Loading QML, building LUTs,
    interpolating color ramps).
    """
    if ctx._initialized:
        return

    if ctx.factors_engine:
        # Check if your FactorEngine uses .noises or .noise_registry
        ctx.noise_registry = getattr(
            ctx.factors_engine, 'noise_registry', getattr(ctx.factors_engine, 'noises', None)
            )
    # 1. Load thematic styles (QML -> LUT)
    ctx.themes.load_theme_style(context="Render Process Init")

    # 2. Load and Interpolate Color Ramps
    # This builds the scipy interp1d functions inside ctx.surfaces_engine
    ctx.surfaces_engine.load_surface_ramps(ctx.resources)

    ctx._initialized = True


# -----------------------------------------------------------------------------
# Tasks (module-level, pure functions; easy to call inline OR from worker loops)
# -----------------------------------------------------------------------------

def read_task(*, seq: int, window: Window, ctx: ReaderContext) -> WorkPacket:
    """Read drivers into pool slots and return an IPC-friendly WorkPacket."""
    max_halo = 0
    for dkey in ctx.io.sources:
        spec = ctx.io.cfg.driver_spec(dkey)
        max_halo = max(max_halo, spec.halo_px)

    refs = {}
    for key, src in ctx.io.sources.items():
        refs[key] = ctx.io.read_driver_block_ref(
            key, src, window, halo_override=max_halo, pool=ctx.pool_map[key]
            )

    return WorkPacket(seq=seq, window_rect=rect_from_window(window), refs=refs)


def render_task(*, packet: WorkPacket, ctx: WorkerContext) -> ResultPacket:
    if not ctx._initialized:
        init_render_task(ctx)

    # --- 1. GEOMETRY TRUTH ---
    inner_window = window_from_rect(packet.window_rect)
    inner_h, inner_w = int(inner_window.height), int(inner_window.width)

    # --- 2. HYDRATION & FIREWALL ---
    raw_blocks = {k: ctx.pool_map[k].view(ref) for k, ref in packet.refs.items()}

    val_2d = {}
    vld_2d = {}
    for k, blk in raw_blocks.items():
        val_2d[k] = np.squeeze(blk.value)
        vld_2d[k] = np.squeeze(blk.valid)

    # --- 3. CALCULATE COMPUTE WINDOW (Halo Space) ---
    # The engines need to know the global coordinates of the 384x384 buffer,
    # not just the 256x256 inner window.
    anchor_blk = raw_blocks[ctx.anchor_key]
    # How much was this block padded?
    r_pad = anchor_blk.inner_slices[0].start if anchor_blk.inner_slices else 0
    c_pad = anchor_blk.inner_slices[1].start if anchor_blk.inner_slices else 0

    # This is the 384x384 global window
    compute_h, compute_w = val_2d[ctx.anchor_key].shape[:2]
    compute_window = Window(
        col_off=inner_window.col_off - c_pad,
        row_off=inner_window.row_off - r_pad,
        width=compute_w,
        height=compute_h
    )

    # --- 4. DRIVER SMOOTHING ---
    for dkey in val_2d.keys():
        dspec = ctx.cfg.driver_spec(dkey)
        if not dspec.cleanup_type: continue

        if dspec.cleanup_type == "categorical":
            val_2d[dkey] = ctx.themes.get_smoothed_ids(val_2d[dkey])
        if dspec.cleanup_type == "continuous":
            radius = dspec.smoothing_radius or 4.0
            data = val_2d[dkey].astype(np.float32)

            # Only blur spatial dimensions (H, W).
            # Do NOT blur across the band axis (Axis 2).
            if data.ndim == 3:
                # sigma=(radius, radius, 0) means blur Y, blur X, but skip Band
                val_2d[dkey] = gaussian_filter(data, sigma=(radius, radius, 0))
            else:
                val_2d[dkey] = gaussian_filter(data, sigma=radius)

    # --- 5. ENGINE EXECUTION (Passing COMPUTE_WINDOW) ---
    # We pass the 384x384 window so noise sampling matches buffer size
    raw_factors = ctx.factors_engine.generate_factors(val_2d, vld_2d, compute_window, ctx.anchor_key)
    factors_2d = {k: np.squeeze(f) for k, f in raw_factors.items()}

    surface_blocks = ctx.surfaces_engine.generate_surface_blocks(
        val_2d=val_2d,
        vld_2d=vld_2d,
        factors_2d=factors_2d,
        style_engine=ctx.themes,
        manifest=ctx.surface_inputs,
        noises=ctx.factors_engine.noise_registry,
        window=compute_window, # <--- 384x384
        anchor_key=ctx.anchor_key
    )

    # --- 6. THE CROP CONTRACT ---
    # Everything was calculated at 384x384. Now we crop to the 256x256 result.
    slices = anchor_blk.inner_slices or (slice(None), slice(None))
    surfaces_in = _slice_collection(surface_blocks, slices)
    factors_in = _slice_collection(factors_2d, slices)

    # --- 7. COMPOSITION & PACKAGING ---
    img_block = ctx.compositor.blend_window(surfaces_in, factors_in, ctx.pipeline)
    #stats_once("img_block after blend", img_block)

    # --- 7. RESULT PACKAGING ---
    if ctx.out_pool is None:
        return ResultPacket(
            seq=packet.seq, window_rect=packet.window_rect, refs=packet.refs, img_block=img_block
        )
    else:
        out_slot = ctx.out_pool.acquire()
        try:
            # Enforce 3D validity for the storage pool only at the point of writing
            valid_mask = np.ones((inner_h, inner_w, 1), dtype=np.float32)

            out_ref = ctx.out_pool.write(
                out_slot,
                value=img_block,
                valid=valid_mask,
                inner_slices=None,
                pad_value=0,
                pad_valid=0.0
            )
        except Exception as e:
            ctx.out_pool.release(out_slot)
            print(f"❌ IPC Write Error at Seq {packet.seq}: {e}")
            raise

        return ResultPacket(
            seq=packet.seq, window_rect=packet.window_rect, refs=packet.refs, out_ref=out_ref
        )

def write_task(*, packet: ResultPacket, ctx: WriterContext) -> None:
    """Write tile and release input slots. Global->Local translation happens HERE."""
    window = window_from_rect(packet.window_rect)

    local_window = Window(
        col_off=int(window.col_off) - int(ctx.write_offset_col),
        row_off=int(window.row_off) - int(ctx.write_offset_row), width=int(window.width),
        height=int(window.height), )

    #  write from packet.img_block
    if packet.img_block is not None:
        ctx.dst.write(packet.img_block, window=local_window)

    #  write from output pool (optional, future-ready)
    elif packet.out_ref is not None:
        if ctx.out_pool is None:
            raise RuntimeError("ResultPacket has out_ref but WriterContext.out_pool is None.")
        out_view = ctx.out_pool.view(packet.out_ref)
        # out_view.value should be (H,W,3) or (3,H,W) depending on your design
        ctx.dst.write(out_view.value, window=local_window)  # adjust if band order differs
        ctx.out_pool.release(packet.out_ref.slot_id)

    else:
        raise RuntimeError("ResultPacket has neither img_block nor out_ref.")

    # Release input slots
    for dkey, ref in packet.refs.items():
        ctx.pool_map[dkey].release(ref.slot_id)


def mp_reader(win_list: list, work_queue: mp.Queue, ctx: ReaderContext):
    """ mp process for reading drivers"""
    with RasterManager(ctx.io.cfg, ctx.io.required_drivers, ctx.io.anchor_key) as io:
        ctx.io = io  # Attach the open handles to the context
        for seq, window in enumerate(win_list):
            # Wait for pool availability
            packet = None
            while packet is None:
                try:
                    packet = read_task(seq=seq, window=window, ctx=ctx)
                except RuntimeError:  # Pool exhausted
                    time.sleep(0.1)
            work_queue.put(packet)
    work_queue.put(None)  # Sentinel: End of stream


def mp_render(work_queue: mp.Queue, result_queue: mp.Queue, ctx: WorkerContext):
    """mp process for rendering."""
    while True:
        packet = work_queue.get()
        if packet is None:
            result_queue.put(None)
            break
        try:
            result = render_task(packet=packet, ctx=ctx)
            result_queue.put(result)
        except Exception as e:
            print(f"Worker Error: {e}")


def mp_writer(
        result_queue: mp.Queue, total_count: int, ctx: WriterContext, profile: dict, out_path: Path
):
    """mp process for writing output."""
    with rasterio.open(out_path, "w", **profile) as dst:
        ctx.dst = dst
        completed = 0
        reorder_buffer = {}  # Handle out-of-order results
        next_seq = 0

        while completed < total_count:
            packet = result_queue.get()
            if packet is None: continue

            reorder_buffer[packet.seq] = packet

            # Write in sequence
            while next_seq in reorder_buffer:
                p = reorder_buffer.pop(next_seq)
                write_task(packet=p, ctx=ctx)
                next_seq += 1
                completed += 1


# -----------------------------------------------------------------------------
# Module-Level Helpers
# -----------------------------------------------------------------------------

from rasterio.windows import Window, union


def preview_list(dst, all_windows, percent, row=0, col=0):
    """Returns (filtered_windows, envelope_window)"""
    block_h = dst.height * (percent / 100.0)
    block_w = dst.width * (percent / 100.0)

    min_row, max_row = row * block_h, (row + 1) * block_h
    min_col, max_col = col * block_w, (col + 1) * block_w

    win_list = [w for w in all_windows if
                min_row <= w.row_off < max_row and min_col <= w.col_off < max_col]

    if not win_list:
        return [], None

    # Calculate the bounding box of all selected windows
    # This ensures we capture the exact pixel dimensions of the subset
    envelope = win_list[0]
    for w in win_list[1:]:
        envelope = union(envelope, w)

    return win_list, envelope


def _slice_collection(collection: Dict[Any, np.ndarray], slices: Tuple[slice, slice]):
    """Slices only the spatial dimensions of every array in a dictionary."""
    sy, sx = slices
    return {k: v[sy, sx, ...] for k, v in collection.items()}
