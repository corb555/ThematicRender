# FORCE SINGLE THREADING INSIDE THE CHILD PROCESS

import multiprocessing
import sys
import traceback
from typing import Dict, Any, Tuple, Optional

import numpy as np
from python_utils.types import ExceptionsType
import rasterio
from rasterio.windows import Window
from scipy.ndimage import gaussian_filter
import setproctitle

from ThematicRender.compositing_engine import CompositingEngine
from ThematicRender.factor_engine import FactorEngine
# render_task.py
from ThematicRender.ipc_packets import RenderPacket, WriterPacket, Op, Envelope, ErrorPacket, \
    send_error, SEV_CANCEL, SEV_FATAL
from ThematicRender.job_context import JobContextStore
from ThematicRender.noise_library import NoiseLibrary
from ThematicRender.surface_engine import SurfaceEngine
from ThematicRender.utils import window_from_rect
from ThematicRender.worker_context_base import sync_ctx_for_packet
from ThematicRender.worker_contexts import WorkerContext


def load_worker_job_ctx(job_id: str, shm_store: JobContextStore) -> WorkerContext:
    """Load the worker context for a specific job from shared job storage."""
    try:
        return shm_store.get_worker_context(job_id)
    except Exception as exc:
        raise RuntimeError(
            f"[RENDER] Failed to load WorkerContext for job '{job_id}': {exc}"
        ) from exc


def render_loop(work_q, writer_q, status_q, shm_name, out_pool, pool_map):
    section = "RENDER_LOOP"
    setproctitle.setproctitle(multiprocessing.current_process().name)

    shm_store = JobContextStore(name=shm_name)
    ctx: Optional[WorkerContext] = None
    workspace = RenderWorkspace()

    while True:
        envelope = work_q.get()
        packet = envelope.payload
        section = "sync ctx"

        #  STATE TRANSLATION: Sync SHM and check for Cancel/Idle/Stale
        ctx = sync_ctx_for_packet(
            ctx=ctx, packet_job_id=packet.job_id, shm_store=shm_store,
            load_ctx=load_worker_job_ctx, err_prefix="WORKER"
        )
        if ctx is None: continue

        # ENGINE TRANSLATION: Rebuild math engines if config changed
        workspace.sync_to_context(ctx)

        try:
            match envelope.op:
                case Op.RENDER_TILE:
                    try:
                        section = "rnder task"
                        result = render_task(
                            packet=packet, ctx=ctx, workspace=workspace, out_pool=out_pool,
                            pool_map=pool_map
                        )
                        writer_q.put(Envelope(op=Op.WRITE_TILE, payload=result))
                    except (ValueError, OSError, KeyError) as e:
                        # SEV_CANCEL: Notify Orch, but STAY in the while loop
                        payload = ErrorPacket(job_id= packet.job_id,tile_id= -1,section="wrn",severity= SEV_CANCEL,message= f"Render {section} err='{e}'")
                        send_error(status_q, payload)
                    except Exception as e:
                        # SEV_FATAL: Notify Orch and EXIT the process
                        stack_trace_str = traceback.format_exc()
                        payload = ErrorPacket(job_id= packet.job_id,tile_id= -1,section="excep",severity= SEV_FATAL,message= f"Render {section} Error {e} {stack_trace_str}")
                        send_error(status_q, payload)
                        break

                case Op.SHUTDOWN:
                    if ctx: ctx.close_local_resources()
                    break

                case Op.JOB_CANCEL:
                    # Passive workers just return to get()
                    continue

                # UNKNOWN MESSAGES
                case _:
                    payload = ErrorPacket(
                        job_id=packet.job_id,tile_id= -1, section=section, severity=SEV_FATAL,message= f"{section} Unknown message rcvd")
                    send_error(status_q, payload)
        except ValueError as e:
            print(f"{section} RENDER1 ERROR {e}")
            payload = ErrorPacket(job_id=packet.job_id, tile_id= -1, section=section, severity=SEV_CANCEL,message=f"Warning: {e}" )
            send_error(status_q, payload)
        except MemoryError as e:
            print(f"{section} RENDER2 Exception {e}")
            payload = ErrorPacket(
                job_id=packet.job_id, tile_id= -1, section=section, severity=SEV_FATAL,message=f"Fatal: {e}")
            send_error(status_q, payload)
            sys.exit(1)


def render_task(*, packet, ctx, workspace, out_pool, pool_map):
    section = "RENDER_TASK"

    # EXTRACT DATA from SHM and set up the spatial compute window
    data_2d, masks_2d, compute_window, h, w = _prepare_compute_context(packet, ctx, pool_map)

    # CLEAN raster driver data through smoothing and categorical generalization
    for drv_key in data_2d.keys():
        drv_spec = ctx.render_cfg.get_spec(drv_key)
        # print(f"Clean driver {drv_key}")
        if not drv_spec.cleanup_type:
            continue

        if drv_spec.cleanup_type == "categorical":
            data_2d[drv_key] = ctx.themes.get_smoothed_ids(data_2d[drv_key])
        elif drv_spec.cleanup_type == "continuous":
            radius = drv_spec.smoothing_radius
            if radius and radius > 0:
                data_2d[drv_key] = gaussian_filter(
                    data_2d[drv_key].astype(np.float32), sigma=radius
                )
    # GENERATE FACTORS (masks representing biomes, density, or gradients)
    raw_factors = workspace.factor_eng.generate_factors(
        data_2d, masks_2d, compute_window, ctx.anchor_key
    )
    factors_2d = {k: np.squeeze(f) for k, f in raw_factors.items()}

    # SYNTHESIZE SURFACES and apply procedural variation (mottling)
    surface_blocks = workspace.surface_eng.generate_surface_blocks(
        data_2d=data_2d, masks_2d=masks_2d, factors_2d=factors_2d, style_engine=ctx.themes,
        surface_inputs=ctx.surface_inputs, noises=workspace.factor_eng.noise_registry,
        window=compute_window, anchor_key=ctx.anchor_key
    )

    # CROP RESULTS to target size
    anchor_ref = packet.block_map[ctx.anchor_key]
    slices = anchor_ref.inner_slices or (slice(None), slice(None))
    surfaces_in = _slice_collection(surface_blocks, slices)
    factors_in = _slice_collection(factors_2d, slices)

    # BLEND the stack
    img_block = workspace.compositor.blend_window(surfaces_in, factors_in, ctx.pipeline)
    img_block = img_block[:, :h, :w]

    # RETURN RESULT
    out_slot = out_pool.acquire()
    try:
        out_ref = out_pool.write(
            out_slot, data=img_block, mask=np.ones((1, h, w), dtype=np.float32), inner_slices=None
        )

        return WriterPacket(
            job_id=packet.job_id, tile_id=packet.tile_id, window_rect=packet.window_rect,
            refs=packet.block_map, out_ref=out_ref, read_duration=packet.read_duration,
            render_duration=0, img_block=img_block
        )
    except Exception as e:
        # If we fail HERE, the Writer will never see this slot.
        # We must release it ourselves before re-raising the error.
        print(f"render out_pool err {e}")
        out_pool.release(out_slot)
        raise


class RenderWorkspace:
    def __init__(self):
        self.factor_eng = None
        self.surface_eng = None
        self.compositor = CompositingEngine()
        self.current_geography_hash = None
        self.current_logic_hash = None
        self.current_style_hash = None

    def sync_to_context(self, ctx: WorkerContext):
        res = ctx.resources
        needs_style_sync = False

        # 1. LOGIC/GEOGRAPHY
        if res.logic_hash != self.current_logic_hash or self.current_logic_hash is None:
            noise_lib = NoiseLibrary(ctx.render_cfg, ctx.render_cfg.noises)
            noise_lib.attach_providers_shm()

            self.factor_eng = FactorEngine(
                ctx.render_cfg, ctx.themes, noise_lib,
                ctx.render_cfg.factors, res, None
            )
            self.current_logic_hash = res.logic_hash
            needs_style_sync = True # Force theme sync for new engine

        # 2. STYLE
        if res.style_hash != self.current_style_hash or self.current_style_hash is None:
            self.surface_eng = SurfaceEngine(ctx.render_cfg)
            self.current_style_hash = res.style_hash
            needs_style_sync = True

        # 3. CONSOLIDATED SYNC
        if needs_style_sync:
            self.setup_style_state(ctx)

    def _rebuild_logic_stack(self, ctx):
        """Creates fresh engines for a new logic state."""
        noise_lib = NoiseLibrary(ctx.render_cfg, ctx.render_cfg.noises)
        noise_lib.attach_providers_shm()

        self.factor_eng = FactorEngine(
            ctx.render_cfg, ctx.themes, noise_lib,
            ctx.render_cfg.factors, ctx.resources, None
        )

    def _rebuild_style_stack(self, ctx):
        """Methodically pushes new settings into the existing engines."""
        # A. Hydrate the Theme Registry instance from the context
        # This executes the 'DEBUG build runt' logic
        ctx.themes.load_metadata(ctx.render_cfg)
        ctx.themes.load_theme_style()

        # B. THE KEY FIX: PUSH the new context into the Factor Engine
        # This kills the 'Ghost' references.
        if self.factor_eng:
            self.factor_eng.update_render_context(ctx.render_cfg, ctx.themes)

        # C. Rebuild Surface Engine (always fresh for style changes)
        self.surface_eng = SurfaceEngine(ctx.render_cfg)
        self.surface_eng.load_surface_ramps(ctx.resources)

    def setup_style_state(self, ctx: WorkerContext):
        """
        Synchronizes the persistent engines with the current job's context.
        """
        # 1. HYDRATE the specific Registry instance provided by the current job.
        ctx.themes.load_metadata(ctx.render_cfg)
        ctx.themes.load_theme_style()

        # 2. PUSH the new worldview into the Factor Engine.
        if self.factor_eng:
            # Re-link the engine to the fresh unpickled registry and config.
            self.factor_eng.update_render_context(ctx.render_cfg, ctx.themes)

        # 3. SYNC the Surface Engine.
        # Ensure the ramp synthesis logic is using the current job's resource paths.
        if self.surface_eng:
            self.surface_eng.load_surface_ramps(ctx.resources)

def _prepare_compute_context(packet: RenderPacket, ctx: WorkerContext, pool_map):
    """
    Rehydrates shared memory and calculates the expanded spatial context.

    Returns:
        tuple: (data_2d, masks_2d, compute_window, target_h, target_w)
    """
    # 1. Determine the target output dimensions
    inner_window = window_from_rect(packet.window_rect)
    h, w = int(inner_window.height), int(inner_window.width)

    # 2. Map shared memory buffers into local process views (3D)
    raw_blocks = {k: pool_map[k].view(ref) for k, ref in packet.block_map.items()}

    # 3. FIREWALL: Squeeze to strictly 2D working planes
    data_2d = {k: np.squeeze(blk.data[0]) for k, blk in raw_blocks.items()}
    masks_2d = {k: np.squeeze(blk.mask[0]) for k, blk in raw_blocks.items()}

    # 4. Coordinate Calculation (Halo / Padding logic)
    anchor_blk_ref = packet.block_map[ctx.anchor_key]
    r_pad = anchor_blk_ref.inner_slices[0].start if anchor_blk_ref.inner_slices else 0
    c_pad = anchor_blk_ref.inner_slices[1].start if anchor_blk_ref.inner_slices else 0

    # Define the expanded spatial window used for noise sampling
    comp_h, comp_w = data_2d[ctx.anchor_key].shape[:2]
    compute_window = rasterio.windows.Window(
        col_off=inner_window.col_off - c_pad, row_off=inner_window.row_off - r_pad, width=comp_w,
        height=comp_h
    )

    return data_2d, masks_2d, compute_window, h, w


def _slice_collection(collection: Dict[Any, np.ndarray], slices: Tuple[slice, slice]):
    sy, sx = slices
    return {k: v[sy, sx, ...] for k, v in collection.items()}


def print_statistics(stats: dict, proc_start: float, launch_elapsed: float, registry_meta: dict):
    """
    Prints the Engine Tuning Report.

    Args:
        stats: {read, render, write, idle, count}
        proc_start: Time when writer process began loop
        launch_elapsed: Time from CLI start to engine start (Launch Tax)
        registry_meta: {hits, misses, static_used, static_total, is_cold}
    """
    # 1. TIME CALCULATIONS
    count = stats["count"] or 1  # prevent div by zero
    avg_read = stats["read"] / count
    avg_render = stats["render"] / count
    avg_write = stats["write"] / count

    # 2. CACHE MATH
    total_reqs = registry_meta["hits"] + registry_meta["misses"]
    hit_ratio = (registry_meta["hits"] / total_reqs * 100) if total_reqs > 0 else 0
    cache_state = "COLD (Priming)" if registry_meta["is_cold"] else "WARM"

    # 3. BALANCE ANALYSIS
    # If Render time is significantly higher than Read time, you are CPU bound.
    # If Idle time is high, workers are starving.
    balance_ratio = stats["render"] / max(stats["read"], 0.001)

    print("\n" + "=" * 60)
    print(f"{'ENGINE TUNING REPORT':^60}")
    print("=" * 60)

    # --- SECTION: CACHE ---
    print(f"CACHE PERFORMANCE [{cache_state}]:")
    print(f"  Hits:          {registry_meta['hits']} ({hit_ratio:2.1f}%)")
    print(f"  Misses:        {registry_meta['misses']}")
    print(
        f"  SHM Saturation: Static {registry_meta['static_used']}/{registry_meta['static_total']}"
    )

    # Suggested Action for Cache
    if hit_ratio < 20 and not registry_meta["is_cold"]:
        print("  ADVICE:        Low hit ratio. Increase static_ratio in config.")
    elif registry_meta['static_used'] == registry_meta['static_total']:
        print("  ADVICE:        Static zone full. Increase total SHM slots.")
    print("-" * 60)

    # --- SECTION: PROCESS BALANCE ---
    print(f"{'Stage':<12} | {'Total Time':<10} | {'Avg/Tile':<10} | {'Status'}")
    print(f"{'-' * 12}-|-{'-' * 10}-|-{'-' * 10}-|-{'-' * 10}")

    read_status = "Bottleneck" if balance_ratio < 0.5 else "Efficient"
    render_status = "Saturated" if balance_ratio > 2.0 else "Balanced"

    print(f"{'Reading':<12} | {stats['read']:9.2f}s | {avg_read:9.3f}s | {read_status}")
    print(f"{'Rendering':<12} | {stats['render']:9.2f}s | {avg_render:9.3f}s | {render_status}")
    print(
        f"{'Writing':<12} | {stats['write']:9.2f}s | {avg_write:9.3f}s | "
        f"{'Idle' if stats['idle'] > stats['write'] else 'Busy'}"
    )

    print("-" * 60)

    # --- SECTION: QUEUE HEALTH ---
    # Idle time measures the gaps where the Writer was waiting for the Renderers
    print(f"WRITER STARVATION: {stats['idle']:5.2f}s (Writer was waiting for work)")

    # --- FINAL ADVICE ---
    if balance_ratio > 2.5:
        print("\nACTION: System is COMPUTE BOUND. Increase RENDERER_COUNT.")
    elif balance_ratio < 0.4:
        print("\nACTION: System is IO BOUND. Increase READER_COUNT.")
    else:
        print("\nACTION: Pipeline is well-balanced.")

    print("=" * 60 + "\n")
