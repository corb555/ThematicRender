import multiprocessing
from typing import Optional

import setproctitle

from ThematicRender.engine_resources import JobContextStore
from ThematicRender.ipc_packets import (BlockReadPacket, Op, Envelope, ErrorPacket, send_error,
                                        BlockLoadedPacket, SEV_CANCEL, SEV_FATAL, )
from ThematicRender.keys import DriverKey
from ThematicRender.utils import window_from_rect
from ThematicRender.worker_context_base import (close_worker_ctx, sync_ctx_for_packet, )
from ThematicRender.worker_contexts import ReaderContext


def load_reader_job_ctx(job_id: str, shm_store: JobContextStore) -> ReaderContext:
    """Load the reader context for a specific job from shared job storage."""
    try:
        return shm_store.get_reader_context(job_id)
    except Exception as exc:
        raise RuntimeError(
            f"[READER] Failed to load ReaderContext for job '{job_id}': {exc}"
        ) from exc


def reader_loop(read_q, status_q, shm_name: str, pool_map) -> None:
    """Run the persistent reader worker loop."""
    section = "READER"
    shm_store = JobContextStore(name=shm_name)
    ctx: Optional[ReaderContext] = None
    setproctitle.setproctitle(multiprocessing.current_process().name)

    try:
        while True:
            envelope: Envelope = read_q.get()

            match envelope.op:
                case Op.LOAD_BLOCK:
                    section = "READER LOAD_TILE"
                    packet: BlockReadPacket = envelope.payload
                    try:
                        ctx = sync_ctx_for_packet(
                            ctx=ctx, packet_job_id=packet.job_id, shm_store=shm_store,
                            load_ctx=load_reader_job_ctx, err_prefix=section, )

                        if ctx is None:
                            # Stale packet for an old/cancelled job; ignore it.
                            continue

                        tile_seq, driver_id, slot_id, duration = read_task(packet, ctx, pool_map)
                        payload = BlockLoadedPacket(
                            job_id=packet.job_id, tile_id=tile_seq, driver_id=driver_id,
                            read_duration=duration
                        )

                        status_q.put(
                            Envelope(
                                op=Op.BLOCK_LOADED, payload=payload
                            )
                        )
                    except (ValueError, FileNotFoundError, OSError) as exc:
                        # JOB-LEVEL FAILURE
                        # We log it, notify Orch to cancel the job, but KEEP the reader alive.
                        payload = ErrorPacket(
                            job_id=packet.job_id, tile_id=packet.tile_id, section=section,
                            severity=SEV_CANCEL,
                            message=f"{section} Error on {packet.driver_id.value}: {exc}"
                        )
                        send_error(status_q, payload)

                    except Exception as e:
                        # SYSTEM-LEVEL FAILURE
                        # Something is fundamentally broken. Report and exit this process.
                        import traceback
                        traceback.print_exc()

                        payload = ErrorPacket(
                            job_id=packet.job_id if packet else "unknown",
                            tile_id=packet.tile_id if packet else -1, section=section,
                            severity=SEV_FATAL, message=f"CRITICAL: {type(e).__name__}: {e}"
                        )
                        send_error(status_q, payload)
                        break  # Exit loop, process dies
                case Op.SHUTDOWN:
                    print(f"[{section}] Shutting down")
                    break

                case _:
                    payload = ErrorPacket(
                        severity=SEV_FATAL, job_id="unknown", tile_id=-1, section=section,
                        message=f"{section} Unknown OpCode: {envelope.op!r}", )
                    send_error(status_q, payload)
    finally:
        close_worker_ctx(ctx)


def read_task(
        packet: BlockReadPacket, ctx: ReaderContext, pool_map: dict
) -> tuple[int, DriverKey, int, float]:
    """Read one driver block directly into its pre-allocated SHM slot."""
    import time
    start_time = time.perf_counter()

    window = window_from_rect(packet.window_rect)
    pool = pool_map[packet.driver_id]

    # 1. Get views into the Shared Memory slot assigned by the Orchestrator
    # We slice [packet.target_slot_id] to get the specific block in the pool
    data_view = pool.data_buf[packet.target_slot_id]
    mask_view = pool.mask_buf[packet.target_slot_id]

    # 2. Perform Direct Read into SHM
    # We pass data_view[0] because Rasterio expects a 2D array for a single-band read
    ctx.io.read_into_shm(
        key=packet.driver_id, window=window, halo=packet.halo, out_data=data_view,
        out_mask=mask_view
    )

    duration = time.perf_counter() - start_time
    return packet.tile_id, packet.driver_id, packet.target_slot_id, duration
