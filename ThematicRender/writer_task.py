import multiprocessing
from typing import Optional

import rasterio
import rasterio.windows
import setproctitle

from ThematicRender.engine_resources import JobContextStore
from ThematicRender.ipc_packets import WriterPacket, Op, Envelope, JobDonePacket, ErrorPacket, \
    send_error, TileWrittenPacket, SEV_FATAL, SEV_CANCEL
from ThematicRender.utils import window_from_rect
from ThematicRender.worker_context_base import sync_ctx_for_packet
from ThematicRender.worker_contexts import WriterContext


def load_writer_job_ctx(job_id: str, shm_store: JobContextStore) -> WriterContext:
    """Load the writer context for a specific job from shared job storage."""
    section = "WRITER - load job ctx"
    try:
        return shm_store.get_writer_context(job_id)
    except Exception as exc:
        raise RuntimeError(
            f"{section} Failed to load WriterContext for job '{job_id}': {exc}"
        ) from exc


def writer_loop(write_q, status_q, shm_name: str, out_pool) -> None:
    section = "WRITER"
    shm_store = JobContextStore(name=shm_name)
    ctx: Optional[WriterContext] = None
    setproctitle.setproctitle(multiprocessing.current_process().name)

    try:
        while True:
            envelope: Envelope = write_q.get()

            match envelope.op:
                case Op.WRITE_TILE:
                    try:
                        packet: WriterPacket = envelope.payload
                        old_ctx_id = id(ctx)
                        ctx = sync_ctx_for_packet(
                            ctx=ctx, packet_job_id=packet.job_id, shm_store=shm_store,
                            load_ctx=load_writer_job_ctx, err_prefix=section, )

                        if ctx is None: continue  # ignore stale packet

                        # Write out the tile
                        write_task(packet=packet, ctx=ctx, out_pool=out_pool)

                        payload = TileWrittenPacket(job_id=packet.job_id, tile_id=packet.tile_id)
                        status_q.put(Envelope(op=Op.TILE_WRITTEN, payload=payload))

                    except (ValueError, FileNotFoundError, OSError) as exc:
                        # Notify Orch so it can transition to CANCELLING
                        payload = ErrorPacket(
                            job_id=packet.job_id, tile_id=packet.tile_id, section=section,
                            severity=SEV_CANCEL, message=str(exc)
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
                        break

                case Op.JOB_DONE | Op.JOB_CANCEL:
                    is_cancel = (envelope.op == Op.JOB_CANCEL)
                    packet: JobDonePacket = envelope.payload

                    if ctx is not None and ctx.matches_job_id(packet.job_id):
                        # 1. Close the file handle
                        ctx.close_local_resources()

                        # 2. If CANCELLED, delete the partial file per spec 5.2
                        if is_cancel:
                            try:
                                if ctx.output_path.exists():
                                    ctx.output_path.unlink()
                            except Exception as e:
                                print(f"⚠️ [Writer] Failed to delete cancelled file: {e}")

                            # 3. THE HANDSHAKE: Notify Orch that cleanup is done
                            status_q.put(Envelope(op=Op.WRITER_ABORTED, payload=packet.job_id))

                        elif envelope.op == Op.JOB_DONE:
                            # Notify Success
                            status_q.put(Envelope(op=Op.TILES_FINALIZED, payload=packet.job_id))

                        ctx = None

                case Op.SHUTDOWN:
                    if ctx:
                        ctx.close_local_resources()
                        # unlink on shutdown to prevent artifacts
                        if ctx.output_path.exists(): ctx.output_path.unlink()
                    break

                case _:
                    payload = ErrorPacket(
                        "-1", -1, section, severity=SEV_FATAL,
                        message=f"Unknown OpCode: {envelope.op!r}"
                        )
                    send_error(status_q, payload)

    except Exception as e:
        # SYSTEM-LEVEL FAILURE
        # Something is fundamentally broken. Report and exit this process.
        import traceback
        traceback.print_exc()

        payload = ErrorPacket(
            job_id=packet.job_id if packet else "unknown", tile_id=packet.tile_id if packet else -1,
            section=section, severity=SEV_FATAL, message=f"CRITICAL: {type(e).__name__}: {e}"
        )
        send_error(status_q, payload)


def write_task(*, packet: WriterPacket, ctx: WriterContext, out_pool) -> None:
    """Write a rendered tile to disk and release transient output resources."""
    try:
        window = window_from_rect(packet.window_rect)
        local_window = rasterio.windows.Window(
            col_off=int(window.col_off) - int(ctx.write_offset_col),
            row_off=int(window.row_off) - int(ctx.write_offset_row), width=int(window.width),
            height=int(window.height)
        )

        if packet.img_block is None:
            raise ValueError("Packet img is empty.")

        ctx.dst.write(packet.img_block, window=local_window)

    finally:
        # Ensure the SHM slot is ALWAYS returned to the pool,
        # even if the disk write failed. This prevents pool exhaustion.
        if packet.out_ref:
            out_pool.release(packet.out_ref.slot_id)
