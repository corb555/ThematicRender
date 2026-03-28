import multiprocessing
import sys
import traceback
from typing import Optional

import setproctitle

from Common.ipc_packets import WriterPacket, JobDonePacket, TileWrittenPacket, Op, Envelope, \
    ErrorPacket, send_error, BlockLoadedPacket, SEV_CANCEL, SEV_FATAL

from Pipeline.engine_resources import JobContextStore
from Pipeline.worker_context_base import (close_worker_ctx, sync_ctx_for_packet, )
from Pipeline.worker_contexts import WriterContext, WorkerContext, ReaderContext

from Render.task_routines import write_task, read_task, render_task, RenderWorkspace


# render_task.py


def load_reader_job_ctx(job_id: str, shm_store: JobContextStore) -> ReaderContext:
    """Load the reader context for a specific job from shared job storage."""
    try:
        return shm_store.get_reader_context(job_id)
    except Exception as exc:
        raise RuntimeError(
            f"[READER] Failed to load ReaderContext for job '{job_id}': {exc}"
        ) from exc


def reader_loop(read_q, status_q, shm_name: str, pool_map) -> None:
    """The 'Air-lock': Translates IPC messages into Rendering Tasks."""
    section = "READER"
    setproctitle.setproctitle(multiprocessing.current_process().name)

    shm_store = JobContextStore(name=shm_name)
    ctx: Optional[ReaderContext] = None

    while True:
        envelope: Envelope = read_q.get()
        if envelope.op == Op.SHUTDOWN: break

        if envelope.op == Op.LOAD_BLOCK:
            packet = envelope.payload

            # 1. System Sync (Authoritative state check)
            ctx = sync_ctx_for_packet(
                ctx=ctx, packet_job_id=packet.job_id, shm_store=shm_store,
                load_ctx=load_reader_job_ctx, err_prefix=section
            )
            if ctx is None: continue

            try:
                # 2. Resource Management (The 'Slot' level)
                # The Loop is responsible for the Pool and SHM logic
                pool = pool_map[packet.driver_id]
                data_view = pool.data_buf[packet.target_slot_id]
                mask_view = pool.mask_buf[packet.target_slot_id]

                # 3. Execution (The 'Buffer' level)
                # We pass the pure NumPy views to the Science layer
                duration = read_task(packet, ctx.io, data_view, mask_view)

                # 4. IPC Feedback
                status_q.put(
                    Envelope(
                        op=Op.BLOCK_LOADED, payload=BlockLoadedPacket(
                            job_id=packet.job_id, tile_id=packet.tile_id,
                            driver_id=packet.driver_id, read_duration=duration
                        )
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
                    tile_id=packet.tile_id if packet else -1, section=section, severity=SEV_FATAL,
                    message=f"CRITICAL: {type(e).__name__}: {e}"
                )
                send_error(status_q, payload)
                break  # Exit loop, process dies

            finally:
                close_worker_ctx(ctx)


def load_worker_job_ctx(job_id: str, shm_store: JobContextStore) -> WorkerContext:
    """Load the worker context for a specific job from shared job storage."""
    try:
        return shm_store.get_worker_context(job_id)
    except Exception as exc:
        raise RuntimeError(
            f"[RENDER] Failed to load WorkerContext for job '{job_id}': {exc}"
        ) from exc

def render_loop(work_q, writer_q, status_q, shm_name, out_pool, pool_map):
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
            ctx=ctx, packet_job_id=packet.job_id, shm_store=shm_store, load_ctx=load_worker_job_ctx,
            err_prefix="WORKER"
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
                        payload = ErrorPacket(
                            job_id=packet.job_id, tile_id=-1, section="wrn", severity=SEV_CANCEL,
                            message=f"Render {section} err='{e}'"
                        )
                        send_error(status_q, payload)
                    except Exception as e:
                        # SEV_FATAL: Notify Orch and EXIT the process
                        stack_trace_str = traceback.format_exc()
                        payload = ErrorPacket(
                            job_id=packet.job_id, tile_id=-1, section="excep", severity=SEV_FATAL,
                            message=f"Render {section} Error {e} {stack_trace_str}"
                        )
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
                        job_id=packet.job_id, tile_id=-1, section=section, severity=SEV_FATAL,
                        message=f"{section} Unknown message rcvd"
                    )
                    send_error(status_q, payload)
        except ValueError as e:
            print(f"{section} RENDER1 ERROR {e}")
            payload = ErrorPacket(
                job_id=packet.job_id, tile_id=-1, section=section, severity=SEV_CANCEL,
                message=f"Warning: {e}"
            )
            send_error(status_q, payload)
        except MemoryError as e:
            print(f"{section} RENDER2 Exception {e}")
            payload = ErrorPacket(
                job_id=packet.job_id, tile_id=-1, section=section, severity=SEV_FATAL,
                message=f"Fatal: {e}"
            )
            send_error(status_q, payload)
            sys.exit(1)


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
