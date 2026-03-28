from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Optional, Protocol, TypeVar

from Pipeline.engine_resources import JobContextStore


class HasJobId(Protocol):
    """Protocol for payloads that carry a job_id."""
    job_id: str


@dataclass(slots=True)
class WorkerContextBase:
    """Base class for process-local worker job context."""
    job_id: str

    def matches_job_id(self, job_id: str) -> bool:
        """Return whether this context belongs to the requested job."""
        return self.job_id == job_id

    def open_local_resources(self) -> None:
        """Open process-local resources for this job."""
        raise NotImplementedError

    def close_local_resources(self) -> None:
        """Close process-local resources for this job."""
        raise NotImplementedError


CTX = TypeVar("CTX", bound=WorkerContextBase)


def get_payload_job_id(payload: Any) -> str:
    """Return the job_id from a packet payload."""
    job_id = getattr(payload, "job_id", None)
    if not job_id:
        raise RuntimeError(f"Payload does not contain a valid job_id: {payload!r}")
    return job_id


def sync_ctx_for_packet(
        *, ctx: Optional[CTX], packet_job_id: str, shm_store: JobContextStore,
        load_ctx: Callable[[str, JobContextStore], CTX], err_prefix: str, ) -> Optional[CTX]:
    """
    Synchronize local worker context against the authoritative SHM job header.
    """
    shm_job_id = shm_store.get_job_id()

    # --- RULE 0: Authoritative State Check ---
    # If SHM says we are in a non-active state (Idle=-1, Cancel=-2, Shutdown=-3),
    # we must immediately stop processing and release resources.
    try:
        # Convert to int to check for negative flags
        state_val = int(shm_job_id)
        if state_val < 0:
            if ctx is not None:
                print(f"🛑 [{err_prefix}] SHM State {state_val} detected. Closing local resources.")
                ctx.close_local_resources()
            return None
    except (ValueError, TypeError):
        # shm_job_id is a valid string ID (e.g., '36'), proceed normally
        pass

    # --- RULE 1: Stale Packet Check ---
    if packet_job_id != shm_job_id:
        # Packet is from an old job or a different job entirely
        return None

    # --- RULE 2: Context Loading / Reloading ---
    if ctx is None or ctx.job_id != shm_job_id:
        if ctx is not None:
            ctx.close_local_resources()

        # Load the new context from SHM
        ctx = load_ctx(packet_job_id, shm_store)

        # --- THE SAFETY CHECK ---
        # Before opening files (Rasterio), ensure we didn't just
        # get cancelled while we were loading the context.
        final_check_id = shm_store.get_job_id()
        try:
            if int(final_check_id) < 0:
                ctx.close_local_resources()
                return None
        except (ValueError, TypeError):
            pass

        # Now safe to hit the disk/GDAL
        ctx.open_local_resources()

    return ctx


def close_worker_ctx(ctx: Optional[WorkerContextBase]) -> None:
    """Best-effort cleanup for a worker context."""
    if ctx is not None:
        ctx.close_local_resources()
