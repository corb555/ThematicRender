from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Optional, Protocol, TypeVar

from ThematicRender.engine_resources import JobContextStore
from ThematicRender.utils import dot_get


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

    Rules:
    1. If packet job_id != SHM header job_id, the packet is stale -> discard.
    2. If local ctx is missing or for a different job, reload from SHM.
    3. Otherwise keep using the current ctx.

    Returns:
        The synchronized context, or None if the packet is stale and should be discarded.
    """
    shm_job_id = shm_store.get_job_id()

    if packet_job_id != shm_job_id:
        # Packet is not for our current job in shmem
        print(f"*** JOB CONTEXT MISMATCH. Packet Job: {packet_job_id} ShmMem Job: {shm_job_id}")
        return None

    if ctx is None or ctx.job_id != shm_job_id:
        if ctx is not None:
            ctx.close_local_resources()
        ctx = load_ctx(packet_job_id, shm_store)
        if hasattr(ctx, 'resources'):
            res = ctx.resources
            # print(f"HASH 4 - sync_ctx - NEW load_ctx: logic hash: {res.logic_hash}")
            if hasattr(res, 'render_cfg'):
                opa = dot_get(res.render_cfg, "drivers.water.max_opacity")
                print(f"opacity: {opa}")
        # else:
        #    print(f"🔄 [sync_ctx] Reloaded context for {err_prefix} (Job: {ctx.job_id})")

        ctx.open_local_resources()

    return ctx


def close_worker_ctx(ctx: Optional[WorkerContextBase]) -> None:
    """Best-effort cleanup for a worker context."""
    if ctx is not None:
        ctx.close_local_resources()
