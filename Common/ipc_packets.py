from dataclasses import dataclass
from enum import IntEnum
from typing import Optional, Tuple, Dict, Any, TypeAlias

import numpy as np
from Common.keys import DriverKey

# ipc_packets.py

WindowRect: TypeAlias = tuple[int, int, int, int]

WKR_TIMEOUT = 7.0
ORCH_TIMEOUT = 10.0

SEV_FATAL = 0  # Triggers a  system Shutdown.
SEV_CANCEL = 1  # Triggers a Job Cancellation.
SEV_WARNING = 2  # Job continues. Logged and sent to the client

# Shared Mem Special Job Ids
JOB_ID_SHUTTING_DOWN = "-3"
JOB_ID_JOB_CANCELLED = "-2"
JOB_ID_IDLE = "-1"


# Message Operations
class Op(IntEnum):
    JOB_REQUEST = 0  # Client -> Orch: New Job
    JOB_DONE = 1  # Orch -> Client: Job Done
    JOB_CANCEL = 2
    LOAD_BLOCK = 3  # Orch -> Reader: Load Block
    BLOCK_LOADED = 4  # Reader -> Orch: Block loaded
    RENDER_TILE = 5  # Orch -> Render: Render Tile
    WRITE_TILE = 6  # Orch -> Writer: Write Tile
    TILE_WRITTEN = 7  # Writer -> Orch: Tile Written
    TILES_FINALIZED = 8  # Writer -> Orch:  Output Finalized
    WRITER_ABORTED = 9
    TELEMETRY = 10
    ERROR = 11  # Any -> Orch: Error occurred
    SHUTDOWN = 12  # Client -> Orch: SHutdown


@dataclass(frozen=True, slots=True)
class Envelope:
    """The standard container for all Queue communications."""
    op: Op
    payload: Any = None


@dataclass(frozen=True, slots=True)
class DriverBlockRef:
    slot_id: int
    data_h_w: Tuple[int, int]
    inner_slices: Optional[Tuple[slice, slice]] = None


@dataclass(frozen=True, slots=True)
class RenderPacket:
    job_id: str
    tile_id: int
    window_rect: WindowRect
    block_map: Dict[DriverKey, DriverBlockRef]  # All the blocks for this Tile
    read_duration: float = 0.0  # Sum of all driver reads for this tile
    queued_at: float = 0.0  # When the coordinator put this in the queue


@dataclass(frozen=True, slots=True)
class WriterPacket:
    job_id: str
    tile_id: int
    window_rect: WindowRect
    refs: Dict[DriverKey, DriverBlockRef]
    img_block: np.ndarray
    out_ref: DriverBlockRef
    read_duration: float = 0.0  # Carried from WorkPacket
    render_duration: float = 0.0  # Time spent in actual math
    worker_idle_time: float = 0.0  # Time worker spent waiting for work_queue
    queued_at: float = 0.0  # When the renderer put this in the result queue


@dataclass(frozen=True, slots=True)
class TileWrittenPacket:
    job_id: str
    tile_id: int


@dataclass(frozen=True, slots=True)
class BlockReadPacket:
    job_id: str
    tile_id: int
    driver_id: DriverKey
    window_rect: WindowRect
    target_slot_id: int
    halo: int = 0
    queued_at: float = 0.0  # When the coordinator put this in the queue


@dataclass(frozen=True, slots=True)
class BlockLoadedPacket:
    job_id: str
    tile_id: int
    driver_id: DriverKey
    read_duration: float


@dataclass(frozen=True, slots=True)
class JobDonePacket:
    job_id: str


@dataclass(frozen=True, slots=True)
class ErrorPacket:
    job_id: str
    tile_id: int
    section: str
    severity: int
    message: str


def send_error(q, payload: ErrorPacket):
    q.put(Envelope(op=Op.ERROR, payload=payload), timeout=1.0)
