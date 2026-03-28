from multiprocessing.shared_memory import SharedMemory
import pickle
import struct
from typing import Optional

from Common.ipc_packets import JOB_ID_JOB_CANCELLED, JOB_ID_SHUTTING_DOWN


class JobContextStore:
    """
    Manages a dedicated Shared Memory segment for Job Contexts.
    Layout: [64b JobID] [4b R_Len] [4b W_Len] [4b WR_Len] [Data Blobs...]
    """
    HEADER_FORMAT = "64s III"  # JobID, ReaderLen, WorkerLen, WriterLen
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, name: Optional[str] = None, size: int = 4 * 1024 * 1024):
        self.len_wtr_bin = 0
        self.len_rndr_bin = 0
        self.len_r_bin = 0

        if name:  # Worker Side: Attach
            self.shm = SharedMemory(name=name)
            self.owner = False
        else:  # Orchestrator Side: Create
            self.shm = SharedMemory(create=True, size=size)
            self.owner = True

        self.size = size

    def write_contexts(self, job_id: str, rdr_ctx, rndr_ctx, wtr_ctx):
        """
        Serializes and writes all contexts to SHM.
        Data is written first; Header is written last to ensure workers
        never read a new Job ID before its data is ready.
        """
        # 1. Prepare Pickles
        r_bin = pickle.dumps(rdr_ctx)
        rndr_bin = pickle.dumps(rndr_ctx)
        wtr_bin = pickle.dumps(wtr_ctx)

        total_needed = self.HEADER_SIZE + len(r_bin) + len(rndr_bin) + len(wtr_bin)
        if total_needed > self.size:
            raise MemoryError(
                f"Contexts ({total_needed:,} bytes) exceed SHM size ({self.size:,})."
                f"rdr {len(r_bin):,} wtr {len(wtr_bin):,} rndr {len(rndr_bin):,}"
            )

        # print(f"HASH STEP 3 - update_context.  logic hash={rndr_ctx.resources.logic_hash}")

        # 2. Calculate Offsets (Skipping the Header space)
        r_start = self.HEADER_SIZE
        w_start = r_start + len(r_bin)
        wr_start = w_start + len(rndr_bin)

        # 3. WRITE DATA BLOBS FIRST
        # These bytes are "invisible" to workers because the Job ID hasn't changed yet
        self.shm.buf[r_start: r_start + len(r_bin)] = r_bin
        self.shm.buf[w_start: w_start + len(rndr_bin)] = rndr_bin
        self.shm.buf[wr_start: wr_start + len(wtr_bin)] = wtr_bin

        self.len_r_bin = len(r_bin)
        self.len_rndr_bin = len(rndr_bin)
        self.len_wtr_bin = len(wtr_bin)

        # 5. THE "COMMIT" STEP
        # We overwrite the first HEADER_SIZE bytes in a single assignment.
        # This acts as the signal to all workers that the new data is ready.
        self._write_header(job_id, len(r_bin), len(rndr_bin), len(wtr_bin))

    def _write_header(
            self, job_id: str, len_r: Optional[int] = None, len_rndr: Optional[int] = None,
            len_wtr: Optional[int] = None
    ):
        """
        Updates the internal state and performs the atomic SHM commit.
        If a length is None, it persists the last known value.
        """
        # Update internal state only if new values are provided
        if len_r is not None:    self.len_r_bin = len_r
        if len_rndr is not None: self.len_rndr_bin = len_rndr
        if len_wtr is not None:  self.len_wtr_bin = len_wtr

        # Pack the header using the (potentially updated) internal state
        header = struct.pack(
            self.HEADER_FORMAT, job_id.encode('utf-8')[:64], self.len_r_bin, self.len_rndr_bin,
            self.len_wtr_bin
        )

        # THE ATOMIC COMMIT
        # Overwriting the header slice in one operation signals workers
        self.shm.buf[:self.HEADER_SIZE] = header

    def set_job_cancel(self):
        self._write_header(JOB_ID_JOB_CANCELLED)

    def set_shutdown(self):
        self._write_header(JOB_ID_SHUTTING_DOWN)

    def get_job_id(self):
        """
        Reads the SHM header.
        If expected_job_id is provided, raises exception on mismatch.
        """
        raw_id, r_len, w_len, wr_len = struct.unpack(
            self.HEADER_FORMAT, self.shm.buf[:self.HEADER_SIZE]
        )
        found_id = raw_id.decode('utf-8').strip('\x00')

        return found_id

    def cleanup(self):
        self.shm.close()
        if self.owner:
            self.shm.unlink()

    def _read_header(self, expected_job_id: Optional[str] = None):
        """
        Reads the SHM header.
        If expected_job_id is provided, raises exception on mismatch.
        """
        raw_id, r_len, w_len, wr_len = struct.unpack(
            self.HEADER_FORMAT, self.shm.buf[:self.HEADER_SIZE]
        )
        found_id = raw_id.decode('utf-8').strip('\x00')

        if expected_job_id and found_id != expected_job_id:
            raise RuntimeError(
                f"Job ID Mismatch in SHM. Requested: {expected_job_id}, Found: {found_id}"
            )

        return found_id, r_len, w_len, wr_len

    def get_reader_context(self, job_id: str):
        # read_header raises exception if job_id not found
        _, r_len, _, _ = self._read_header(job_id)
        start = self.HEADER_SIZE
        return pickle.loads(self.shm.buf[start:start + r_len])

    def get_worker_context(self, job_id: str):
        # read_header raises exception if job_id not found
        _, r_len, w_len, _ = self._read_header(job_id)
        start = self.HEADER_SIZE + r_len
        return pickle.loads(self.shm.buf[start:start + w_len])

    def get_writer_context(self, job_id: str):
        # read_header raises exception if job_id not found
        _, r_len, w_len, wr_len = self._read_header(job_id)
        start = self.HEADER_SIZE + r_len + w_len
        return pickle.loads(self.shm.buf[start:start + wr_len])
