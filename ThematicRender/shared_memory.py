import collections
from dataclasses import dataclass
from multiprocessing import shared_memory, Queue
from typing import Optional, Tuple, Dict
import uuid

import numpy as np
from rasterio.windows import Window

from ThematicRender.ipc_packets import DriverBlockRef, WKR_TIMEOUT
from ThematicRender.keys import DriverKey

# shared_memory
Slice2D = Tuple[slice, slice]


@dataclass(frozen=True, slots=True)
class DriverBlockView:
    slot_id: int
    data: np.ndarray
    mask: np.ndarray
    data_h_w: Tuple[int, int]
    mask_h_w: Tuple[int, int]
    inner_slices: Optional[Slice2D] = None


@dataclass(frozen=True)
class PoolSpec:
    data_shape: tuple  # (Bands, H, W) e.g., (1, 384, 384)
    data_dtype: np.dtype
    mask_shape: tuple  # (Bands, H, W) e.g., (1, 384, 384)
    mask_dtype: np.dtype


class BlockPool:
    def __init__(self, spec: PoolSpec, slots: int) -> None:
        self.spec = spec
        self.slots = slots
        self._data = np.empty((slots, *spec.data_shape), dtype=spec.data_dtype)
        self._mask = np.empty((slots, *spec.mask_shape), dtype=spec.mask_dtype)
        self._free: list[int] = list(range(slots))

    def __getitem__(self, slot_id: int) -> 'DriverBlockView':
        data = self._data[slot_id]
        mask = self._mask[slot_id]

        return DriverBlockView(
            slot_id=slot_id, data=data, mask=mask, data_h_w=(data.shape[1], data.shape[2]),
            mask_h_w=(mask.shape[1], mask.shape[2]), inner_slices=None, )

    def acquire(self) -> int:
        if not self._free:
            raise RuntimeError("BlockPool exhausted: no free slots.")
        return self._free.pop()

    def release(self, slot_id: int) -> None:
        if not (0 <= slot_id < self.slots):
            raise ValueError(f"slot_id out of range: {slot_id}")
        self._free.append(slot_id)

    def write(
            self, slot_id: int, data: np.ndarray, mask: np.ndarray,
            inner_slices: Optional[Tuple[slice, slice]] = None, pad_data: float = 0.0,
            pad_mask: float = 0.0
    ) -> 'DriverBlockRef':
        """
        Coerces any input into the (Band, H, W) storage slot.
        Accepts pad arguments to match RasterManager signature.
        """
        # Standardize Data to 3D (B, H, W)
        v_data = data[np.newaxis, ...] if data.ndim == 2 else data
        # Standardize Mask to 3D (B, H, W)
        v_mask = mask[np.newaxis, ...] if mask.ndim == 2 else mask

        vb, vh, vw = v_data.shape
        mb, mh, mw = v_mask.shape

        # 4D Write: [Slot, Band, Row, Col]
        self._data[slot_id, :vb, :vh, :vw] = v_data
        self._mask[slot_id, :mb, :mh, :mw] = v_mask

        return DriverBlockRef(
            slot_id=slot_id, data_h_w=(vh, vw), inner_slices=inner_slices
        )

    def view(self, ref: 'DriverBlockRef') -> DriverBlockView:
        data = self._data[ref.slot_id, :, :ref.data_h_w[0], :ref.data_h_w[1]]
        mask = self._mask[ref.slot_id, :, :ref.data_h_w[0], :ref.data_h_w[1]]

        return DriverBlockView(
            slot_id=ref.slot_id, data=data, mask=mask, data_h_w=ref.data_h_w,
            mask_h_w=(mask.shape[-2], mask.shape[-1]), inner_slices=ref.inner_slices, )


def _standardize_shape(shape: tuple) -> tuple:
    """Forces any 2D or 3D shape into a 3D (Bands, H, W) tuple."""
    if len(shape) == 2:  # (H, W) -> (1, H, W)
        return 1, shape[0], shape[1]
    if len(shape) == 3:
        if shape[2] <= 4:  # (H, W, B) -> (B, H, W)
            return shape[2], shape[0], shape[1]
    return shape  # Already (B, H, W)


class SharedMemoryPool:
    def __init__(self, spec: PoolSpec, slots: int, prefix: str):
        self.prefix = prefix
        self.spec = spec
        self.slots = slots
        self.session_id = str(uuid.uuid4())[:8]

        # 1. Handshake
        sig_name = f"{prefix}_sig"
        self._sig_shm = self._create_shm(sig_name, 64)
        self._sig_shm.buf[:8] = self.session_id.encode('ascii')

        # 2. Define Shapes and Dtypes
        self._d_shape = (slots, *_standardize_shape(spec.data_shape))
        self._m_shape = (slots, *_standardize_shape(spec.mask_shape))
        self._d_name = f"{prefix}_data"
        self._m_name = f"{prefix}_mask"

        # 3. Allocation (Main Process)
        v_size = int(np.prod(self._d_shape) * np.dtype(spec.data_dtype).itemsize)
        m_size = int(np.prod(self._m_shape) * np.dtype(spec.mask_dtype).itemsize)

        self._d_shm = self._create_shm(self._d_name, v_size)
        self._m_shm = self._create_shm(self._m_name, m_size)

        # 4. Local Cache for NumPy Views
        # These will be None when the object is unpickled in a worker process
        self._d_buf_local = None
        self._m_buf_local = None

        # Queue for slot management
        self._available_slots = Queue()
        for i in range(slots):
            # print(f">>> SHM PUT >>>")
            self._available_slots.put(i)

    @property
    def data_buf(self) -> np.ndarray:
        """Process-safe access to the Value buffer."""
        if self._d_buf_local is None:
            # Re-attach to the SHM handle (which was pickled/unpickled)
            # and wrap it in a fresh numpy array for this process.
            self._d_buf_local = np.ndarray(
                self._d_shape, dtype=self.spec.data_dtype, buffer=self._d_shm.buf
            )
        return self._d_buf_local

    @property
    def mask_buf(self) -> np.ndarray:
        """Process-safe access to the Validity buffer."""
        if self._m_buf_local is None:
            self._m_buf_local = np.ndarray(
                self._m_shape, dtype=self.spec.mask_dtype, buffer=self._m_shm.buf
            )
        return self._m_buf_local

    def write_at_slot(self, slot_id: int, data: np.ndarray, mask: np.ndarray):
        """Directly writes to a specific slot without managing acquisition."""
        # Ensure 3D (B, H, W)
        d_in = data[np.newaxis, ...] if data.ndim == 2 else data
        m_in = mask[np.newaxis, ...] if mask.ndim == 2 else mask

        db, dh, dw = d_in.shape
        mb, mh, mw = m_in.shape

        # data_buf and mask_buf are the @property numpy views
        self.data_buf[slot_id, :db, :dh, :dw] = d_in
        self.mask_buf[slot_id, :mb, :mh, :mw] = m_in

    def write(self, slot_id: int, data: np.ndarray, mask: np.ndarray, **kwargs) -> 'DriverBlockRef':
        # 1. Ensure inputs are 3D (B, H, W)
        d_in = data[np.newaxis, ...] if data.ndim == 2 else data
        m_in = mask[np.newaxis, ...] if mask.ndim == 2 else mask

        db, dh, dw = d_in.shape
        mb, mh, mw = m_in.shape

        # 2. Write to the lazy-attached buffers
        # The slice handles edge tiles (h, w < 256) correctly.
        self.data_buf[slot_id, :db, :dh, :dw] = d_in
        self.mask_buf[slot_id, :mb, :mh, :mw] = m_in

        return DriverBlockRef(
            slot_id=slot_id, data_h_w=(dh, dw), inner_slices=kwargs.get('inner_slices')
        )

    def view(self, ref: DriverBlockRef) -> DriverBlockView:
        h, w = ref.data_h_w
        data = self.data_buf[ref.slot_id, :, :h, :w]
        mask = self.mask_buf[ref.slot_id, :, :h, :w]
        return DriverBlockView(
            slot_id=ref.slot_id, data=data, mask=mask, data_h_w=(h, w),
            mask_h_w=(mask.shape[-2], mask.shape[-1]), inner_slices=ref.inner_slices, )

    @staticmethod
    def _create_shm(name, size):
        try:
            return shared_memory.SharedMemory(name=name, create=True, size=size)
        except FileExistsError:
            ex = shared_memory.SharedMemory(name=name)
            ex.close()
            ex.unlink()
            return shared_memory.SharedMemory(name=name, create=True, size=size)

    def verify_connection(self):
        # Force re-binding during verification
        _ = self.data_buf
        _ = self.mask_buf
        try:
            sig_name = f"{self.prefix}_sig"
            temp_sig = shared_memory.SharedMemory(name=sig_name)
            content = bytes(temp_sig.buf[:8]).decode('ascii').strip('\x00')
            match = content == self.session_id
            temp_sig.close()
            return match
        except:
            return False

    def acquire(self, timeout=WKR_TIMEOUT, block: bool = True):
        """Modified to support non-blocking calls for eviction logic."""
        # print(f"  <<< SHM GET <<<")

        if block:
            res = self._available_slots.get(block=True, timeout=timeout)
            return res
        else:
            # Raises queue.Empty if nothing is there
            return self._available_slots.get_nowait()

    def release(self, i):
        """Returns a slot to the 'True Free' pool."""
        self._available_slots.put(i)  # print(f">>> SHM PUT >>>")

    def cleanup(self):
        for s in [self._d_shm, self._m_shm, self._sig_shm]:
            s.close()
            s.unlink()

class SlotRegistry:
    def __init__(
            self, pool_map: Dict[DriverKey, SharedMemoryPool], context_id: str,
            static_count: int
    ):
        self.is_cold = True
        self.pool_map = pool_map
        self.context_id = context_id
        self.is_warm_hit_detected = False
        self.hits = 0
        self.misses = 0
        self.transit_demands = 0

        self.static_cache = {k: {} for k in pool_map.keys()}
        self.ref_counts = {k: collections.defaultdict(int) for k in pool_map.keys()}

        self.static_available = {}
        self.transit_indices = {k: set() for k in pool_map.keys()}

        for key, pool in pool_map.items():
            # Use the explicit count passed from the Orchestrator
            # (Ensuring we don't exceed actual pool capacity)
            actual_static = min(static_count, pool.slots - 1)

            # Drain all indices to partition them
            all_indices = [pool.acquire(block=False) for _ in range(pool.slots)]

            # Partition
            self.static_available[key] = sorted(all_indices[:actual_static])
            transits = all_indices[actual_static:]

            # Put transit indices back into the pool's shared queue
            for idx in transits:
                self.transit_indices[key].add(idx)
                pool.release(idx)

    def start_session(self):
        """Called by Orchestrator at the start of every job."""
        self.hits = 0
        self.misses = 0
        self.transit_demands = 0
        self.is_warm_hit_detected = False

    def get_or_allocate(self, key: DriverKey, window: Window) -> Tuple[int, bool]:
        """
        Coordinates slot assignment using a stable spatial key.
        Returns (slot_id, is_cached).
        """
        # Stable identity based on global pixel offsets
        win_key = (window.col_off, window.row_off, window.width, window.height)

        # 1. STATIC CACHE HIT
        if win_key in self.static_cache[key]:
            slot_id = self.static_cache[key][win_key]
            self.ref_counts[key][slot_id] += 1
            self.hits += 1
            self.is_warm_hit_detected = True
            return slot_id, True

        # 2. STATIC CACHE MISS: TRY PRIMING STATIC ZONE
        self.misses += 1
        if self.static_available[key]:
            slot_id = self.static_available[key].pop(0)
            self.static_cache[key][win_key] = slot_id
            self.ref_counts[key][slot_id] = 1
            return slot_id, False

        # 3. STATIC ZONE FULL: USE TRANSIT SLOT (SCRATCHPAD)
        # This acts as a cache miss every time for blocks beyond the static capacity.
        self.transit_demands += 1

        pool = self.pool_map[key]
        try:
            # Transit slots are acquired from the shared Pool Queue
            slot_id = pool.acquire(block=False)
            self.ref_counts[key][slot_id] = 1
            return slot_id, False
        except:
            # If both zones are exhausted, we must wait or fail
            raise RuntimeError(
                f"Memory Exhausted for {key}. "
                f"Static zone is frozen and Transit zone is fully utilized by active workers. "
                f"Increase total slots or reduce MAX_IN_FLIGHT."
            )

    def release(self, key: DriverKey, slot_id: int):
        """
        Indicates a block is no longer needed by a specific tile.
        Static slots remain pinned; Transit slots return to the pool.
        """
        if self.ref_counts[key][slot_id] <= 0:
            return

        self.ref_counts[key][slot_id] -= 1

        # Only return to pool if it's a Transit slot and no one else is using it
        if self.ref_counts[key][slot_id] == 0:
            if slot_id in self.transit_indices[key]:
                self.pool_map[key].release(slot_id)
            else:
                # It is a Static slot. We do not release it to the pool;
                # it stays in self.static_cache forever for this context.
                pass

    def get_telemetry(self) -> dict:
        """Returns raw counters and physical memory usage for the Orchestrator."""
        total_bytes = 0
        for pool in self.pool_map.values():
            # Sum up data and mask buffers
            total_bytes += pool._d_shm.size + pool._m_shm.size

        # Calculate usage of the Static partition from the first driver pool
        first_key = next(iter(self.static_cache))
        used = len(self.static_cache[first_key])
        total = used + len(self.static_available[first_key])

        return {
            "mb_allocated": total_bytes / (1024 * 1024),
            "slots_used": used,
            "slots_total": total,
            "hits": self.hits,
            "misses": self.misses,
            "transit_demands": self.transit_demands, # New
            "is_cold": not self.is_warm_hit_detected
        }

    def reset_context(self, new_context_id: str):
        self.context_id = new_context_id
        self.is_cold = True
        self.hits = 0
        self.misses = 0

        # Clear all mappings but keep the slot_ids
        for key in self.static_cache.keys():
            # Move all currently mapped static slots back to 'available'
            mapped_ids = list(self.static_cache[key].values())
            self.static_available[key].extend(mapped_ids)
            self.static_available[key].sort()
            self.static_cache[key].clear()
