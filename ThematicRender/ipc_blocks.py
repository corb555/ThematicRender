from dataclasses import dataclass
import os
from typing import Optional, Tuple, Dict
import uuid

import numpy as np
from rasterio.windows import Window

from ThematicRender.keys import DriverKey

# ipc_blocks.py

Slice2D = Tuple[slice, slice]
WindowRect = Tuple[int, int, int, int]  # (col_off, row_off, width, height)


def rect_from_window(w: Window) -> WindowRect:
    return int(w.col_off), int(w.row_off), int(w.width), int(w.height)


def window_from_rect(r: WindowRect) -> Window:
    col, row, w, h = r
    return Window(col, row, w, h)


@dataclass(frozen=True)
class DriverBlockRef:
    slot_id: int
    value_hw: Tuple[int, int]
    inner_slices: Optional[Tuple[slice, slice]] = None

@dataclass(frozen=True, slots=True)
class DriverBlockView:
    slot_id: int
    value: np.ndarray
    valid: np.ndarray
    value_hw: Tuple[int, int]
    valid_hw: Tuple[int, int]
    inner_slices: Optional[Slice2D] = None


@dataclass(frozen=True, slots=True)
class WorkPacket:
    """Reader -> Worker (IPC-friendly)."""
    seq: int
    window_rect: WindowRect
    refs: Dict[DriverKey, DriverBlockRef]
    read_duration: float = 0.0  # Time spent in disk I/O -> SHM


@dataclass(frozen=True, slots=True)
class ResultPacket:
    seq: int
    window_rect: WindowRect
    refs: Dict[DriverKey, DriverBlockRef]
    img_block: Optional[np.ndarray] = None
    out_ref: Optional[DriverBlockRef] = None
    # Metrics
    read_duration: float = 0.0
    render_duration: float = 0.0
    write_duration: float = 0.0

@dataclass(frozen=True)
class PoolSpec:
    value_shape: tuple  # (Bands, H, W) e.g., (1, 384, 384)
    value_dtype: np.dtype
    valid_shape: tuple  # (Bands, H, W) e.g., (1, 384, 384)
    valid_dtype: np.dtype

class BlockPool:
    def __init__(self, spec: PoolSpec, slots: int) -> None:
        self.spec = spec
        self.slots = slots
        self._value = np.empty((slots, *spec.value_shape), dtype=spec.value_dtype)
        self._valid = np.empty((slots, *spec.valid_shape), dtype=spec.valid_dtype)
        self._free: list[int] = list(range(slots))

    def __getitem__(self, slot_id: int) -> 'DriverBlockView':
        """
        index into the _value and _valid arrays!
        Without the [slot_id], we are passing 384 tiles at once.
        """
        # meta retrieval logic (if using the metadata-in-SHM pattern)
        # or just simple slicing if in single-thread

        return DriverBlockView(
            slot_id=slot_id, value=self._value[slot_id], valid=self._valid[slot_id],
            # ... rest of metadata ...
        )

    def acquire(self) -> int:
        if not self._free:
            raise RuntimeError("BlockPool exhausted: no free slots.")
        return self._free.pop()

    def release(self, slot_id: int) -> None:
        if not (0 <= slot_id < self.slots):
            raise ValueError(f"slot_id out of range: {slot_id}")
        self._free.append(slot_id)

    def write(self, slot_id: int, value: np.ndarray, valid: np.ndarray,
              inner_slices: Optional[Tuple[slice, slice]] = None,
              pad_value: float = 0.0, pad_valid: float = 0.0) -> 'DriverBlockRef':
        """
        Coerces any input into the (Band, H, W) storage slot.
        Accepts pad arguments to match RasterManager signature.
        """
        # Standardize Value to 3D (B, H, W)
        v_data = value[np.newaxis, ...] if value.ndim == 2 else value
        # Standardize Valid to 3D (B, H, W)
        m_data = valid[np.newaxis, ...] if valid.ndim == 2 else valid

        vb, vh, vw = v_data.shape
        mb, mh, mw = m_data.shape

        # 4D Write: [Slot, Band, Row, Col]
        self._value[slot_id, :vb, :vh, :vw] = v_data
        self._valid[slot_id, :mb, :mh, :mw] = m_data

        return DriverBlockRef(
            slot_id=slot_id,
            value_hw=(vh, vw),
            inner_slices=inner_slices
        )

    def view(self, ref: 'DriverBlockRef'):
        """Returns a namedtuple/object with .value and .valid as (B, H, W)"""
        # Slice the 4D buffer into a 3D view for the worker
        v = self._value[ref.slot_id, :, :ref.value_hw[0], :ref.value_hw[1]]
        m = self._valid[ref.slot_id, :, :ref.value_hw[0], :ref.value_hw[1]]
        return type('View', (), {'value': v, 'valid': m})

from multiprocessing import shared_memory, Queue


def _standardize_shape(shape: tuple) -> tuple:
    """Forces any 2D or 3D shape into a 3D (Bands, H, W) tuple."""
    if len(shape) == 2:  # (H, W) -> (1, H, W)
        return (1, shape[0], shape[1])
    if len(shape) == 3:
        if shape[2] <= 4:  # (H, W, B) -> (B, H, W)
            return (shape[2], shape[0], shape[1])
    return shape # Already (B, H, W)


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
        self._v_shape = (slots, *_standardize_shape(spec.value_shape))
        self._m_shape = (slots, *_standardize_shape(spec.valid_shape))
        self._v_name = f"{prefix}_val"
        self._m_name = f"{prefix}_vld"

        # 3. Allocation (Main Process)
        v_size = int(np.prod(self._v_shape) * np.dtype(spec.value_dtype).itemsize)
        m_size = int(np.prod(self._m_shape) * np.dtype(spec.valid_dtype).itemsize)

        self._v_shm = self._create_shm(self._v_name, v_size)
        self._m_shm = self._create_shm(self._m_name, m_size)

        # 4. Local Cache for NumPy Views
        # These will be None when the object is unpickled in a worker process
        self._v_buf_local = None
        self._m_buf_local = None

        # Queue for slot management
        self._available_slots = Queue()
        for i in range(slots):
            self._available_slots.put(i)

    @property
    def value_buf(self) -> np.ndarray:
        """Process-safe access to the Value buffer."""
        if self._v_buf_local is None:
            # Re-attach to the SHM handle (which was pickled/unpickled)
            # and wrap it in a fresh numpy array for this process.
            self._v_buf_local = np.ndarray(
                self._v_shape,
                dtype=self.spec.value_dtype,
                buffer=self._v_shm.buf
            )
        return self._v_buf_local

    @property
    def valid_buf(self) -> np.ndarray:
        """Process-safe access to the Validity buffer."""
        if self._m_buf_local is None:
            self._m_buf_local = np.ndarray(
                self._m_shape,
                dtype=self.spec.valid_dtype,
                buffer=self._m_shm.buf
            )
        return self._m_buf_local

    def write(self, slot_id: int, value: np.ndarray, valid: np.ndarray, **kwargs) -> 'DriverBlockRef':
        # Coerce inputs...
        v_in = value[np.newaxis, ...] if value.ndim == 2 else value
        if v_in.ndim == 3 and v_in.shape[2] <= 4: v_in = v_in.transpose(2, 0, 1)
        m_in = valid[np.newaxis, ...] if valid.ndim == 2 else valid
        if m_in.ndim == 3 and m_in.shape[2] <= 4: m_in = m_in.transpose(2, 0, 1)

        vb, vh, vw = v_in.shape
        # USE THE PROPERTIES instead of internal attributes
        self.value_buf[slot_id, :vb, :vh, :vw] = v_in
        self.valid_buf[slot_id, :m_in.shape[0], :vh, :vw] = m_in

        return DriverBlockRef(slot_id=slot_id, value_hw=(vh, vw),
                              inner_slices=kwargs.get('inner_slices'))

    def view(self, ref: 'DriverBlockRef'):
        h, w = ref.value_hw
        # USE THE PROPERTIES
        return type('View', (), {
            'value': self.value_buf[ref.slot_id, :, :h, :w],
            'valid': self.valid_buf[ref.slot_id, :, :h, :w]
        })

    def _create_shm(self, name, size):
        try: return shared_memory.SharedMemory(name=name, create=True, size=size)
        except FileExistsError:
            ex = shared_memory.SharedMemory(name=name); ex.close(); ex.unlink()
            return shared_memory.SharedMemory(name=name, create=True, size=size)

    def verify_connection(self):
        # Force re-binding during verification
        _ = self.value_buf
        _ = self.valid_buf
        try:
            sig_name = f"{self.prefix}_sig"
            temp_sig = shared_memory.SharedMemory(name=sig_name)
            content = bytes(temp_sig.buf[:8]).decode('ascii').strip('\x00')
            match = content == self.session_id
            temp_sig.close()
            return match
        except: return False

    def acquire(self): return self._available_slots.get()
    def release(self, i): self._available_slots.put(i)

    def cleanup(self):
        for s in [self._v_shm, self._m_shm, self._sig_shm]:
            s.close(); s.unlink()
