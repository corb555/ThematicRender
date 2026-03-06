from dataclasses import dataclass
from typing import Optional, Tuple, Dict

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


@dataclass(frozen=True, slots=True)
class DriverBlockRef:
    slot_id: int
    value_shape: Tuple[int, ...]
    value_dtype: str
    valid_shape: Tuple[int, ...]
    valid_dtype: str
    value_hw: Tuple[int, int]
    valid_hw: Tuple[int, int]
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


@dataclass(frozen=True, slots=True)
class ResultPacket:
    """Worker -> Writer (IPC-friendly).
    """
    seq: int
    window_rect: WindowRect
    refs: Dict[DriverKey, DriverBlockRef]
    img_block: Optional[np.ndarray] = None
    out_ref: Optional[DriverBlockRef] = None


@dataclass(frozen=True, slots=True)
class PoolSpec:
    value_shape: Tuple[int, ...]
    value_dtype: np.dtype
    valid_shape: Tuple[int, ...]
    valid_dtype: np.dtype


class BlockPool:
    """

    """

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

    def write(
            self, slot_id: int, *, value: np.ndarray, valid: np.ndarray,
            inner_slices: Slice2D | None, pad_value: float | int = 0,
            pad_valid: float = 0.0, ) -> DriverBlockRef:
        """Copy arrays into fixed-size slot, padding as needed."""
        slot_val = self._value[slot_id]
        slot_vld = self._valid[slot_id]

        # Fill padding first (outside-map semantics)
        slot_val[...] = np.asarray(pad_value, dtype=slot_val.dtype)
        slot_vld[...] = np.asarray(pad_valid, dtype=slot_vld.dtype)

        vh, vw = value.shape[:2]
        if vh > slot_val.shape[0] or vw > slot_val.shape[1]:
            raise ValueError(f"Block Pool write. invalid vh, hw.  {vh} {vw}")
        slot_val[:vh, :vw] = value

        ah, aw = valid.shape[:2]
        if ah > slot_vld.shape[0] or aw > slot_vld.shape[1]:
            raise ValueError(f"Block pool write valid {valid.shape} exceeds slot {slot_vld.shape}")
        slot_vld[:ah, :aw, ...] = valid

        return DriverBlockRef(
            slot_id=slot_id, value_shape=slot_val.shape, value_dtype=str(slot_val.dtype),
            valid_shape=slot_vld.shape, valid_dtype=str(slot_vld.dtype), value_hw=(vh, vw),
            valid_hw=(ah, aw), inner_slices=inner_slices, )

    def view(self, ref: DriverBlockRef) -> DriverBlockView:
        """Returns a view of a SINGLE slot"""
        return DriverBlockView(
            slot_id=ref.slot_id, value=self._value[ref.slot_id],  # <--- MUST HAVE [ref.slot_id]
            valid=self._valid[ref.slot_id],  # <--- MUST HAVE [ref.slot_id]
            value_hw=ref.value_hw, valid_hw=ref.valid_hw, inner_slices=ref.inner_slices, )


from multiprocessing import shared_memory


class SharedMemoryPool:
    """
    A multiprocess-safe ring buffer using SharedMemory.
    Backs NumPy arrays with mmap'd memory for zero-copy transfer.
    """

    def __init__(self, spec: PoolSpec, slots: int, prefix: str):
        self.spec = spec
        self.slots = slots
        self._free = list(range(slots))

        # We create two distinct blocks per pool: one for values, one for validity
        self._val_shm = shared_memory.SharedMemory(
            create=True, size=slots * np.prod(spec.value_shape) * spec.value_dtype.itemsize,
            name=f"{prefix}_val"
        )
        self._vld_shm = shared_memory.SharedMemory(
            create=True, size=slots * np.prod(spec.valid_shape) * spec.valid_dtype.itemsize,
            name=f"{prefix}_vld"
        )

        # Wrap SHM in NumPy views
        self._value = np.ndarray(
            (slots, *spec.value_shape), dtype=spec.value_dtype, buffer=self._val_shm.buf
        )
        self._valid = np.ndarray(
            (slots, *spec.valid_shape), dtype=spec.valid_dtype, buffer=self._vld_shm.buf
        )

    def acquire(self) -> int:
        if not self._free: return -1  # Signal exhaustion
        return self._free.pop()

    def release(self, slot_id: int) -> None:
        self._free.append(slot_id)

    def write(
            self, slot_id: int, *, value: np.ndarray, valid: np.ndarray,
            inner_slices: Slice2D | None, pad_value=0, pad_valid=0.0
    ) -> DriverBlockRef:
        # (Same logic as your previous BlockPool.write, but writing into self._value[slot_id])
        # ... copy logic ...
        return DriverBlockRef(
            slot_id=slot_id, value_shape=self.spec.value_shape,
            value_dtype=str(self.spec.value_dtype), valid_shape=self.spec.valid_shape,
            valid_dtype=str(self.spec.valid_dtype), value_hw=value.shape[:2],
            valid_hw=valid.shape[:2], inner_slices=inner_slices
        )

    def view(self, ref: DriverBlockRef) -> DriverBlockView:
        return DriverBlockView(
            slot_id=ref.slot_id, value=self._value[ref.slot_id], valid=self._valid[ref.slot_id],
            value_hw=ref.value_hw, valid_hw=ref.valid_hw, inner_slices=ref.inner_slices
        )

    def close(self):
        self._val_shm.close()
        self._vld_shm.close()
        self._val_shm.unlink()
        self._vld_shm.unlink()
