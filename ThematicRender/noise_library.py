from dataclasses import dataclass, field
from multiprocessing.shared_memory import SharedMemory
from typing import Dict, Any, Optional, Tuple

import numpy as np


@dataclass(slots=True)
class NoiseProvider:
    """
    Standardized provider of procedural noise.
    The 'tile' is stored in Shared Memory to avoid massive I/O during IPC.
    """
    shm_name: str
    shape: Tuple[int, int]
    dtype: np.dtype

    # Internal handles (Not Pickled)
    _shm: Optional[SharedMemory] = field(default=None, init=False, repr=False)
    _tile: Optional[np.ndarray] = field(default=None, init=False, repr=False)

    def __getstate__(self):
        """Exclude live memory handles from the pickle stream."""
        return {
            "shm_name": self.shm_name, "shape": self.shape, "dtype": self.dtype,
        }

    def __setstate__(self, state):
        """Restore metadata; _shm and _tile remain None until attach() is called."""
        self.shm_name = state["shm_name"]
        self.shape = state["shape"]
        self.dtype = state["dtype"]
        self._shm = None
        self._tile = None

    def attach_shm(self) -> None:
        """WORKER SIDE: Map the shared memory into local process space."""
        if self._tile is not None:
            return
        self._shm = SharedMemory(name=self.shm_name)
        self._tile = np.ndarray(self.shape, dtype=self.dtype, buffer=self._shm.buf)

    def close(self) -> None:
        """WORKER SIDE: Close the local handle to shared memory."""
        if self._shm:
            self._shm.close()
            self._shm = None
            self._tile = None

    def unlink(self) -> None:
        """ORCHESTRATOR SIDE: Physically delete the shared memory segment."""
        self.close()
        try:
            temp_shm = SharedMemory(name=self.shm_name)
            temp_shm.close()
            temp_shm.unlink()
        except FileNotFoundError:
            pass

    def cleanup(self, unlink: bool = False):
        """Close local handle and optionally delete the SHM segment."""
        if self._shm is not None:
            self._shm.close()
            if unlink:
                try:
                    self._shm.unlink()
                except (FileNotFoundError, PermissionError):
                    pass
            self._shm = None
            self._tile = None

    @property
    def tile(self) -> np.ndarray:
        if self._tile is None:
            raise RuntimeError(f"NoiseProvider '{self.shm_name}' accessed before attach_shm().")
        return self._tile

    @property
    def h(self) -> int:
        return self.shape[0]

    @property
    def w(self) -> int:
        return self.shape[1]

    def window_noise(self, window, *, row_off=0, col_off=0, scale_override=None) -> np.ndarray:
        """Hot path: Stays exactly as efficient as before."""
        h, w = int(window.height), int(window.width)
        r0, c0 = int(window.row_off) + int(row_off), int(window.col_off) + int(col_off)

        s = scale_override if scale_override is not None else 1.0
        rows = (np.arange(r0, r0 + h) * s % self.h).astype(np.int64, copy=False)
        cols = (np.arange(c0, c0 + w) * s % self.w).astype(np.int64, copy=False)

        noise1 = self.tile[np.ix_(rows, cols)]

        # Pattern breaker logic
        rows2 = ((np.arange(r0, r0 + h) + 503) * (s * 0.97) % self.h).astype(np.int64)
        cols2 = ((np.arange(c0, c0 + w) + 503) * (s * 0.97) % self.w).astype(np.int64)
        noise2 = self.tile[np.ix_(rows2, cols2)]

        return (noise1 * 0.7 + noise2 * 0.3)[..., np.newaxis]


class NoiseLibrary:
    def __init__(self, cfg, profiles: Dict[str, Any], create_shm: bool = False):
        self.providers: Dict[str, NoiseProvider] = {}
        self.profiles = profiles

        # Use a consistent seed from the global config
        base_seed = cfg.get_global("seed", 42)

        for noise_id, profile in profiles.items():
            shm_name = f"tr_noise_{noise_id}"

            # Use a standard tile size for all noise (e.g., 2048x2048)
            noise_shape = (2048, 2048)

            if create_shm:
                # 1. Generate the heavy noise data exactly once
                tile_data = generate_fbm_noise_tile(
                    shape=noise_shape, sigmas=profile.sigmas, weights=profile.weights,
                    stretch=profile.stretch, seed=base_seed + profile.seed_offset
                )

                # 2. Cleanup and Allocate Shared Memory
                try:
                    old = SharedMemory(name=shm_name)
                    old.close()
                    old.unlink()
                except FileNotFoundError:
                    pass

                shm = SharedMemory(create=True, size=tile_data.nbytes, name=shm_name)

                # 3. Copy pixels to SHM
                shm_view = np.ndarray(tile_data.shape, dtype=tile_data.dtype, buffer=shm.buf)
                shm_view[:] = tile_data[:]
                shm.close()

            # --- BOTH MODES: Register the Provider ---
            # Workers just create the handle to attach to later
            self.providers[noise_id] = NoiseProvider(
                shm_name=shm_name, shape=noise_shape, dtype=np.float32
            )

    def attach_providers_shm(self):
        """Called by Workers during JIT Context Switch."""
        for provider in self.providers.values():
            provider.attach_shm()

    def detach_providers_shm(self):
        """Called by Workers during Job Finalization."""
        for provider in self.providers.values():
            provider.close()

    def get(self, noise_id: str) -> NoiseProvider:
        return self.providers[noise_id]

    def cleanup(self, unlink: bool = False):
        for provider in self.providers.values():
            provider.cleanup(unlink=unlink)


def generate_fbm_noise_tile(
        shape: tuple[int, int], *, sigmas: tuple[float, ...] = (1.5, 4.0, 10.0),
        weights: tuple[float, ...] = (0.4, 0.3, 0.3), stretch: tuple[float, float] = (1.0, 1.0),
        seed: int = 42
) -> np.ndarray:
    from scipy.ndimage import gaussian_filter
    rng = np.random.default_rng(seed)
    out = np.zeros(shape, dtype="float32")

    for sigma, w in zip(sigmas, weights):
        if w <= 0: continue  # Optimization

        # 1. Generate unique noise for this octave
        n = rng.uniform(-0.5, 0.5, shape).astype("float32")

        # 2. Apply the blur
        s_y, s_x = sigma * stretch[0], sigma * stretch[1]
        n = gaussian_filter(n, sigma=(s_y, s_x), mode="wrap")

        # 3.  Per-Octave Normalization
        # We force this octave back to a 0.0-1.0 range so weights are meaningful
        n_min, n_max = n.min(), n.max()
        if n_max - n_min > 1e-6:
            n = (n - n_min) / (n_max - n_min)

        # 4. Add to composite based on weight
        out += float(w) * n

    # 5. Final Global Normalization
    mn, mx = out.min(), out.max()
    if mx - mn > 1e-6:
        out = (out - mn) / (mx - mn)
    return out.astype("float32")
