
from dataclasses import dataclass
from typing import Dict, Any

import numpy as np

# noise_registry.py

@dataclass(frozen=True, slots=True)
class NoiseProvider:
    """
    Standardized provider of procedural noise.
    CONTRACT: window_noise() always returns (H, W, 1) float32.
    """
    tile: np.ndarray

    def __post_init__(self) -> None:
        if self.tile.ndim != 2:
            raise ValueError("NoiseProvider.tile must be 2D")
        if self.tile.dtype != np.float32:
            raise ValueError("NoiseProvider.tile must be float32")

    @property
    def h(self) -> int:
        return self.tile.shape[0]

    @property
    def w(self) -> int:
        return self.tile.shape[1]

    def window_noise(self, window, *, row_off=0, col_off=0, scale_override=None) -> np.ndarray:
        h = int(window.height)
        w = int(window.width)
        r0 = int(window.row_off) + int(row_off)
        c0 = int(window.col_off) + int(col_off)

        # If we want larger or smaller waves without changing the noise tile
        s = scale_override if scale_override is not None else 1.0

        rows = (np.arange(r0, r0 + h) * s % self.h).astype(np.int64, copy=False)
        cols = (np.arange(c0, c0 + w) * s % self.w).astype(np.int64, copy=False)
        return self.tile[np.ix_(rows, cols)][..., np.newaxis]


class NoiseRegistry:
    """
    Engine responsible for procedural resource generation.
    """
    def __init__(self, cfg, profiles: Dict[str, Any]):
        self.providers: Dict[str, NoiseProvider] = {}
        self.profiles = profiles

        base_seed = cfg.get("seed", 42)

        for noise_id, profile in profiles.items():
            # Use a high-quality 2k tile as the basis for all lookups
            tile = generate_fbm_noise_tile(
                shape=(2048, 2048),
                sigmas=profile.sigmas,
                weights=profile.weights,
                stretch=profile.stretch,
                seed=base_seed + profile.seed_offset
            )
            self.providers[noise_id] = NoiseProvider(tile)

    def keys(self):
        return self.providers.keys()

    def get(self, noise_id: str) -> NoiseProvider:
        provider = self.providers.get(noise_id)
        if provider is None:
            raise KeyError(f"Noise ID '{noise_id}' not found in Registry.")
        return provider


def generate_fbm_noise_tile(
        shape: tuple[int, int], *, sigmas: tuple[float, ...] = (1.5, 4.0, 10.0),
        weights: tuple[float, ...] = (0.4, 0.3, 0.3), stretch: tuple[float, float] = (1.0, 1.0),
        # (Y_mult, X_mult)
        seed: int = 42, ) -> np.ndarray:
    from scipy.ndimage import gaussian_filter
    rng = np.random.default_rng(seed)
    out = np.zeros(shape, dtype="float32")

    for sigma, w in zip(sigmas, weights):
        n = rng.uniform(-0.5, 0.5, shape).astype("float32")

        # Use a tuple for sigma to allow stretching
        # e.g. sigma=(1.5 * 1.0, 1.5 * 4.0)
        s_y = sigma * stretch[0]
        s_x = sigma * stretch[1]

        n = gaussian_filter(n, sigma=(s_y, s_x), mode="wrap")
        out += float(w) * n

    out -= float(out.mean())
    mx = float(np.max(np.abs(out))) or 1.0
    return (out / (2.0 * mx)).astype("float32", copy=False)

