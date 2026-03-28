import numpy as np

EPS = 1e-9


def lerp(a, b, t):
    """Safe lerp that prevents (H,W) + (H,W,1) -> (H,W,H) broadcasting."""
    # If t is 3D and b is 2D, expand b
    if hasattr(t, 'ndim') and t.ndim == 3:
        if hasattr(b, 'ndim') and b.ndim == 2:
            b = b[..., np.newaxis]
        if hasattr(a, 'ndim') and a.ndim == 2:
            a = a[..., np.newaxis]

    return a + t * (b - a)


def normalize_step(val: np.ndarray, min_v: float, max_v: float) -> np.ndarray:
    denom = max_v - min_v
    denom = denom if abs(denom) > EPS else 1.0
    fac = (val - min_v) / denom
    return np.clip(fac, 0.0, 1.0)[..., np.newaxis]


def smoothstep(t: np.ndarray) -> np.ndarray:
    t = np.clip(t, 0.0, 1.0)
    return t * t * (3.0 - 2.0 * t)
