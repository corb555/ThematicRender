from time import perf_counter
from typing import Optional

import numpy as np

from ThematicRender.settings import _BlendSpec

ERR_PREFIX = "❌ Error: Blend Pipeline - "

# Output tiling defaults
DEFAULT_BLOCK_SIZE = 256
ALPHA_DENOM = 255.0
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


def _validate_factor(
        factor: Optional[np.ndarray], context: str, spec: "_BlendSpec",
        factors: Optional[dict[str, np.ndarray]] = None, ) -> np.ndarray:
    if factor is not None:
        return factor

    available = ""
    if factors is not None:
        keys = sorted(factors.keys())
        available = f" Available factors: {keys}."

    raise ValueError(
        f"{ERR_PREFIX} {context} - factor '{spec.factor_nm}' is None "
        f"(action='{spec.comp_op}')."
        f"{available}"
    )


def apply_factor_jitter(
        t: np.ndarray, noise: np.ndarray, amplitude: float, atten_power: float = 1.0
) -> np.ndarray:
    """
    Applies unbiased noise to a 0..1 factor.

    Args:
        t: The base factor (0..1).
        noise: Seamless noise block (-0.5 to 0.5).
        amplitude: Strength of the jitter.
        atten_power: 1.0 = standard parabolic envelope (clean edges).
                     0.0 = flat addition (noisy edges/islands).
    """
    if amplitude <= 0:
        return t

    # Envelope: 4*t*(1-t) creates a curve that is 0 at the edges and 1.0 at 0.5
    # Raising to atten_power allows "spreading" the noise wider or tighter.
    envelope = np.power(4.0 * t * (1.0 - t), atten_power)

    return np.clip(t + (noise * amplitude * envelope), 0.0, 1.0)

def _onoff(v: bool) -> str:
    """Render a boolean as a compact CLI indicator.

    Args:
        v: Value to render.

    Returns:
        `"✅"` if True.
    """
    return "✅" if v else " - "


DTYPE_ALIASES = {
    "uint8": np.uint8, "ubyte": np.uint8, "byte": np.uint8, "int16": np.int16, "uint16": np.uint16,
    "int32": np.int32, "uint32": np.uint32, "float32": np.float32, "float": np.float32,
    "float64": np.float64, "double": np.float64,
}


class TimerStats:
    def __init__(self):
        self.stats = {}
        self.start_time = 0
        self.current_block = None

    def start(self, name):
        self.start_time = perf_counter()
        self.current_block = name

    def end(self):
        elapsed = perf_counter() - self.start_time
        self.stats[self.current_block] = self.stats.get(self.current_block, 0) + elapsed

    def summary(self):
        for name, total_time in self.stats.items():
            print(f"{name}: {total_time:.2f} seconds")


class GenMarkdown:
    def __init__(self):
        self.lines = []

    def header(self, txt, level=1):
        self.lines.append(f"\n{'#' * level} {txt} \n")

    def bold(self, txt):
        return f"**{txt}**"

    def italic(self, txt):
        return f"_{txt}_"

    def text(self, txt):
        self.lines.append(f"{txt} \n")

    def tbl_hdr(self, *cols):
        self.lines.append("| " + " | ".join(cols) + " |")
        self.lines.append("| " + " | ".join(["---"] * len(cols)) + " |")

    def tbl_row(self, *cols):
        # Clean up None values and ensure string conversion
        row = [str(c) if c is not None else "" for c in cols]
        self.lines.append("| " + " | ".join(row) + " |")

    def bullet(self, txt):
        self.lines.append(f"* {txt} ")

    def format_dict(self, d: dict) -> str:
        """Converts a dictionary to a compact string for table cells."""
        if not d: return ""
        return "<br>".join([f"{k}: {v}" for k, v in d.items()])

    def render(self):
        return "\n".join(self.lines)


# Globally track seen message IDs
_SEEN_MSGS = set()

def print_once(msg_id: str, *args, **kwargs):
    """Prints a message only the first time a specific msg_id is encountered."""
    if msg_id not in _SEEN_MSGS:
        print(*args, **kwargs)
        _SEEN_MSGS.add(msg_id)

def stats_once(tag, a):
    print_once(tag, f"{tag} shape={a.shape} min={float(a.min()):.4f} max={float(a.max()):.4f} mean={float(a.mean()):.4f}")


def reset_print_once():
    """Call this at the start of process_rasters if you want a fresh log per run."""
    _SEEN_MSGS.clear()