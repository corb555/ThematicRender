from time import perf_counter
from typing import Optional

import numpy as np
from rasterio.windows import Window

from ThematicRender.ipc_packets import WindowRect

ERR_PREFIX = "❌ Error: Blend Pipeline - "

# Output tiling defaults
DEFAULT_BLOCK_SIZE = 256
ALPHA_DENOM = 255.0


def _validate_factor(
        factor: Optional[np.ndarray], context: str, spec,
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

import pickle

from typing import Any


def dot_get(obj: Any, path: str, default: Any = None) -> Any:
    """
    Retrieves a nested value from a dictionary or object using a dot-separated path.
    Example: dot_get(cfg, "drivers.water.max_opacity")
    """
    if obj is None:
        return default

    # 1. If we were passed the RenderConfig object itself,
    # start the search inside its raw_defs dictionary.
    current = obj.raw_defs if hasattr(obj, 'raw_defs') else obj

    # 2. Split the path (e.g., "drivers.water.max_opacity" -> ["drivers", "water", "max_opacity"])
    keys = path.split(".")

    for key in keys:
        if isinstance(current, dict):
            # Move one level deeper into the dictionary
            current = current.get(key)
        elif hasattr(current, key):
            # Handle cases where it might be a nested dataclass/object
            current = getattr(current, key)
        else:
            return default

        # If at any point we hit a dead end, return the default
        if current is None:
            return default

    return current


def assert_pickle(obj: Any, name: str) -> None:
    """
    Attempt to pickle an object into a memory buffer to ensure it
    crosses process boundaries safely.
    """
    # Check size:
    find_the_fat(obj, name)

    try:
        # Use a high protocol for performance, matching what SHM uses
        pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception as exc:
        print(f"❌ Pickle Error for {name}")
        debug_pickle(obj)


def debug_pickle(obj, path=""):
    """Recursively checks which attribute is failing to pickle."""
    for attr in dir(obj):
        if attr.startswith('__'): continue
        val = getattr(obj, attr)
        try:
            pickle.dumps(val)
        except Exception:
            print(f"❌ Failed at: {path}.{attr} (Type: {type(val)})")
            # Recurse into the failing object
            debug_pickle(val, f"{path}.{attr}")


def find_the_fat(obj, name="root"):
    """Recursively finds which attribute is larger than 1MB."""
    p_size = len(pickle.dumps(obj))
    if p_size > 1_000_000:
        print(f"📦 {name} is {p_size:,} bytes (Type: {type(obj)})")

        # If it's a dataclass with slots
        if hasattr(obj, "__slots__"):
            for s in obj.__slots__:
                find_the_fat(getattr(obj, s), f"{name}.{s}")
        # If it's a standard object
        elif hasattr(obj, "__dict__"):
            for k, v in obj.__dict__.items():
                find_the_fat(v, f"{name}.{k}")
        # If it's a dict
        elif isinstance(obj, dict):
            for k, v in obj.items():
                find_the_fat(v, f"{name}.{k}")


def window_from_rect(r: WindowRect) -> Window:
    col, row, w, h = r
    return Window(col, row, w, h)


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

    @staticmethod
    def bold(txt):
        return f"**{txt}**"

    @staticmethod
    def italic(txt):
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

    @staticmethod
    def format_dict(d: dict) -> str:
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
    print_once(
        tag, f"{tag} shape={a.shape} min={float(a.min()):.4f} max={float(a.max()):.4f} mean="
             f"{float(a.mean()):.4f}"
    )


def reset_print_once():
    """Call this at the start of process_rasters if you want a fresh log per run."""
    _SEEN_MSGS.clear()
