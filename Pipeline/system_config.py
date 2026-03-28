from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import (Any, Dict)

import numpy as np
from Common.keys import DriverKey, DriverRndrSpec, DriverHWSpec
from Render.utils import DTYPE_ALIASES


# system_config.py
@dataclass(slots=True)
class SystemConfig:
    defs: Dict[str, Any]
    driver_specs: Dict[DriverKey, DriverRndrSpec]

    @classmethod
    def load_engine_specs(cls, engine_config_path: Path) -> "SystemConfig":
        """
        ROUTINE 0: Cold Boot Loader.
        Parses engine.yml to define the physical capacity of the machine.
        This is used to initialize EngineResources and SHM pools.
        """
        import yaml
        context = "[SystemConfig] Load engine specs"
        print(context)
        with open(engine_config_path, 'r') as f:
            defs = yaml.safe_load(f)

        system = defs.get("system")
        try:
            # 1. Extract Hardware Capacity
            # system = defs.get("system")
            # This is the "Physical Pipe Diameter"
            system_max_halo = int(system.get("max_halo"))

            # 2. Parse Driver Hardware Registry
            # This defines the physical data types for SHM allocation
            driver_specs = {}
            dtype_map = {
                "float32": np.float32, "uint8": np.uint8, "int16": np.int16, "float64": np.float64
            }

            for dname, data in defs.get("driver_specs").items():
                # Note: We use string keys or dynamic enums here
                # to allow users to add new drivers without code changes.
                dkey = to_enum_sys(DriverKey, dname)

                driver_specs[dkey] = DriverHWSpec(
                    dtype=dtype_map.get(data["dtype"]),
                    # All drivers in SHM are allocated at the machine's max halo capacity
                    halo_px=system_max_halo, )

            # 3. Construct the "Machine" Config
            return cls(
                defs=defs, driver_specs=driver_specs
            )

        except MemoryError as e:
            print(f"❌ FATAL ENGINE BOOT ERROR: {context} -> {str(e)}")
            raise SystemExit(1)

    def get(self, path: str, default: Any = None) -> Any:
        """
        Access nested engine settings using dot notation.
        Example: cfg.get_setting("system.socket_path")
        """
        parts = path.split(".")
        val = self.defs
        for part in parts:
            if isinstance(val, dict):
                val = val.get(part)
            else:
                return default
        return val if val is not None else default


def _parse_dtype(v: Any, *, where: str) -> np.dtype:
    """Parse dtype from config values."""
    if v is None:
        raise ValueError(f"{where}: dtype is None")

    if isinstance(v, np.dtype):
        return v

    if isinstance(v, type) and issubclass(v, np.generic):
        return np.dtype(v)

    if isinstance(v, str):
        key = v.strip()
        if key in DTYPE_ALIASES:
            return np.dtype(DTYPE_ALIASES[key])
        raise ValueError(f"{where}: unknown dtype string '{v}'")

    raise ValueError(f"{where}: unsupported dtype {type(v).__name__}: {v!r}")


def to_enum_sys(key_cls, value):
    """Converts YAML strings to their corresponding Enum values with helpful hints."""
    if value is None:
        return None
    try:
        # Enums lookup by value
        return key_cls(value)
    except ValueError:
        # Extract all valid values from the Enum class
        valid_options = [item.value for item in key_cls]

        # Format the error message with the "Available" hint
        raise ValueError(
            f"❌ CONFIG ERROR: '{value}' is not a valid {key_cls.__name__}.\n"
            f"👉 Available: {', '.join(map(str, valid_options))}"
        ) from None  # 'from None' hides the original internal traceback for a cleaner CLI
