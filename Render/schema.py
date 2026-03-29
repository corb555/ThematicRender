from enum import StrEnum
from typing import Any


def _allowed(enum_cls: type[StrEnum]) -> list[str]:
    return [e.value for e in enum_cls]


RENDER_SCHEMA: dict[str, Any] = {
    "version": {"type": "integer", "required": False, "default": 1},
    "anchor": {"type": "string", "required": True},
    "seed": {"type": "integer", "required": False, "default": 1},
    "override_factor": {"type": "string", "required": False},
    "debug_factors": {"type": "string", "required": False},
    "files": {"type": "dict", "required": True},
    "prefixed_files": {"type": "dict", "required": False},
    "theme_smoothing_specs": {"type": "dict", "required": False},
    "driver_specs": {"type": "dict", "required": False},
    "factor_specs": {"type": "dict", "required": False},
    "factors": {"type": "dict", "required": False},
    "noise_profiles": {"type": "dict", "required": False},
    "pipeline": {"type": "list", "required": True},
    "surface_modifier_specs": {"type": "dict", "required": False},
    "surfaces": {"type": "dict", "required": False},
    "theme_render": {"type": "dict", "required": False},

    # ------------------------------------------------------------------
    # Driver parameter blocks
    # ------------------------------------------------------------------
    "drivers": {
        "type": "dict", "required": True, "allow_unknown": True, "schema": {
            "precip": {
                "type": "dict", "required": False, "default": {}, "schema": {
                    "start": {"type": "float", "default": 180.0},
                    "full": {"type": "float", "default": 750.0}, "noise_amp": {"type": "float"},
                    "noise_atten_power": {"type": "float", "default": 1.0},
                    "contrast": {"type": "float", "default": 1.0, "min": 0.1},
                },
            },

            "forest": {
                "type": "dict", "required": False, "default": {}, "schema": {
                    "start": {"type": "float", "default": 0.0},
                    "full": {"type": "float", "default": 86.0}, "noise_amp": {"type": "float"},
                    "noise_atten_power": {"type": "float", "default": 1.0},
                    "contrast": {"type": "float", "default": 1.0, "min": 0.1},
                },
            },

            "theme_alpha": {
                "type": "dict", "required": False, "default": {}, "schema": {
                    "strength": {"type": "float", "default": 1.0},
                },
            },

            "lith": {
                "type": "dict", "required": False, "default": {}, "schema": {
                    "start": {"type": "float", "default": 0.0},
                    "full": {"type": "float", "default": 255.0},
                    "noise_amp": {"type": "float", "default": 0.0},
                    "noise_atten_power": {"type": "float"},
                },
            },

            "snow": {
                "type": "dict", "required": False, "default": {}, "schema": {
                    "snowline": {"type": "float", "default": 2800.0},
                    "ramp": {"type": "float", "default": 150.0},
                    "dither_strength": {"type": "float", "default": 0.20}, "color": {
                        "type": "string", "default": "edeff0", "regex": r"^[0-9a-fA-F]{6}$",
                    },
                },
            },

            "hillshade": {
                "type": "dict", "required": False, "default": {}, "schema": {
                    "strength": {"type": "float", "default": 0.80, "min": 0.0, "max": 1.0},
                    "protect_shadows": {"type": "float", "default": 0.20, "min": 0.0, "max": 1.0},
                    "protect_highlights": {
                        "type": "float", "default": 0.10, "min": 0.0, "max": 1.0
                    }, "shadow_start": {"type": "float", "default": 0.00, "min": 0.0, "max": 1.0},
                    "shadow_end": {"type": "float", "default": 0.235, "min": 0.0, "max": 1.0},
                    "highlight_start": {"type": "float", "default": 0.86, "min": 0.0, "max": 1.0},
                    "highlight_end": {"type": "float", "default": 1.00, "min": 0.0, "max": 1.0},
                },
            },
        },
    },

    # ------------------------------------------------------------------
    # Output options (optional)
    # ------------------------------------------------------------------
    "output": {
        "type": "dict", "required": False, "default": {}, "schema": {
            "creation_options": {
                "type": "dict", "required": False, "default": {}, "valuesrules": {
                    "anyof": [{"type": "string"}, {"type": "integer"}, {"type": "float"},
                              {"type": "boolean"}, ]
                },
            },
        },
    },
}
