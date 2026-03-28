from dataclasses import dataclass, field, asdict
from typing import Dict, Any, Optional


@dataclass(slots=True)
class LogicSpec:
    # Common Parameters (Found in almost all drivers)
    start: float = field(
        default=0.0, metadata={"min": 0, "max": 8000, "step": 1.0, "label": "Start Range"}
    )
    full: float = field(
        default=100.0, metadata={"min": 0, "max": 8000, "step": 1.0, "label": "Full Range"}
    )
    noise_amp: float = field(
        default=0.0, metadata={
            "min": 0, "max": 2.0, "step": 0.01, "label": "Noise Amplitude"
        }
    )
    noise_atten_power: float = field(
        default=1.0, metadata={
            "min": 0, "max": 4.0, "step": 0.1, "label": "Noise Attenuation"
        }
    )
    contrast: float = field(
        default=1.0, metadata={"min": 0.1, "max": 5.0, "step": 0.1, "label": "Contrast"}
    )
    max_opacity: float = field(
        default=1.0, metadata={
            "min": 0.0, "max": 1.0, "step": 0.01, "label": "Max Opacity"
        }
    )
    sensitivity: float = field(
        default=1.0, metadata={
            "min": 0.0, "max": 5.0, "step": 0.1, "label": "Sensitivity"
        }
    )
    blur_px: float = field(
        default=0.0, metadata={"min": 0, "max": 128, "step": 1.0, "label": "Blur (Pixels)"}
    )

    # Specialized Parameters (Optional, used by specific drivers like Water or Hillshade)
    glint_scale: Optional[float] = field(default=None, metadata={"min": 1, "max": 20, "step": 0.5})
    glint_floor: Optional[float] = field(default=None, metadata={"min": 0, "max": 1, "step": 0.05})
    ripple_scale: Optional[float] = field(
        default=None, metadata={"min": 0.1, "max": 10, "step": 0.1}
    )
    strength: Optional[float] = field(default=None, metadata={"min": 0, "max": 2, "step": 0.1})

    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        """Creates a spec from YAML data, ignoring extra keys."""
        # Filter keys to only those defined in the dataclass
        valid_keys = {f.name for f in cls.__dataclass_fields__.values()}
        filtered_data = {k: v for k, v in data.items() if k in valid_keys}
        return cls(**filtered_data)

    def to_dict(self):
        """Returns a clean dict for YAML export, removing None values."""
        return {k: v for k, v in asdict(self).items() if v is not None}
