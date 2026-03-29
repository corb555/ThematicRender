from dataclasses import dataclass, field
from pathlib import Path
import time
from typing import Optional, Tuple

from rasterio.windows import Window

from Common.keys import DriverKey, RequiredResources
from Render.render_config import RenderConfig


@dataclass(slots=True)
class JobManifest:
    job_id: str
    render_cfg: "RenderConfig"
    resources: "RequiredResources"

    final_out_path: Path
    temp_out_path: Path

    profile: dict
    region_id: str
    envelope: Optional[Window]
    write_offset: Tuple[int, int]
    render_params: Tuple[float, float, float]
    driver_metadata: dict[DriverKey, dict[str, int]]


@dataclass(slots=True)
class JobControl:
    """Tracks runtime progress. Paths are delegated to the manifest."""
    manifest: Optional[JobManifest] = None
    total_tiles: int = 0
    tiles_written: int = 0
    start_time: float = field(default_factory=time.perf_counter)  # Record start instantly

    def clear_job(self):
        """Put the pipeline in an IDLE state."""
        self.manifest = None
        self.total_tiles = 0
        self.tiles_written = 0
        self.start_time = 0.0

    @property
    def busy(self) -> bool:
        return self.manifest is not None

    @property
    def job_id(self) -> str:
        return self.manifest.job_id if self.manifest else "-1"

    @property
    def temp_out_path(self) -> Optional[Path]:
        return self.manifest.temp_out_path if self.manifest else None

    @property
    def final_out_path(self) -> Optional[Path]:
        return self.manifest.final_out_path if self.manifest else None

    def mark_tile_written(self) -> bool:
        self.tiles_written += 1
        return self.tiles_written >= self.total_tiles

    @property
    def elapsed(self) -> float:
        """Return elapsed seconds since job start."""
        if self.start_time == 0:
            return 0.0
        return time.perf_counter() - self.start_time
