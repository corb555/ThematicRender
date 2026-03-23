from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Set, Optional

import rasterio
import rasterio.windows

from ThematicRender.io_manager import IOManager
from ThematicRender.keys import DriverKey
from ThematicRender.render_config import RenderConfig
from ThematicRender.worker_context_base import (WorkerContextBase, )


@dataclass(slots=True)
class ReaderContext(WorkerContextBase):
    """Job settings for reader execution."""
    # Addtional params in WorkerContextBase
    job_id: str
    render_cfg: RenderConfig
    anchor_key: DriverKey
    source_paths: dict[DriverKey, Path]
    _io: Optional[IOManager] = field(default=None, init=False, repr=False)

    @property
    def io(self) -> IOManager:
        """Return the initialized local IO manager."""
        if self._io is None:
            raise RuntimeError("[READER] ReaderContext.io accessed before initialization.")
        return self._io

    def open_local_resources(self) -> None:
        """Open process-local dataset handles for this job."""
        self.close_local_resources()
        self._io = IOManager(self.render_cfg, self.source_paths, self.anchor_key)
        self._io.__enter__()

    def close_local_resources(self) -> None:
        """Close process-local dataset handles if open."""
        if self._io is not None:
            self._io.__exit__(None, None, None)
            self._io = None


@dataclass(slots=True)
class WorkerContext:
    """ Job settings for worker execution."""
    job_id: str
    render_cfg: RenderConfig
    themes: Any
    compositor: Any
    pipeline: Any
    anchor_key: DriverKey
    surface_inputs: Set[Any]
    resources: Any
    noise_registry: Any

    def open_local_resources(self):
        # Attach the noise library to shared memory
        self.noise_registry.attach_providers_shm()

    def close_local_resources(self) -> None:
        return


@dataclass(slots=True)
class WriterContext(WorkerContextBase):
    """Job settings for writer execution."""
    job_id: str
    output_path: Path
    output_profile: dict[str, Any]
    write_offset_row: int = 0
    write_offset_col: int = 0
    _dst: Optional[Any] = field(default=None, init=False, repr=False)

    def open_local_resources(self) -> None:
        """Open the output file handle locally in the writer process."""
        self.close_local_resources()
        self._dst = rasterio.open(self.output_path, "r+")

    def close_local_resources(self) -> None:
        """Close the output file handle if open."""
        if self._dst is not None:
            self._dst.close()
            self._dst = None

    @property
    def dst(self) -> Any:
        """Return the initialized Rasterio destination handle."""
        section = "WRITER dst"
        if self._dst is None:
            raise RuntimeError(
                f"{section} Writer handle not initialized. Call open_local_resources() first."
            )
        return self._dst
