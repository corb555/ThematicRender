from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Set, Optional

from Pipeline.io_manager import IOManager
from Common.keys import DriverKey
from Pipeline.render_config import RenderConfig
from Pipeline.worker_context_base import (WorkerContextBase, )

# worker_contexts.py

@dataclass(slots=True)
class ReaderContext(WorkerContextBase):
    """Job settings for reader execution."""
    job_id: str
    render_cfg: RenderConfig
    anchor_key: DriverKey
    source_paths: dict[DriverKey, Path]

    # This must be in slots for the property to work
    _io: Optional[IOManager] = field(default=None, init=False, repr=False)

    @property
    def io(self) -> IOManager:
        """
        DIFFERENCE: This property now triggers its own initialization
        if it finds that the process-local IOManager is missing.
        """
        if self._io is None:
            # If the airlock forgot to open the door, we open it ourselves
            self.open_local_resources()

            # Final safety check
            if self._io is None:
                raise RuntimeError(f"[READER] Failed to auto-initialize IO for Job {self.job_id}")

        return self._io

    def open_local_resources(self) -> None:
        """
        FIX: This method is called by sync_ctx_for_packet.
        It must physically create the IOManager and open the file handles.
        """
        # 1. Clean up if already open (sanity check)
        self.close_local_resources()

        # 2. Instantiate the IOManager (Pure Science layer)
        # We use the config and paths that WERE pickled
        self._io = IOManager(
            render_cfg=self.render_cfg,
            drivers=self.source_paths,
            anchor_key=self.anchor_key
        )

        # 3. Physically open the Rasterio handles
        # This makes the IOManager 'Live' in this specific process
        self._io.__enter__()

        # print(f"✅ [ReaderContext] Local resources initialized for Job {self.job_id}")

    def close_local_resources(self) -> None:
        """Physically close file handles."""
        if self._io is not None:
            try:
                self._io.__exit__(None, None, None)
            except Exception:
                pass
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


# worker_contexts.py

@dataclass(slots=True)
class WriterContext(WorkerContextBase):
    """Job settings for writer execution."""
    job_id: str
    output_path: Path
    output_profile: dict[str, Any]  # <--- ADD THIS FIELD
    write_offset_row: int = 0
    write_offset_col: int = 0

    # Internal state (not part of __init__)
    _dst: Optional[Any] = field(default=None, init=False, repr=False)

    @property
    def dst(self):
        """Self-healing property: Opens the file handle if missing in this process."""
        if self._dst is None:
            self.open_local_resources()
        return self._dst

    def open_local_resources(self) -> None:
        """Physically open the .tmp file for writing."""
        self.close_local_resources()

        # Guard against race conditions during cancellation
        if not self.output_path.exists():
             raise FileNotFoundError(f"Writer cannot open missing file: {self.output_path}")


        import rasterio
        try:
            self._dst = rasterio.open(self.output_path, "r+")
        except Exception as e:
            raise RuntimeError(f"GDAL failed to open {self.output_path.name} in r+ mode: {e}")

    def close_local_resources(self) -> None:
        """Gracefully close the Rasterio handle."""
        if self._dst is not None:
            try:
                self._dst.close()
            except:
                pass
            self._dst = None
