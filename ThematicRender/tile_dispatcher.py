from queue import Empty
from typing import Iterable, Dict, List, Callable, Optional, TypeAlias, Tuple

import rasterio
from rasterio.windows import Window

from ThematicRender.engine_resources import EngineResources
from ThematicRender.io_manager import get_read_geometry
from ThematicRender.ipc_packets import RenderPacket, DriverBlockRef, BlockReadPacket, WindowRect, \
    Envelope, Op, DispatchResult
from ThematicRender.keys import DriverKey
from ThematicRender.render_config import JobManifest

EnvelopeHandler: TypeAlias = Callable[[Envelope], None]


class TileDispatcher:
    """Manage per-tile block state for the active render job.

    This class owns tile assembly state only. It does not write to worker
    queues directly. Instead, it prepares packets and returns them to the
    orchestrator, which remains the single control-plane sender.

    Responsibilities:
        - Track active tiles for the current job
        - Allocate and track driver block references
        - Build LOAD_BLOCK packets for newly dispatched tiles
        - Track pending block counts
        - Return a RenderPacket when a tile becomes render-ready
        - Release shared-memory block references once a tile is written
    """

    def __init__(
            self, *, resources: "EngineResources", max_in_flight: int = 10, ) -> None:
        """Initialize dispatcher state.

        Args:
            resources: Shared engine resources, including the block registry.
            max_in_flight: Maximum number of tiles allowed in the active
                pipeline at once.
        """
        self.resources = resources
        self.max_tiles_in_flight = max_in_flight

        # Debug / telemetry counters
        self.unk_block_read = 0

        # State for the active job
        self.active_tiles: Dict[int, dict] = {}
        self.current_tile_iterator: Optional[Iterable[Tuple[int, Window]]] = None
        self.current_job_manifest: Optional["JobManifest"] = None

    def initialize_job(
            self, job_manifest: "JobManifest", win_list: List[rasterio.windows.Window], ) -> None:
        """Prepare dispatcher state for a new render job.

        Args:
            job_manifest: Fully resolved manifest for the active job.
            win_list: Tile windows to process for this job.
        """
        print(f"[Dispatcher] Job Initialized with {len(win_list)} tiles.")
        self.current_job_manifest = job_manifest
        self.active_tiles.clear()
        self.current_tile_iterator = enumerate(win_list)
        self.flush_queues()

    def prime_pipeline(self, job_id: str) -> List[DispatchResult]:
        """
        Prepare initial tile dispatches up to the in-flight limit.

        Args:
            job_id: Active job identifier.

        Returns:
            A list of dispatch results for the orchestrator to send.
        """
        results: List[DispatchResult] = []

        for _ in range(self.max_tiles_in_flight):
            result = self.dispatch_next_tile(job_id)
            if result.tile_id is None:
                break
            results.append(result)

        return results

    def dispatch_next_tile(self, job_id: str) -> DispatchResult:
        if self.current_tile_iterator is None or self.current_job_manifest is None:
            return DispatchResult(tile_id=None, read_packets=[], render_packet=None)

        try:
            tile_id, window = next(self.current_tile_iterator)
        except StopIteration:
            return DispatchResult(tile_id=None, read_packets=[], render_packet=None)

        block_table: Dict[DriverKey, DriverBlockRef] = {}
        read_requests: List[Envelope] = []
        pending_block_count = 0
        manifest = self.current_job_manifest

        for driver_id in manifest.resources.drivers:
            slot_id, is_cached = self.resources.registry.get_or_allocate(driver_id, window)

            halo = manifest.render_cfg.get_halo_for_driver(driver_id)
            # Use a static function that doesn't need an open file handle
            meta = manifest.driver_metadata[driver_id]
            geom = get_read_geometry(window, halo, meta['width'], meta['height'])
            block_table[driver_id] = DriverBlockRef(
                slot_id=slot_id, data_h_w=geom.full_h_w, inner_slices=geom.inner_slices, )

            if is_cached:
                continue

            pending_block_count += 1
            block_req = BlockReadPacket(
                job_id=job_id, driver_id=driver_id, tile_id=tile_id, target_slot_id=slot_id,
                window_rect=self.rect_from_window(window), halo=halo, )
            read_requests.append(Envelope(op=Op.LOAD_BLOCK, payload=block_req))

        self.active_tiles[tile_id] = {
            "pending_blocks": pending_block_count, "block_map": block_table, "window": window,
            "read_duration": 0.0,
        }

        if pending_block_count == 0:
            return DispatchResult(
                tile_id=tile_id, read_packets=[], render_packet=RenderPacket(
                    job_id=job_id, tile_id=tile_id, window_rect=self.rect_from_window(window),
                    block_map=block_table, read_duration=0.0, ), )

        return DispatchResult(
            tile_id=tile_id, read_packets=read_requests, render_packet=None, )

    def get_cached_tile_render_packet(
            self, job_id: str, tile_id: int, ) -> Optional[RenderPacket]:
        """Return a RenderPacket immediately if a tile has no pending reads.

        This supports the fast-path where all driver blocks were already cached
        when the tile was dispatched.

        Args:
            job_id: Active job identifier.
            tile_id: Tile to inspect.

        Returns:
            A RenderPacket if the tile is fully ready, else None.
        """
        tile = self.active_tiles.get(tile_id)
        if tile is None:
            return None

        if tile["pending_blocks"] != 0:
            return None

        return RenderPacket(
            job_id=job_id, tile_id=tile_id, window_rect=self.rect_from_window(tile["window"]),
            block_map=tile["block_map"], read_duration=tile["read_duration"], )

    def on_driver_block_loaded(
            self, job_id: str, tile_id: int, read_duration: float = 0.0, ) -> Optional[
        RenderPacket]:
        """Record one loaded driver block and return a tile when ready.

        Args:
            job_id: Active job identifier.
            tile_id: Tile whose block has finished loading.
            read_duration: Time spent loading this driver block.

        Returns:
            A RenderPacket if this block completes the tile, else None.

        Raises:
            ValueError: If pending block count underflows.
        """
        tile = self.active_tiles.get(tile_id)
        if tile is None:
            self.unk_block_read += 1
            return None

        tile["read_duration"] += read_duration
        tile["pending_blocks"] -= 1

        if tile["pending_blocks"] < 0:
            raise ValueError(
                f"Tile {tile_id} pending_blocks underflow for job '{job_id}'"
            )

        if tile["pending_blocks"] != 0:
            return None

        return RenderPacket(
            job_id=job_id, tile_id=tile_id, window_rect=self.rect_from_window(tile["window"]),
            block_map=tile["block_map"], read_duration=tile["read_duration"], )

    def on_tile_written(self, tile_id: int) -> bool:
        """Release all block references for a completed tile.

        Args:
            tile_id: Tile that has been fully written to output.

        Returns:
            True if the tile existed and resources were released, else False.
        """
        finished_tile = self.active_tiles.pop(tile_id, None)
        if finished_tile is None:
            return False

        for driver_key, ref in finished_tile["block_map"].items():
            self.resources.registry.release(driver_key, ref.slot_id)

        return True

    def flush_queues(self) -> None:
        """Best-effort drain of worker input queues before a new job starts."""
        for q in [self.resources.read_q, self.resources.work_q]:
            while not q.empty():
                try:
                    q.get_nowait()
                except Empty:
                    break

    def abort_job(self) -> None:
        """Release all active tile resources and clear dispatcher job state."""
        for tile in self.active_tiles.values():
            for driver_key, ref in tile["block_map"].items():
                self.resources.registry.release(driver_key, ref.slot_id)

        self.active_tiles.clear()
        self.current_tile_iterator = None
        self.current_job_manifest = None

    @staticmethod
    def rect_from_window(w: Window) -> WindowRect:
        """Convert a Rasterio window into an integer tuple."""
        return int(w.col_off), int(w.row_off), int(w.width), int(w.height)
