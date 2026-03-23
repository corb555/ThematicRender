# pipeline_engine.py
from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import json
from pathlib import Path
from queue import Empty
import time
from typing import List, Callable, Optional, TypeAlias, Tuple, Counter

from rasterio.windows import Window

from ThematicRender.command_proxy import CommandProxy
from ThematicRender.compositing_engine import CompositingEngine
from ThematicRender.engine_resources import EngineResources
from ThematicRender.factor_engine import FactorEngine
from ThematicRender.io_manager import IOManager, IOSystem
from ThematicRender.ipc_packets import (Envelope, Op, JobDonePacket, ErrorPacket, BlockLoadedPacket, \
                                        TileWrittenPacket, SEV_WARNING, SEV_CANCEL, SEV_FATAL)
from ThematicRender.job_control import JobControl
from ThematicRender.noise_library import NoiseLibrary
from ThematicRender.reader_task import ReaderContext
from ThematicRender.render_config import derive_resources, JobManifest, RenderConfig
from ThematicRender.render_task import WorkerContext
from ThematicRender.surface_engine import SurfaceEngine
from ThematicRender.system_config import SystemConfig
from ThematicRender.tile_dispatcher import TileDispatcher
from ThematicRender.writer_task import WriterContext

EnvelopeHandler: TypeAlias = Callable[[Envelope], None]

from ThematicRender.theme_registry import ThemeRegistry


def dbg(msg, id):
    pass


# pipeline_engine.py
class PipelineEngine:
    def __init__(self, system_yml_path: Path):
        self.proxy = None
        self.engine_cfg = SystemConfig.load_engine_specs(system_yml_path)
        self.eng_resources: EngineResources = EngineResources(self.engine_cfg)

        self.io_system: IOSystem = IOSystem()
        self.dispatcher = TileDispatcher(resources=self.eng_resources, max_in_flight=10)
        self.orchestrator = PipelineOrchestrator(
            self.eng_resources, self.io_system, self.dispatcher
        )

    def start(self):
        self.eng_resources.setup_engine()
        self.proxy = CommandProxy(
            socket_path=self.engine_cfg.get("system.socket_path"),
            status_q=self.eng_resources.status_q, response_q=self.eng_resources.response_q
        )
        self.proxy.start()
        self.orchestrator.run_loop()


class PipelineOrchestrator:
    def __init__(self, eng_resources, io_system, dispatcher):
        self.eng_resources = eng_resources
        self.resolver: JobResolver = JobResolver()
        self.io_system = io_system
        self.dispatcher = dispatcher
        self.stats = JobTelemetry()
        self.last_progress_pulse = 0.0
        self.previous_ts = 0

        # Render engines
        self.factor_eng = None
        self.surface_eng = None
        self.factory = None

        self.pending_jobs: List[dict] = []
        self.job_control: JobControl = JobControl()

        # Command dispatch table
        self._dispatch: dict[Op, EnvelopeHandler] = {
            Op.JOB_REQUEST: self._handle_job_request, Op.SHUTDOWN: self._handle_shutdown,
            Op.BLOCK_LOADED: self._handle_block_loaded, Op.TILE_WRITTEN: self._handle_tile_written,
            Op.TILES_FINALIZED: self._handle_tiles_finalized, Op.ERROR: self._handle_error,
            Op.WRITER_ABORTED: self._handle_wr_abort,
        }

        self.running = True

    def run_loop(self) -> None:
        while self.running:
            # TODO Add 1.4 State Machine
            # TODO Add 2.3 Heartbeat Monitor
            try:
                # Timeout allows us to pulse progress even if no tiles are finishing
                envelope: Envelope = self.eng_resources.status_q.get(timeout=0.05)
            except Empty:
                self._pulse_client_progress()  # Pulse during idle/stall
                continue
            except KeyboardInterrupt:
                break

            self.update_telemetry(envelope.op)

            # Dispatch envelope to a handler
            self._dispatch.get(envelope.op, self._handle_unknown_op)(envelope)

            # Pulse during active processing
            self._pulse_client_progress()

    def _handle_job_request(self, envelope: Envelope) -> None:
        """Queue a new job request and start it if no job is active."""
        data = envelope.payload

        # Queue job
        self.pending_jobs.append(data)

        # Immediately run it if we're not busy
        if not self.job_control.busy:
            self._start_next_job()

    def showtime(self, msg):
        wall_start = datetime.now()
        start_ts = wall_start.strftime("%H:%M:%S.%f")[:-3]
        if self.previous_ts == 0:
            print(f"{start_ts} {msg}")
        else:
            print(f"{start_ts} {msg}. Elapsed: {wall_start - self.previous_ts}")
        self.previous_ts = wall_start

    def _start_next_job(self) -> bool:
        """Start the next queued job, if any."""
        if not self.pending_jobs: return False
        json_job_req = self.pending_jobs.pop(0)
        job_id = json_job_req.get("job_id", "unknown")

        try:
            # 1. Resolve request into a fully populated manifest
            job_manifest = self.resolver.create_job_manifest(json_job_req)

            # 2. HYDRATE: Setup persistent engines (Only if not already warm)
            if self.factor_eng is None:
                self._hydrate_logic(job_manifest.render_cfg, job_manifest.resources)

            # 3. HOT-SWAP: Update settings for the current job
            self.factor_eng.cfg = job_manifest.render_cfg
            self.surface_eng.cfg = job_manifest.render_cfg
            self.theme_reg.load_metadata(job_manifest.render_cfg)

            # 4. INITIALIZE FS: Prepare output directory and temp file
            self._unlink_file_if_exists(job_manifest.temp_out_path)
            self.io_system.initialize_physical_output(
                job_manifest.temp_out_path, job_manifest.profile
            )

            # 5. CONTEXT & IPC: Build worker recipes and publish to SHM
            win_list = self._generate_job_windows(job_manifest)
            factory = ContextFactory(
                self.factor_eng, self.surface_eng, themes=self.theme_reg,
                compositor=self.compositor, noise_registry=self.noise_lib, )
            reader_ctx, worker_ctx, writer_ctx = factory.sync_and_build(
                job_manifest, self.eng_resources, )
            self.job_control = JobControl(
                manifest=job_manifest, total_tiles=len(win_list), )

            #  Publish job context for workers
            self.eng_resources.update_context(
                job_id=job_manifest.job_id, reader_data=reader_ctx, worker_data=worker_ctx,
                writer_data=writer_ctx, )

            # 9. Initialize dispatcher with GLOBAL windows and prime pipeline
            self.dispatcher.initialize_job(job_manifest, win_list)
            dispatch_results = self.dispatcher.prime_pipeline(job_manifest.job_id)

            for result in dispatch_results:
                for env in result.read_packets:
                    self.send_to_worker("read_q", env)

                if result.render_packet is not None:
                    self.send_to_worker(
                        "work_q", Envelope(op=Op.RENDER_TILE, payload=result.render_packet), )

            return True
        except (ValueError, FileNotFoundError, IOError) as exc:
            # SEV_CANCEL equivalent: Configuration or Input Error
            print(f"⚠️ [ORCHESTRATOR] Job '{job_id}' Config Error : {exc}")
            self._send_to_client(
                {
                    "msg": "error", "job_id": job_id, "severity": 1,  # SEV_CANCEL
                    "message": f"⚠️ Job Initialization Failed: {str(exc)}"
                }
            )
            self.job_control.clear_job()

            # Try to start the next pending job if there is one
            self._start_next_job()
            return False

        except Exception as exc:
            # SEV_FATAL: Something crashed in the Orchestrator code itself
            import traceback
            traceback.print_exc()
            self._send_to_client(
                {
                    "msg": "error", "job_id": job_id, "severity": 0,  # SEV_FATAL
                    "message": f"SYSTEM CRITICAL: {str(exc)}"
                }
            )
            self._handle_shutdown(None)
            return False

    def _handle_tiles_finalized(self, envelope: Envelope) -> None:
        """Publish the finalized temp file and notify the client."""
        if self.job_control is None:
            raise ValueError("[ORCHESTRATOR] Received TILES_FINALIZED but no job is active")

        finalized_job_id = envelope.payload
        if finalized_job_id != self.job_control.job_id:
            raise ValueError(
                f"[ORCHESTRATOR] Received TILES_FINALIZED for job '{finalized_job_id}', "
                f"but active job is '{self.job_control.job_id}'"
            )

        try:
            # Publish temp -> final atomically after writer flush/close completes
            self.job_control.temp_out_path.replace(
                self.job_control.final_out_path
                )  # print(f"✅ [Orchestrator] Render complete for job: '{self.job_control.job_id}'")
        except MemoryError as exc:
            print(
                f"❌ [Orchestrator] Failed to publish temp output "
                f"'{self.job_control.temp_out_path}' -> '{self.job_control.final_out_path}': {exc}"
            )
            self._unlink_file_if_exists(self.job_control.temp_out_path)
            self._send_to_client(
                {
                    "msg": "error", "job_id": self.job_control.job_id,
                    "message": f"publish failure: {exc}",
                }
            )
            self.job_control.clear_job()
            self._start_next_job()
            return

        duration = self.job_control.elapsed
        print(
            f"✅ [Orchestrator] RENDER COMPLETE FOR JOB '{self.job_control.job_id}' "
            f"| Tiles: {self.job_control.total_tiles} "
            f"| Time: {duration:.3f}s "
            f"({(duration / self.job_control.total_tiles) * 1000:.1f}ms/tile)"
        )

        self._send_to_client(
            {
                "msg": "complete", "job_id": self.job_control.job_id,
                "path": str(self.job_control.final_out_path),
            }
        )
        self.showtime(f"JOB {self.job_control.job_id} COMPLETE")

        self.job_control.clear_job()
        self._start_next_job()

    def _generate_job_windows(self, manifest: JobManifest) -> List[Window]:
        """Calculates global windows using a uniform 256x256 grid."""

        if manifest.envelope is not None:
            # Use the existing preview envelope
            target_env = manifest.envelope
        else:
            # FULL RENDER: Create a virtual envelope covering the entire anchor
            meta = manifest.driver_metadata[manifest.resources.anchor_key]
            target_env = Window(0, 0, meta['width'], meta['height'])

        tiles = []
        # Step by 256 pixels across the target area
        for r in range(int(target_env.row_off), int(target_env.row_off + target_env.height), 256):
            for c in range(
                    int(target_env.col_off), int(target_env.col_off + target_env.width), 256
            ):
                # Calculate width/height, ensuring we don't go out of bounds
                w = min(256, int(target_env.col_off + target_env.width) - c)
                h = min(256, int(target_env.row_off + target_env.height) - r)
                tiles.append(Window(c, r, w, h))

        return tiles

    def _handle_block_loaded(self, envelope: Envelope) -> None:
        """Advance a tile after one block finishes loading."""
        packet: BlockLoadedPacket = envelope.payload
        if not self.valid_job_id(packet.job_id):
            return

        render_packet = self.dispatcher.on_driver_block_loaded(
            packet.job_id, packet.tile_id, packet.read_duration, )
        if render_packet is not None:
            self.send_to_worker(
                "work_q", Envelope(op=Op.RENDER_TILE, payload=render_packet), )

    def _handle_tile_written(self, envelope: Envelope) -> None:
        """Release tile resources via the dispatcher and advance job progress."""
        packet: TileWrittenPacket = envelope.payload
        if not self.valid_job_id(packet.job_id):
            return
        if True:
            dbg(
                f" >>> [RECV] Q: {"status":8} | OP: {envelope.op.name:15} | TILE: "
                f"{packet.tile_id:<5} | "
                f"JOB: {packet.job_id}"
                f" [STATE] In-Flight: {"":<3} | Pending Jobs: {"":<2} | "
                f"Progress: ", packet.tile_id
            )

        if not self.dispatcher.on_tile_written(packet.tile_id):
            return

        is_complete = self.job_control.mark_tile_written()
        if is_complete:
            self._finalize_job()
            return

        dispatch_result = self.dispatcher.dispatch_next_tile(self.job_control.job_id)
        if dispatch_result.tile_id is None:
            return

        for env in dispatch_result.read_packets:
            self.send_to_worker("read_q", env)

        if dispatch_result.render_packet is not None:
            self.send_to_worker(
                "work_q", Envelope(op=Op.RENDER_TILE, payload=dispatch_result.render_packet), )

    def _handle_unknown_op(self, envelope: Envelope) -> None:
        """Fallback handler for unregistered OpCodes."""
        self.stats.unknown_ops += 1
        print(f"⚠️ [Orchestrator] ERROR: Unknown OpCode: {envelope.op}")
        self._handle_shutdown(envelope)

    def _handle_job_cancel(self, envelope: Envelope) -> None:
        # TODO Implement
        print(f"⚠️ [Orchestrator] Job Cancel not implemented: {envelope.op}")

    def _handle_wr_abort(self, envelope: Envelope) -> None:
        # TODO Implement 2.2
        print(f"⚠️ [Orchestrator] Writer Abort not implemented: {envelope.op}")

    def valid_job_id(self, job_id):
        valid = job_id == self.job_control.job_id
        if not valid:
            self.stats.bad_job_ids += 1
        return valid

    def _handle_error(self, envelope: Envelope) -> None:
        """
        Handle a pipeline error, cancel output, or shutdown based on severity.
        Severity: 0=Fatal (Shutdown), 1=Cancel Job, 2=Warning (Continue)
        """
        # TODO Implement Remainder of error handling spec
        payload: ErrorPacket = envelope.payload
        job_id = payload.job_id or self.job_control.job_id
        sev = payload.severity

        # 1. Log to Orchestrator Console
        sev_label = {0: "FATAL", 1: "ERROR", 2: "WARNING"}.get(sev, "ERROR")
        print(
            f"❌ [{sev_label}] {payload.stage} stage failure "
            f"(Tile: {payload.tile_id}, Job: '{job_id}'): {payload.message}"
        )

        # 2. Forward to Client Proxy
        # We send the raw severity so the client can decide how to color the UI
        self._send_to_client(
            {
                "msg": "error", "job_id": job_id, "severity": sev,
                "message": f"{payload.stage} {sev_label.lower()}: {payload.message}",
            }
        )

        # 3. Action Logic
        if sev == SEV_WARNING:
            # Severity 2: Do nothing else; let the pipeline continue
            return

        if sev == SEV_CANCEL:
            # Severity 1: Stop the current job if it matches the active ID
            if self.job_control.job_id == job_id:
                # Notify Writer to unlink and close
                packet = JobDonePacket(job_id=self.job_control.job_id)
                self.send_to_worker('writer_q', Envelope(op=Op.JOB_CANCEL, payload=packet))

                # Reclaim Shared Memory Slots
                self.dispatcher.abort_job()

                # Local cleanup and state reset
                self._unlink_file_if_exists(self.job_control.temp_out_path)
                self.job_control.clear_job()

                # Note: We do NOT automatically call _start_next_job() here  # to allow the
                # developer to inspect the failure state.

        elif sev == SEV_FATAL:
            # Severity 0: The system is in an unrecoverable state
            print("🚨 FATAL ERROR: Initiating emergency system shutdown.")
            self._handle_shutdown(envelope)

    def _handle_shutdown(self, _envelope: Envelope) -> None:
        """Stop the event loop and discard any active temp output."""
        # TODO Implement 2.1 Shutdown
        if self.job_control.busy:
            packet = JobDonePacket(job_id=self.job_control.job_id)
            self.send_to_worker('writer_q', Envelope(op=Op.SHUTDOWN, payload=packet))
            self._unlink_file_if_exists(self.job_control.temp_out_path)

        self.running = False
        print("🛑 [Orchestrator] Shutting Down.")

    def send_to_worker(self, queue_attr: str, envelope: Envelope) -> None:
        """
        Centrally managed 'put' for all worker queues with state logging.
        queue_attr: 'read_q', 'work_q', or 'writer_q'
        """
        # 1.  Queue Put
        queue = getattr(self.eng_resources, queue_attr)
        queue.put(envelope)

        # 2. Update Stats
        payload = envelope.payload
        tile_id = getattr(payload, 'tile_id', '-')
        job_id = getattr(payload, 'job_id', self.job_control.job_id)

        # 3. Capture Current Orchestrator State
        pending_jobs = len(self.pending_jobs)
        in_flight = len(self.dispatcher.active_tiles)

        prog_str = f"{self.job_control.tiles_written}/{self.job_control.total_tiles}"

        # 4. Print Multi-line Visibility Block
        if True:
            dbg(
                f" >>> [SEND] Q: {queue_attr:8} | OP: {envelope.op.name:15} | TILE: {tile_id:<5} | "
                f"JOB: {job_id}"
                f" [STATE] In-Flight: {in_flight:<3} | Pending Jobs: {pending_jobs:<2} | "
                f"Progress: {prog_str}", tile_id
            )

    @staticmethod
    def _build_temp_output_path(final_path: Path, job_id: str) -> Path:
        """Return a temp output path in the same directory as the final output."""
        return final_path.with_name(f"{final_path.stem}.{job_id}.tmp{final_path.suffix}")

    @staticmethod
    def _unlink_file_if_exists(path: Optional[Path]) -> None:
        """Best-effort unlink."""
        if path is None:
            return
        try:
            if path.exists():
                path.unlink()
        except Exception as exc:
            print(f"⚠️ [Orchestrator] Failed to unlink temp file '{path}': {exc}")

    def _hydrate_logic(self, render_cfg, resources) -> None:

        # this is  Render specific, not Pipeline
        """Initialize and immediately validate the persistent math engines."""
        self.noise_lib = NoiseLibrary(render_cfg, profiles=render_cfg.noises, create_shm=True)
        self.eng_resources.manage_noise_library(self.noise_lib)
        self.theme_reg = ThemeRegistry(render_cfg)

        # Create the engine
        self.factor_eng = FactorEngine(
            render_cfg, self.theme_reg, self.noise_lib, render_cfg.factors, resources, None
        )

        # --- EARLY SANITY CHECK ---
        # If the engine holds a Queue, it will fail here instantly.
        # assert_pickle(self.factor_eng, "FactorEngine (Initial Hydration)")

        self.surface_eng = SurfaceEngine(render_cfg)
        # assert_pickle(self.surface_eng, "SurfaceEngine (Initial Hydration)")

        self.compositor = CompositingEngine()

    def _finalize_job(self) -> None:
        """Begin successful job finalization by asking the writer to flush and close."""
        if self.job_control is None:
            raise ValueError("[ORCHESTRATOR] Finalize Job but no job is active")

        packet = JobDonePacket(job_id=self.job_control.job_id)
        self.send_to_worker('writer_q', Envelope(op=Op.JOB_DONE, payload=packet))

    def _send_to_client(self, payload: dict) -> None:
        """Send a response payload back to the socket proxy."""
        self.eng_resources.response_q.put(payload)

    def update_telemetry(self, op):
        self.stats.last_op = op
        self.stats.op_counts[op] += 1
        self.stats.print_report(orchestrator=self, interval=1.0)

    def _pulse_client_progress(self) -> None:
        """Send a progress heartbeat to the client if a job is active."""
        if not self.job_control.busy:
            return

        now = time.perf_counter()
        if now - self.last_progress_pulse < 5.0:
            return

        job = self.job_control
        # Calculate integer percentage
        pct = int((job.tiles_written / job.total_tiles) * 100) if job.total_tiles > 0 else 0

        self._send_to_client(
            {
                "msg": "progress", "request_id": job.job_id, "progress": pct, "message": ""
            }
        )
        self.last_progress_pulse = now

        # print(f"Orch Dispatch MSG OP: {op}")


class JobResolver:
    """Resolve incoming job requests into fully validated render manifests."""

    def create_job_manifest(self, json_request: dict) -> JobManifest:
        # 1. Extract parameters
        # TODO  should system map client job_ids to internal job_ids?  Internal should be
        #  monotonically increasing.
        job_id = json_request.get("job_id")
        if not job_id:
            raise ValueError("Job request is missing required 'job_id'")

        params = json_request.get("params", {})
        config_path = Path(params.get("config_path")).expanduser()
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
        print(f"\n\nNEW JOB REQUEST - create_job_manifest for Job '{job_id}'")

        # 2. Load and resolve render configuration
        print("LOADING RENDER CONFIG")
        try:
            render_cfg = RenderConfig.load(config_path=config_path)
            render_cfg.resolve_paths(
                prefix=params.get("prefix", ""),
                build_dir=Path(params.get("build_dir", "build")).expanduser(),
                output_file=params.get("output_file", "output.tif")
            )
        except Exception as e:
            raise ValueError(f"YAML Syntax or Path error: {str(e)}")

        # 3. GENERATE HASHES for Hot-Reloading

        # LOGIC HASH: Representing Factor math, logic parameters, and driver specs
        logic_data = {
            "factors": render_cfg.raw_defs.get("factors", {}),
            "factor_specs": render_cfg.raw_defs.get("factor_specs", {}),
            "drivers": render_cfg.raw_defs.get("drivers", {}),
            "driver_specs": render_cfg.raw_defs.get("driver_specs", {})
        }
        logic_hash = self.generate_content_hash(logic_data)

        # 1. Capture QML Freshness
        qml_path = render_cfg.path("theme_qml")

        # Get timestamp (0 if file is missing)
        qml_mtime = qml_path.stat().st_mtime if qml_path and qml_path.exists() else 0

        # 2. Inject into Style Data before hashing
        style_data = {
            "surfaces": render_cfg.raw_defs.get("surfaces", {}),
            "pipeline": render_cfg.raw_defs.get("pipeline", []),
            "theme_smoothing": render_cfg.raw_defs.get("theme_smoothing_specs", {}),
            "surface_modifier_specs": render_cfg.raw_defs.get("surface_modifier_specs", {}),
            "qml_mtime": qml_mtime
        }
        style_hash = self.generate_content_hash(style_data)

        # TODO noise_profiles is not hashed

        # 4. Resolve Resources and Geography
        resources = derive_resources(render_cfg=render_cfg)

        # Resolve output paths
        final_out_path = Path(render_cfg.files["output"])
        temp_out_path = self.build_temp_output_path(final_out_path, job_id)
        render_cfg.files["output"] = temp_out_path

        # Metadata capture
        percent = float(params.get("percent", 0.0))
        row_focal = float(params.get("row", 0.0))
        col_focal = float(params.get("col", 0.0))
        envelope: Optional[Window] = None
        write_offset = (0, 0)
        driver_metadata = {}

        # Handle rasterio.errors.RasterioIOError
        try:
            with IOManager(render_cfg, resources.drivers, resources.anchor_key) as io:
                # Geography Hash (based on paths and mtimes)
                geography_hash = self.generate_region_hash(resources)
                profile = self.build_output_profile(io)

                for dkey in resources.drivers:
                    src = io.sources[dkey]
                    driver_metadata[dkey] = {"width": src.width, "height": src.height}

                if 0.0 < percent < 1.0:
                    envelope = self.calculate_preview_window(
                        io.anchor_src, percent=percent, rel_x=col_focal, rel_y=row_focal
                    )
                    if envelope is not None:
                        profile.update(
                            {
                                "width": int(envelope.width), "height": int(envelope.height),
                                "transform": io.anchor_src.window_transform(envelope),
                            }
                        )
                        write_offset = (int(envelope.row_off), int(envelope.col_off))
        except Exception as e:
            raise IOError(f"Could not open source files: {str(e)}")

        # 5. Add hashes
        resources = resources.with_hashes(
            geography_hash=geography_hash, logic_hash=logic_hash, style_hash=style_hash
        )

        return JobManifest(
            job_id=job_id, render_cfg=render_cfg, resources=resources,
            final_out_path=final_out_path, temp_out_path=temp_out_path, profile=profile,
            region_id=geography_hash, envelope=envelope, write_offset=write_offset,
            render_params=(percent, row_focal, col_focal), driver_metadata=driver_metadata
        )

    @staticmethod
    def generate_content_hash(data: dict) -> str:
        """Create a stable MD5 hash of a dictionary."""
        # sort_keys=True is vital to ensure the same YAML content
        # produces the same hash regardless of key order.
        encoded = json.dumps(data, sort_keys=True).encode("utf-8")
        return hashlib.md5(encoded).hexdigest()

    @staticmethod
    def build_temp_output_path(final_path: Path, job_id: str) -> Path:
        """
        Build a temporary output path in the same directory as the final output.

        Args:
            final_path: Final published output path.
            job_id: Active job identifier.

        Returns:
            Temporary render output path.
        """
        return final_path.with_name(f"{final_path.stem}.{job_id}.tmp")

    @staticmethod
    def generate_region_hash(resources) -> str:
        """
        Create a stable hash based on file paths and modification timestamps.

        Args:
            resources: Resolved render resources.

        Returns:
            Stable hash representing the current source-data region context.
        """
        context_parts = []

        for path in sorted(Path(p).resolve() for p in resources.drivers.values()):
            stat = path.stat()
            context_parts.append(f"{path}|{stat.st_mtime_ns}")

        raw_context = "|".join(context_parts)
        return hashlib.md5(raw_context.encode("utf-8")).hexdigest()

    @staticmethod
    def build_output_profile(io: "IOManager") -> dict:
        """
        Generate the Rasterio profile for the output GeoTIFF.

        Args:
            io: Open IO manager for the anchor dataset.

        Returns:
            Raster output profile dictionary.
        """
        anchor = io.anchor_src
        return {
            "driver": "GTiff", "height": anchor.height, "width": anchor.width, "count": 3,
            "dtype": "uint8", "crs": anchor.crs, "transform": anchor.transform, "tiled": True,
            "blockxsize": 256, "blockysize": 256, "compress": "deflate", "predictor": 2,
            "nodata": None,
        }

    @staticmethod
    def calculate_preview_window(
            src, percent: float, rel_x: float, rel_y: float, ) -> Window:
        """
        Calculate a global preview window using normalized focal coordinates.

        Args:
            src: Anchor Rasterio source.
            percent: Fraction of the full image size to render.
            rel_x: Horizontal focal point in normalized coordinates.
            rel_y: Vertical focal point in normalized coordinates.

        Returns:
            Block-aligned preview window.
        """
        full_w, full_h = src.width, src.height
        target_w, target_h = int(full_w * percent), int(full_h * percent)

        col_off = int(full_w * rel_x) - (target_w // 2)
        row_off = int(full_h * rel_y) - (target_h // 2)

        col_off = (max(0, col_off) // 256) * 256
        row_off = (max(0, row_off) // 256) * 256

        col_off = max(0, min(col_off, full_w - target_w))
        row_off = max(0, min(row_off, full_h - target_h))

        return Window(
            col_off, row_off, min(target_w, full_w - col_off), min(target_h, full_h - row_off), )


class ContextFactory:
    """
    Assembles worker-specific contexts for serialization.
    Handles the "handshake" between the persistent daemon state
    and the ephemeral render task.
    """

    def __init__(
            self, factor_eng, surface_eng, themes, compositor, noise_registry
    ):
        # These are the procedural engines that persist in the Daemon
        self.factor_eng = factor_eng
        self.surface_eng = surface_eng
        self.themes = themes
        self.compositor = compositor
        self.noise_registry = noise_registry

    def sync_and_build(
            self, manifest: JobManifest, resources: 'EngineResources'
    ) -> Tuple[ReaderContext, WorkerContext, WriterContext]:
        """
        1. Synchronizes the Registry (Purge slot cache if region changed).
        2. Builds Contexts for Reader, Worker, and Writer.
        """

        # 1. Registry Context Check
        # If the region_id has changed, we must purge the slot cache mappings
        if resources.registry.context_id != manifest.region_id:
            print(
                f"🔄 [ContextFactory] Region is different. Region hash={manifest.region_id}. "
                f"Purging Slot Cache"
            )
            resources.registry.reset_context(manifest.region_id)
        else:
            print(f" [ContextFactory] Warm Slot Cache: {manifest.region_id}")

        # 2. Instantiate Reader Context
        reader_ctx = ReaderContext(
            render_cfg=manifest.render_cfg, anchor_key=manifest.resources.anchor_key,
            source_paths=manifest.resources.drivers, job_id=manifest.job_id
        )

        # 3. Instantiate Worker Context
        worker_ctx = WorkerContext(
            render_cfg=manifest.render_cfg, themes=self.themes, compositor=self.compositor,
            pipeline=manifest.render_cfg.pipeline, anchor_key=manifest.resources.anchor_key,
            surface_inputs=manifest.resources.surface_inputs, resources=manifest.resources,
            noise_registry=self.noise_registry, job_id=manifest.job_id
        )

        # 4. Instantiate Writer Context
        writer_ctx = WriterContext(
            output_path=manifest.temp_out_path, output_profile=manifest.profile,
            write_offset_row=manifest.write_offset[0], write_offset_col=manifest.write_offset[1],
            job_id=manifest.job_id
        )

        return reader_ctx, worker_ctx, writer_ctx


@dataclass
class JobTelemetry:
    job_id: str = "IDLE"
    start_time: float = 0.0
    last_report_time: float = 0.0
    total_tiles: int = 0
    tiles_written: int = 0
    op_counts: Counter = field(default_factory=Counter)
    last_op: Op = Op.TILES_FINALIZED
    unknown_ops: int = 0
    bad_job_ids: int = 0
    pending_dependencies: dict[int, int] = field(default_factory=dict)

    def reset(self, job_id: str, total_tiles: int) -> None:
        """Reset telemetry for a newly started job."""
        self.job_id = job_id
        self.total_tiles = total_tiles
        self.tiles_written = 0
        self.start_time = time.perf_counter()
        self.last_report_time = 0.0
        self.pending_dependencies.clear()

    def print_report(
            self, *, orchestrator: "PipelineOrchestrator", interval: float = 5.0, ) -> None:
        """Print a throttled runtime report for debugging."""
        return
