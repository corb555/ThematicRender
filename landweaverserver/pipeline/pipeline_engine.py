# pipeline_engine.py
from dataclasses import dataclass, field
from datetime import datetime
import hashlib
from pathlib import Path
from queue import Empty
import time
import traceback
from types import SimpleNamespace
from typing import List, Callable, Optional, TypeAlias, Counter

from rasterio.windows import Window

from landweaverserver.common.ipc_packets import (Envelope, Op, JobDonePacket, ErrorPacket, BlockLoadedPacket, \
                                                 TileWrittenPacket, SEV_WARNING, SEV_CANCEL, SEV_FATAL)
from landweaverserver.pipeline.client_proxy import ClientProxy
from landweaverserver.pipeline.engine_resources import EngineResources
from landweaverserver.pipeline.io_manager import IOManager, IOSystem
from landweaverserver.pipeline.job_control import JobControl, JobManifest
from landweaverserver.pipeline.render_stack import RenderStack
from landweaverserver.pipeline.system_config import SystemConfig
from landweaverserver.pipeline.tile_dispatcher import TileDispatcher, DispatchResult
from landweaverserver.render.render_config import derive_resources, RenderConfig, analyze_pipeline

EnvelopeHandler: TypeAlias = Callable[[Envelope], None]


def dbg(msg, id):
    pass


# pipeline_engine.py
class PipelineEngine:
    def __init__(self, system_yml_path: Path):
        self.client_proxy = None
        resolver = JobResolver(config_loader=lambda p: RenderConfig.load(p))
        render_stack = RenderStack()
        engine_cfg = SystemConfig.load_engine_specs(system_yml_path)
        self.eng_resources = EngineResources(engine_cfg)
        io_system: IOSystem = IOSystem()
        dispatcher = TileDispatcher(resources=self.eng_resources, max_in_flight=10)
        self.client_proxy = ClientProxy(
            engine_cfg.get("system.socket_path"), status_q=self.eng_resources.status_q,
            response_q=self.eng_resources.response_q
        )
        self.orchestrator = PipelineOrchestrator(
            eng_resources=self.eng_resources, io_system=io_system, dispatcher=dispatcher,
            resolver=resolver, render_stack=render_stack
        )

    def start(self):
        self.eng_resources.start()
        self.client_proxy.start()
        self.orchestrator.loop()


class PipelineOrchestrator:
    def __init__(self, eng_resources, io_system, dispatcher, resolver, render_stack):
        self.last_activity_ts = None
        self.previous_ts = None
        self.eng_resources = eng_resources
        self.resolver = resolver
        self.render_stack = render_stack
        self.io_system = io_system
        self.dispatcher = dispatcher
        self.stats = JobTelemetry()
        self.last_progress_pulse = 0.0
        self.pending_jobs: List[dict] = []
        self.job_control: JobControl = JobControl()

        # Command dispatch table
        self._dispatch: dict[Op, EnvelopeHandler] = {
            Op.JOB_REQUEST: self._handle_job_request, Op.SHUTDOWN: self._initiate_shutdown,
            Op.BLOCK_LOADED: self._handle_block_loaded, Op.TILE_WRITTEN: self._handle_tile_written,
            Op.TILES_FINALIZED: self._handle_tiles_finalized, Op.ERROR: self._handle_error,
            Op.WRITER_ABORTED: self._handle_wr_abort,
        }

        self.running = True

    def loop(self) -> None:
        while self.running:
            try:
                # 1.  INGESTION
                try:
                    envelope: Envelope = self.eng_resources.status_q.get(timeout=0.05)
                    self.last_activity_ts = time.perf_counter()  # Reset watchdog
                except Empty:
                    # Heartbeat check (Phase 3 of your Tock strategy)
                    # TODO self._check_for_deadlocks()
                    self._pulse_client_progress()
                    continue

                # 2.  DISPATCH
                self.update_telemetry(envelope.op)
                self._dispatch.get(envelope.op, self._handle_unknown_op)(envelope)
                self._pulse_client_progress()

            except KeyboardInterrupt:
                print("🛑 User initiated shutdown (Ctrl+C)")
                self._initiate_shutdown("User Interruption")
                break

            except Exception as e:
                # Something went wrong in the Orchestrator logic itself.
                # A. High-Fidelity Logging
                print(f"\n CRITICAL ORCHESTRATOR FAILURE")
                traceback.print_exc()

                # B. Notify the Client
                self._send_to_client(
                    {
                        "msg": "error", "job_id": "system", "severity": 0,
                        "message": f"render pipeline Crash: {str(e)}"
                    }
                )

                # C. Resource Reclamation
                self._initiate_shutdown(f"System Error: {e}")

                # D. Exit the loop
                break

    def _handle_job_request(self, envelope: Envelope) -> None:
        """Queue a new job request. Start it if no job is active."""
        data = envelope.payload

        # Queue job
        self.pending_jobs.append(data)

        # Immediately run it if we're not busy.
        if not self.job_control.busy:
            self._start_next_job()

    def _start_next_job(self) -> None:
        """
        Starts the next valid queued job.
        Walks thru the queue until it finds a job that can start
        """
        while self.pending_jobs:
            json_job_req = self.pending_jobs.pop(0)
            job_id = json_job_req.get("job_id", "unknown")

            try:
                job_manifest = self._prepare_manifest(json_job_req)
            except (ValueError, IOError) as exc:
                print(f"Job {job_id} error: {exc}")
                self._send_to_client(
                    {
                        "msg": "error", "job_id": job_id, "severity": 1, "message": str(exc),
                        "report": "",
                    }
                )
                continue
            self._launch_job(job_manifest)
            return

    def _prepare_manifest(self, json_job_req) -> JobManifest:
        """
        Prepare job manifest
        Return manifest on success or raise exception
        """
        # 1. Parse render config and build the manifest
        job_manifest = self.resolver.create_job_manifest(json_job_req)

        # 2. Verify Engine has the required sources for this job - raises Exception
        self._verify_required_sources(job_manifest)

        # 3. Verify render config - raises Exception
        self._verify_render_config(job_manifest)

        return job_manifest

    def _launch_job(self, job_manifest: JobManifest) -> bool:
        """
        Launch the  job in the  manifests after preparing worker contexts.
        """
        job_id = job_manifest.job_id
        self.showtime("Launch Job")

        try:
            # Reset cache if job is for a different region
            self.eng_resources.sync_to_geography(job_manifest.region_id)

            # 4. Init render engines

            if self.render_stack.factor_eng is None:
                self.render_stack.init_render_engines(
                    job_manifest.render_cfg, job_manifest.resources, self.eng_resources, )
                self.showtime("render Stack done")

            # 5. Initialize output file
            self._unlink_file_if_exists(job_manifest.temp_out_path)
            self.io_system.initialize_physical_output(
                job_manifest.temp_out_path, job_manifest.profile, )

            # 6. Reset telemetry
            if hasattr(self.eng_resources, "registry"):
                self.eng_resources.registry.start_session()

            # 7. Prepare worker contexts
            reader_ctx, worker_ctx, writer_ctx = self.render_stack.prepare_job_contexts(
                job_manifest
            )
            self.showtime("Worker Context done")

            # 8. Initialize job control
            win_list = self._generate_job_windows(job_manifest)
            self.job_control = JobControl(
                manifest=job_manifest, total_tiles=len(win_list), )

            # 9. Publish context to workers
            self.eng_resources.update_context(
                job_id=job_id, reader_data=reader_ctx, worker_data=worker_ctx,
                writer_data=writer_ctx, )
            self.showtime("Context published")

            # 10. Initialize Dispatcher
            self.dispatcher.initialize_job(job_manifest, win_list)

            # Prime the pipeline
            candidates = self.dispatcher.get_priming_list(job_id)

            for result in candidates:
                for read_env in result.read_packets:
                    self.send_to_worker("reader_q", read_env)

                if result.render_packet:
                    self.send_to_worker(
                        "worker_q", Envelope(op=Op.RENDER_TILE, payload=result.render_packet), )
            self.showtime("pipeline primed")
            # Return to main processing loop and finish rest of work
            return True

        except Exception:
            raise

    def _handle_job_cancel(self, envelope: Envelope) -> None:
        print(f"⚠️ [Orchestrator] Job Cancel : {envelope.op}")
        # 1. Global SHM Flip (Interruption)
        self.eng_resources.cancel_active_job()

        # 2. pipeline Signaling (Cleanup)
        self.send_to_worker('writer_q', Envelope(op=Op.JOB_CANCEL))

        # 3. Logic cleanup
        self.job_control.clear_job()
        self._start_next_job()

    def _handle_error(self, envelope: Envelope) -> None:
        """
        Handle a pipeline error, cancel output, or shutdown based on severity.
        Severity: 0=Fatal (Shutdown), 1=Cancel Job, 2=Warning (Continue)
        """
        payload: ErrorPacket = envelope.payload
        job_id = payload.job_id or self.job_control.job_id
        sev = payload.severity
        print(f"Err sev={sev}")

        # 1. Log to Orchestrator Console
        sev_label = {0: "FATAL", 1: "CANCEL", 2: "WARNING"}.get(sev)
        print(
            f"pipeline received: Sev: {sev_label} From: {payload.section}   "
            f"Job: '{job_id}' Error: {payload.message}"
        )

        # 2. Forward to Client Proxy
        # We send the raw severity so the client can decide how to color the UI
        self._send_to_client(
            {
                "msg": "error", "job_id": job_id, "severity": sev,
                "message": f"{payload.section} {sev_label.lower()}: {payload.message}",
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
                self.eng_resources.cancel_active_job()

                # Local cleanup and state reset
                self.job_control.clear_job()
                self._start_next_job()

        elif sev == SEV_FATAL:
            # Severity 0: The system is in an unrecoverable state
            print(" FATAL ERROR: Initiating  system shutdown.")
            self._initiate_shutdown(" FATAL ERROR: Initiating  system shutdown.")

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
            )  # print(f"✅ [Orchestrator] render complete for job: '{self.job_control.job_id}'")
        except Exception as exc:
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

        # CACHE stats
        self._print_cache_analysis()
        self.job_control.clear_job()
        self._start_next_job()

    def _handle_block_loaded(self, envelope: Envelope) -> None:
        """Advance a tile after one block finishes loading."""
        packet: BlockLoadedPacket = envelope.payload
        if not self.valid_job_id(packet.job_id):
            return

        render_packet = self.dispatcher.on_source_block_loaded(
            packet.job_id, packet.tile_id, packet.read_duration, )
        if render_packet is not None:
            self.send_to_worker(
                "worker_q", Envelope(op=Op.RENDER_TILE, payload=render_packet), )

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

        dispatch_result: DispatchResult = self.dispatcher.dispatch_next_tile(
            self.job_control.job_id
        )
        if dispatch_result.tile_id is None:
            return

        for env in dispatch_result.read_packets:
            self.send_to_worker("reader_q", env)

        if dispatch_result.render_packet is not None:
            self.send_to_worker(
                "worker_q", Envelope(op=Op.RENDER_TILE, payload=dispatch_result.render_packet), )

    def _handle_wr_abort(self, envelope: Envelope) -> None:
        self._initiate_shutdown()

    def valid_job_id(self, job_id):
        return job_id == self.job_control.job_id

    def _initiate_shutdown(self, reason: str):
        """ cleanup to prevent zombie processes and SHM leaks."""
        eng = self.eng_resources
        print(f"\n [Orchestrator] Shutdown Initiated: {reason}")

        # 1. Global shutdown Signal
        # This acts as a circuit breaker for workers currently processing a tile.
        if eng.ctx_store:
            try:
                eng.ctx_store.set_shutdown()
                print("   - Global State set to SHUTDOWN ")
            except Exception as e:
                print(f"   ⚠️ Failed to set  shutdown state: {e}")

        # 2. Sequential Poison Pill Injection
        # We must send one SHUTDOWN packet per process in each pool.
        print("   - Dispatching poison pills to worker queues...")

        # Reader Pills
        for _ in eng.reader_procs:
            self.send_to_worker("reader_q", Envelope(op=Op.SHUTDOWN, payload=None))

        # Worker Pills
        for _ in eng.worker_procs:
            self.send_to_worker("worker_q", Envelope(op=Op.SHUTDOWN, payload=None))

        # Writer Pill (Only 1 writer)
        if eng.writer_proc:
            self.send_to_worker("writer_q", Envelope(op=Op.SHUTDOWN, payload=None))

        # 3. Graceful Join with Forceful Fallback
        # We give the processes a moment to see the pill and close their own handles.
        all_procs = eng.reader_procs + eng.worker_procs + [eng.writer_proc]

        print("   - Waiting for processes to exit...")
        for proc in all_procs:
            if proc is None:
                continue

            if proc.is_alive():
                # Wait 1.0s for the process to exit cleanly via its loop logic
                proc.join(timeout=1.0)

                # If still alive after timeout, it's deadlocked; force a hard kill
                if proc.is_alive():
                    print(f"   ⚠️ Process {proc.name} unresponsive. Force terminating...")
                    proc.terminate()
                    proc.join(timeout=0.2)  # Final join to clean up resources

        # 4. Physical Resource Reclamation
        # Unlink all SHM segments and close queues via the ExitStack logic
        print("   - Unlinking Shared Memory and closing IPC...")
        eng.cleanup()

        print("✅ [Orchestrator] System Purge Complete. Daemon Halted.")
        self.running = False

    def showtime(self, msg):
        wall_start = datetime.now()
        start_ts = wall_start.strftime("%H:%M:%S.%f")[:-3]
        if self.previous_ts is None:
            print(f"{start_ts} {msg}")
        else:
            print(f"{start_ts} {msg}. Elapsed: {wall_start - self.previous_ts}")
        self.previous_ts = wall_start

    def _handle_unknown_op(self, envelope: Envelope) -> None:
        """Fallback handler for unregistered OpCodes."""
        self.stats.unknown_ops += 1
        print(f"⚠️ [Orchestrator] ERROR: Unknown OpCode: {envelope.op}")
        self._initiate_shutdown(" ")

    def _print_cache_analysis(self):
        stats = self.eng_resources.registry.get_telemetry()

        total_req = stats['hits'] + stats['misses']
        hit_pct = (stats['hits'] / total_req * 100) if total_req > 0 else 0
        fill_pct = (stats['slots_used'] / stats['slots_total'] * 100) if stats[
                                                                             'slots_total'] > 0 \
            else 0

        # 1. Basic Stats
        print(f"\n---  CACHE MEMORY REPORT ---")
        print(f"Physical RAM Reserved: {stats['mb_allocated']:.1f} MB")
        print(f"Cache Fill:      {stats['slots_used']}/{stats['slots_total']} ({fill_pct:.1f}%)")
        print(
            f"Hit Ratio:             {hit_pct:.1f}% ({stats['hits']} hits, {stats['misses']} "
            f"misses)"
        )

        # 2. Automated Analysis
        print(f"Analysis:")

        if stats['is_cold']:
            print(f"  🔸 Status: COLD START. This was the first time rendering this region.")
        elif hit_pct > 99.9:
            print(f"  🔹 Status: 100% of data was served from RAM.")
            print(f"  🔹 Impact: The entire region is currently pinned in memory.")
        elif hit_pct > 80:
            print(f"  🔹 Status: OPTIMIZED. Most data was served from RAM.")
            print(f"  🔹 Impact: Your current slot count is perfectly tuned for this area.")
        elif fill_pct > 95:
            print(f"  🛑 Status: SATURATED. The cache is full and ejecting blocks.")
            print(f"  🛑 Recommendation: On your 32GB system, INCREASE 'slots' in system.yml.")
        if fill_pct < 40 and not stats['is_cold']:
            print(f"  💡 Status: OVER-PROVISIONED. You are  using {fill_pct:.1f}% of reserved RAM.")
            print(
                f"  💡 Recommendation: You can safely reduce slots if you need RAM for other apps."
            )

        # 3. Disk I/O Savings
        # Estimate: Each block is roughly 0.4MB (256x256 float32 + mask)
        io_saved_mb = stats['hits'] * 0.4
        if io_saved_mb > 0:
            print(f"Performance Gain: Prevented {io_saved_mb:.1f} MB of redundant disk reads.")
        print("-" * 30 + "\n")

        # Calculate how much of the work relied on 'Scratchpad' memory
        transit_pct = (stats['transit_demands'] / (stats['hits'] + stats['misses']) * 100)

        print(f"Transit Utilization:   {stats['transit_demands']} blocks ({transit_pct:.1f}%)")
        print(f"Analysis (Ratio Balance):")

        # RULE 1: Static Ratio is too LOW
        if stats['slots_used'] == stats['slots_total'] and stats['transit_demands'] > 0:
            if stats['mb_allocated'] < 8000:  # You have 32GB, so 8GB is a safe ceiling
                print(f"  📈 Suggestion: INCREASE 'static_ratio' or 'input_slots'.")
                print(
                    f"     You have plenty of RAM. Moving more blocks to Static will boost Hit "
                    f"Ratio."
                )

        # RULE 2: Static Ratio is too HIGH (Danger of Deadlock)
        # If transit_demands is very high relative to available transit slots
        transit_slots = stats['slots_total'] * 0.25  # current default
        if stats['transit_demands'] > (transit_slots * 2):  # Very high turnover
            print(f"  ⚠️ Warning: High Transit Turnover.")
            print(
                f"     If workers hit a timeout (7s), DECREASE 'static_ratio' to free up "
                f"scratchpad."
            )

        # RULE 3: Perfect Balance
        if hit_pct > 70 and stats['transit_demands'] == 0:
            print(f"    Status: OPTIMIZED BALANCE.")
            print(f"     The region fits entirely in the Static cache.")

    @staticmethod
    def _generate_job_windows(manifest: JobManifest) -> List[Window]:
        """Calculates global windows using a uniform 256x256 grid."""

        if manifest.envelope is not None:
            # Use the existing preview envelope
            target_env = manifest.envelope
        else:
            # FULL RENDER: Create a virtual envelope covering the entire anchor
            meta = manifest.source_metadata[manifest.resources.anchor_key]
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

    def send_to_worker(self, queue_attr: str, envelope: Envelope) -> None:
        """
        Hardened IPC dispatch for worker queues with fault detection.
        """
        # 1. DEFENSIVE QUEUE LOOKUP
        # getattr is safe, but we must ensure the attribute exists and isn't None
        queue = getattr(self.eng_resources, queue_attr, None)
        if queue is None:
            print(f"⚠️ [SEND_ERROR] Target queue '{queue_attr}' is not initialized.")
            return

        # 2. THE HARDENED 'PUT'
        try:
            # We use a non-blocking put or a very short timeout to detect
            # deadlocked queues, but for this architecture, a standard put
            # wrapped in exception handling is usually the 'Fact-Based' choice.
            queue.put(envelope)
        except (OSError, ValueError, BrokenPipeError) as e:
            # This happens if the queue was closed by another process
            # or the system is halfway through a shutdown.
            print(f"❌ [IPC FAILURE] Cannot send to {queue_attr}: {e}")
            # If the system is supposed to be running, this is a fatal logic error
            if self.running:
                self._initiate_shutdown(f"IPC Channel {queue_attr} collapsed.")
            return

        # 3. DEFENSIVE METADATA EXTRACTION
        # We must assume self.job_control or envelope.payload could be None
        # during edge-case state transitions (like a shutdown).
        payload = envelope.payload
        tile_id = getattr(payload, 'tile_id', '-')

        # Safe Job ID fallback
        job_id = "N/A"
        if payload and hasattr(payload, 'job_id'):
            job_id = payload.job_id
        elif self.job_control:
            job_id = self.job_control.job_id

        # 4. SAFE STATE CAPTURE
        pending_jobs = len(self.pending_jobs)
        in_flight = len(self.dispatcher.active_tiles) if self.dispatcher else 0

        # Calculate progress only if job_control is active
        if self.job_control:
            prog_str = f"{self.job_control.tiles_written}/{self.job_control.total_tiles}"
        else:
            prog_str = "IDLE"

        # 5. VISIBILITY (Using the existing dbg helper)
        dbg(
            f" >>> [SEND] Q: {queue_attr:8} | OP: {envelope.op.name:15} | TILE: {tile_id:<5} | "
            f"JOB: {job_id:10} | "
            f"[STATE] In-Flight: {in_flight:<3} | Pending: {pending_jobs:<2} | "
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
        if now - self.last_progress_pulse < 0.3:
            return

        job = self.job_control

        # Calculate float percentage and clamp to a visible minimum.
        raw_pct = (job.tiles_written / job.total_tiles) * 100.0 if job.total_tiles > 0 else 0.0
        pct = round(min(99.99, max(0.01, raw_pct)), 2)

        self._send_to_client(
            {
                "msg": "progress",
                "request_id": job.job_id,
                "progress": pct,
                "message": "",
            }
        )
        self.last_progress_pulse = now

    def _verify_required_sources(self, job_manifest) -> None:
        """Verify that all sources required by the render are available.

        Args:
            job_manifest: Resolved manifest for the pending job.

        Raises:
            Exception if not all required sources are available
        """
        job_id = job_manifest.job_id

        required_sources = set(job_manifest.resources.sources.keys())
        allocated_pools = set(self.eng_resources.pool_map.keys())

        if required_sources.issubset(allocated_pools):
            return

        missing = required_sources - allocated_pools
        missing_sorted = ", ".join(sorted(missing))

        error_msg = (f"⚠️ Job: {job_id} - Missing source configuration\n"
                     f"This render requires source(s) that are not available in the current "
                     f"engine configuration.\n"
                     f"Missing: {missing_sorted}\n"
                     f"To fix this, add the missing source(s) to 'engine_config.yml' under "
                     f"'source_specs', then restart Land Weaver Server.")
        raise ValueError(f"⚠️ {error_msg}")

    def _verify_render_config(self, job_manifest) -> None:
        """Verify that the render pipeline is internally valid.

        Args:
            job_manifest: Resolved manifest for the pending job.

        Raises:
            Exception if the pipeline audit fails.
        """
        audit_ctx = SimpleNamespace(
            render_cfg=job_manifest.render_cfg, eng_resources=self.eng_resources,
            theme_registry=self.render_stack.theme_reg,
            anchor_key=job_manifest.resources.anchor_key, )

        has_errors, report_md, raw_errors = analyze_pipeline(audit_ctx)
        if not has_errors:
            return True

        job_id = job_manifest.job_id
        error_summary = "\n".join(f"• {err}" for err in raw_errors[:2])

        if len(raw_errors) > 2:
            error_summary += f"\n...and {len(raw_errors) - 2} more errors."

        final_msg = f"⚠️ render config errors:\n{error_summary}"
        raise ValueError(final_msg)


class JobResolver:
    def __init__(self, config_loader: Callable[[Path], RenderConfig]):
        self.config_loader = config_loader

    def create_job_manifest(self, json_request: dict) -> JobManifest:
        # 1. Extract parameters
        job_id = json_request.get("job_id")
        if not job_id:
            raise ValueError("Job request is missing required 'job_id'")

        params = json_request.get("params", {})
        config_path = Path(params.get("config_path")).expanduser()
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
        print("=" * 60)
        print(f"\n\nNEW JOB REQUEST - create_job_manifest for Job '{job_id}'")

        # 2. Load and resolve job configuration
        print("LOADING JOB CONFIG")
        try:
            render_cfg = self.config_loader(config_path)

        except Exception as e:
            raise ValueError(f"render Config syntax error: {str(e)}")
        try:
            render_cfg.resolve_paths(
                prefix=params.get("prefix", ""),
                build_dir=Path(params.get("build_dir", "build")).expanduser(),
                output_file=params.get("output_file", "output.tif")
            )
        except Exception as e:
            raise ValueError(f"render Config error: {str(e)}")

        # 3. GET HASHES - used to detect what part of config has changed
        hashes = render_cfg.get_hashes()

        # 4. Resolve Resources and Geography
        resources = derive_resources(render_cfg=render_cfg)

        # Resolve output paths
        final_out_path = Path(render_cfg.files["output"])
        temp_out_path = self.build_temp_output_path(final_out_path, job_id)
        render_cfg.files["output"] = temp_out_path

        # Setup preview parameters
        percent = float(params.get("percent", 0.0))
        row_focal = float(params.get("row", 0.0))
        col_focal = float(params.get("col", 0.0))
        envelope: Optional[Window] = None
        write_offset = (0, 0)
        source_metadata = {}

        try:
            with IOManager(render_cfg, resources.sources, resources.anchor_key) as io:
                # Geography Hash (based on paths and mtimes)
                geography_hash = self.generate_region_hash(resources)
                profile = self.build_output_profile(io)

                for dkey in resources.sources:
                    try:
                        src = io.sources[dkey]
                        source_metadata[dkey] = {"width": src.width, "height": src.height}
                    except Exception as e:
                        raise IOError(f"IO err {dkey}: {str(e)}")

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
        except MemoryError as e:
            raise IOError(f"Could not open file: {str(e)}")

        # 5. Add hashes
        resources = resources.with_hashes(
            geography_hash=geography_hash, hashes=hashes
        )

        return JobManifest(
            job_id=job_id, render_cfg=render_cfg, resources=resources,
            final_out_path=final_out_path, temp_out_path=temp_out_path, profile=profile,
            region_id=geography_hash, envelope=envelope, write_offset=write_offset,
            render_params=(percent, row_focal, col_focal), source_metadata=source_metadata
        )

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

        for path in sorted(Path(p).resolve() for p in resources.sources.values()):
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
