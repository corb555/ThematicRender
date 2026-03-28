# engine_resources.py
from contextlib import ExitStack
import multiprocessing as mp
from typing import Dict, List, Optional, Tuple

import numpy as np
from Common.ipc_packets import JOB_ID_IDLE
from Pipeline.job_context import JobContextStore
from Pipeline.rendering_loops import render_loop, reader_loop, writer_loop
from Render.noise_library import NoiseLibrary

from Pipeline.shared_memory import PoolSpec, SharedMemoryPool, SlotRegistry
from Pipeline.system_config import SystemConfig


# engine_resources.py

class EngineResources:
    def __init__(self, engine_cfg: 'SystemConfig'):
        self.engine_cfg = engine_cfg
        self.stack = ExitStack()
        self.system_params = self.engine_cfg.get("system", {})

        # Resource Containers
        self.pool_map: Dict[str, SharedMemoryPool] = {}
        self.output_pool: Optional[SharedMemoryPool] = None
        self.registry: Optional[SlotRegistry] = None
        self.ctx_store: Optional['JobContextStore'] = None

        # TODO NOISE LIBRARY SHOULD NOT BE IN ENGINE RESOURCES
        self.noise_lib: Optional['NoiseLibrary'] = None

        # IPC Queues
        self.status_q = None
        self.read_q = None
        self.work_q = None
        self.writer_q = None
        self.response_q = None

        # Process Handles
        self.reader_procs: List[mp.Process] = []
        self.renderer_procs: List[mp.Process] = []
        self.writer_proc: Optional[mp.Process] = None

    def setup_engine(self) -> None:
        print("[EngineResources] Performing Cold Boot...")
        ctx_mp = mp.get_context("spawn")

        # 1. Initialize Queues
        self.status_q = ctx_mp.Queue()
        self.read_q = ctx_mp.Queue()
        self.work_q = ctx_mp.Queue()
        self.writer_q = ctx_mp.Queue()
        self.response_q = ctx_mp.Queue()

        # 2. Shared Memory Context Side-channel
        self.ctx_store = JobContextStore()
        self.stack.callback(self.ctx_store.cleanup)

        # 3. Pre-allocate  SHM Pools & Partition Registry
        # These are  calculated based on worker count and driver superset
        self._initialize_shm_pools(
            input_slots=self.system_params.get("input_slots"),
            num_renderers=self.system_params.get("renderer_count"),
            num_readers=self.system_params.get("reader_count"),
            buffer_factor=self.system_params.get("transit_buffer_factor")
        )

        # 4. Spawn Workers
        shm_name = self.ctx_store.shm.name

        self.reader_procs = [ctx_mp.Process(
            target=reader_loop, args=(self.read_q, self.status_q, shm_name, self.pool_map),
            name=f"RasterRead_{i}"
        ) for i in range(self.system_params.get("reader_count"))]

        self.renderer_procs = [ctx_mp.Process(
            target=render_loop,
            args=(self.work_q, self.writer_q, self.status_q, shm_name, self.output_pool,
                  self.pool_map), name=f"RasterRender_{i}"
        ) for i in range(self.system_params.get("renderer_count"))]

        self.writer_proc = ctx_mp.Process(
            target=writer_loop, args=(self.writer_q, self.status_q, shm_name, self.output_pool),
            name="RasterWrite_1"
        )

        for proc in self.reader_procs + self.renderer_procs + [self.writer_proc]:
            proc.start()

        print(f"[EngineResources] Workers HOT. SHM Store: {shm_name}")

    def _initialize_shm_pools(
            self, input_slots: int, num_renderers: int, num_readers: int, buffer_factor: float
            ) -> None:
        """
        Allocates physical SHM segments and calculates the Static/Transit partition.
        """
        block_h, block_w = 256, 256
        max_halo = self.engine_cfg.get("system.max_halo", 0)
        pool_h = block_h + 2 * max_halo
        pool_w = block_w + 2 * max_halo

        driver_specs = self.engine_cfg.get("driver_specs", {})
        num_drivers = len(driver_specs)

        # 1. Input Pools (The Physical Contract)
        for drv_key, spec_cfg in driver_specs.items():
            dtype_str = spec_cfg.get("dtype", "float32")
            val_dtype = np.uint8 if dtype_str == "uint8" else np.float32

            spec = PoolSpec(
                data_shape=(1, pool_h, pool_w), data_dtype=np.dtype(val_dtype),
                mask_shape=(1, pool_h, pool_w), mask_dtype=np.dtype(np.float32)
            )

            pool = SharedMemoryPool(spec, input_slots, prefix=f"tr_{drv_key}")
            self.stack.callback(pool.cleanup)
            self.pool_map[drv_key] = pool

        # 2. Output Pool
        out_slots = int(max(16, int(num_renderers * buffer_factor)))
        out_spec = PoolSpec(
            data_shape=(3, 256, 256), data_dtype=np.dtype(np.uint8), mask_shape=(1, 256, 256),
            mask_dtype=np.dtype(np.float32)
        )
        self.output_pool = SharedMemoryPool(out_spec, slots=out_slots, prefix="tr_output")
        self.stack.callback(self.output_pool.cleanup)

        # 3. Calculate Persistent Partitioning
        # This is calculated ONCE based on the superset of drivers in engine.yml
        static_count, transit_count = calculate_shm_partitions(
            total_slots=input_slots, num_renderers=num_renderers, num_readers=num_readers,  # Added
            num_drivers=num_drivers, buffer_factor=buffer_factor
        )

        self.registry = SlotRegistry(
            pool_map=self.pool_map, context_id="boot", static_count=static_count
        )

        # Detailed Memory Report
        print(f"\n[MemoryPlan] Partitioning:")
        load = num_renderers * num_drivers + num_readers
        print(f"   - Total Pool Size:  {input_slots:4} slots per driver ('system.yml:input_slots')")
        print(
            f"   - Transit Highway:  {transit_count:4} slots (Capacity for {buffer_factor}x load)"
            )
        print(
            f"          - Load:             {load} ({num_renderers} Renderers × {num_drivers} "
            f"Drivers + {num_readers} Readers)"
            )
        print(
            f"   - Static Cache:     {static_count:4} slots (Remaining slots pinned for Warm "
            f"Previews)\n"
            )
        print(
            f"   - Output Highway:   {out_slots:4} slots (Write-buffer for {num_renderers} "
            f"Renderers @ {buffer_factor}x)"
            )
        print(f"\n   💡 [Optimization Tips]:")
        if static_count < (input_slots * 0.2):
            print(f"   ⚠️  Warning: Static Cache is very small (<20%). Previews may be slow.")
            print(
                f"      To fix: Increase 'input_slots' in system.yml to at least "
                f"{transit_count + 100}."
                )
        else:
            print(f"   ✅  Static Cache is healthy.")

        if transit_count < (num_renderers * num_drivers):
            print(
                f"   🛑 Critical: Transit Highway is under-sized! Potential for pipeline deadlock."
                )
            print(f"      To fix: Increase 'input_slots' or decrease 'transit_buffer_factor'.")
        else:
            print(f"   ✅  Transit Highway is healthy.")

        print(
            f"   - To change Transit Highway, adjust 'system.yml:transit_buffer_factor' (Current: "
            f"{buffer_factor})"
            )
        print(f"   - Driver Count is sum of entries in driver_specs")
        print("-" * 60 + "\n")

    def manage_noise_library(self, noise_lib: 'NoiseLibrary'):
        """Register the noise library for automatic unlinking on shutdown."""
        self.noise_lib = noise_lib
        # Register the cleanup with the stack (unlink=True because we are the owner)
        self.stack.callback(noise_lib.cleanup, unlink=True)

    def update_context(self, job_id: str, reader_data, worker_data, writer_data):
        """
        Public API for the Orchestrator to update the side-channel.
        This must be called BEFORE dispatching tiles to the queues.
        """
        if not self.ctx_store:
            raise RuntimeError("Engine not initialized.")

        self.ctx_store.write_contexts(job_id, reader_data, worker_data, writer_data)

    def cancel_active_job(self):
        """Interrupts workers by setting the SHM state to -2."""
        if self.ctx_store:
            print(f"🛑 [Engine] Authoritative State: CANCEL (-2)")
            self.ctx_store.set_job_cancel()

    def set_engine_idle(self):
        """Sets the SHM state to -1 (Idle). Workers will release resources."""
        if self.ctx_store:
            self.ctx_store.set_job_id(JOB_ID_IDLE)  # TODO add a generic setter

    def shutdown(self):
        print("[EngineResources] Initiating Graceful Shutdown...")

        # 1. SOFT KILL: Tell all workers via SHM to stop immediately (-3)
        if self.ctx_store:
            try:
                self.ctx_store.set_shutdown()
                print("   - SHM State set to SHUTDOWN (-3)")
            except Exception as e:
                print(f"   ⚠️ Could not set SHM shutdown state: {e}")

        # 2. WAIT: Give workers a brief moment (e.g., 1 second) to see the SHM signal
        # and release their own file locks/resources gracefully.
        import time
        time.sleep(1.0)

        # 3. HARD KILL: Terminate any workers that are deadlocked or didn't listen
        print("   - Terminating remaining worker processes...")
        for proc in self.reader_procs + self.renderer_procs + [self.writer_proc]:
            if proc and proc.is_alive():
                # Try to join them first
                proc.join(timeout=0.5)
                # If still alive, force terminate
                if proc.is_alive():
                    proc.terminate()

        # 4. UNLINK: Close queues and unlink SHM segments via ExitStack
        print("   - Unlinking IPC and Shared Memory...")
        self.stack.close()
        print("✅ [EngineResources] Shutdown Complete.")


def calculate_shm_partitions(
        total_slots: int, num_renderers: int, num_readers: int,  # New parameter
        num_drivers: int, buffer_factor: float = 2.0, max_transit_ratio: float = 0.8
) -> Tuple[int, int]:
    """
    Determines the split between Static Cache and Transit Highway.

    Formula:
    transit_floor = ((Renderers * Drivers) + Readers) * buffer_factor
    """
    # 1. Calculate the 'Worst-Case' Transit Floor
    # Renderers need 'num_drivers' slots each; Readers only need 1 slot each.
    active_demand = (num_renderers * num_drivers) + num_readers
    transit_floor = int(active_demand * buffer_factor)

    # 2. Safety Cap
    max_transit = int(total_slots * max_transit_ratio)
    transit_count = min(transit_floor, max_transit)

    # 3. Final Static Cache Size
    static_count = max(0, total_slots - transit_count)

    return static_count, transit_count
