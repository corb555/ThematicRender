# engine_resources.py
from contextlib import ExitStack
import multiprocessing as mp
from typing import Dict, List, Optional

import numpy as np

from ThematicRender.job_context import JobContextStore
from ThematicRender.keys import DriverKey
from ThematicRender.noise_library import NoiseLibrary
from ThematicRender.reader_task import reader_loop
from ThematicRender.render_task import render_loop
from ThematicRender.shared_memory import PoolSpec, SharedMemoryPool, SlotRegistry
from ThematicRender.system_config import SystemConfig
from ThematicRender.writer_task import writer_loop


# engine_resources.py

class EngineResources:
    def __init__(self, engine_cfg: 'SystemConfig'):
        self.engine_cfg = engine_cfg
        # noinspection PyAbstractClass
        self.stack = ExitStack()
        self.system_params = self.engine_cfg.get("system", {})

        # Resource Containers
        self.pool_map: Dict['DriverKey', 'SharedMemoryPool'] = {}
        self.output_pool: Optional['SharedMemoryPool'] = None
        self.registry: Optional['SlotRegistry'] = None
        self.ctx_store: Optional[JobContextStore] = None
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
        # Create  and register for cleanup
        self.ctx_store = JobContextStore()
        self.stack.callback(self.ctx_store.cleanup)

        # 3. Pre-allocate Raster SHM Pools
        self._initialize_shm_pools(self.system_params.get("input_slots"))

        # 4. Spawn Workers (Pass the SHM name so they can attach)
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

    def shutdown(self):
        print("[EngineResources] Shutting down...")
        for proc in self.reader_procs + self.renderer_procs + [self.writer_proc]:
            if proc and proc.is_alive():
                proc.terminate()
        self.stack.close()

    def _initialize_shm_pools(self, input_slots: int, out_slots=16) -> None:
        """Determines required memory footprint and allocates SHM segments.
        :param out_slots:
        """
        # This logic is extracted from the previous monolithic setup_engine
        # We use the config to sense the required halo and data types.

        # Use a standard 256x256 block size for the pools
        block_h, block_w = 256, 256
        max_halo = self.engine_cfg.get("system.max_halo")
        pool_h = block_h + 2 * max_halo
        pool_w = block_w + 2 * max_halo

        print(f"Allocating Input Slots: {input_slots} per driver")

        # 1. Input Pools (DEM, Forest, etc.)
        for drv_key in self.engine_cfg.get("driver_specs"):
            dtype = self.engine_cfg.get(f"driver_specs.{drv_key}.dtype")
            val_dtype = np.uint8 if dtype == "uint8" else np.float32

            spec = PoolSpec(
                data_shape=(1, pool_h, pool_w), data_dtype=np.dtype(val_dtype),
                mask_shape=(1, pool_h, pool_w), mask_dtype=np.dtype(np.float32)
            )

            pool = SharedMemoryPool(spec, input_slots, prefix=f"tr_{drv_key}")
            self.stack.callback(pool.cleanup)
            self.pool_map[drv_key] = pool

        # 2. Output Pool (Fixed 256x256 RGB for the compositor)
        out_spec = PoolSpec(
            data_shape=(3, 256, 256), data_dtype=np.dtype(np.uint8), mask_shape=(1, 256, 256),
            mask_dtype=np.dtype(np.float32)
        )
        self.output_pool = SharedMemoryPool(out_spec, slots=out_slots, prefix="tr_output")
        self.stack.callback(self.output_pool.cleanup)
        print(f"Allocating Output Slots: {out_slots} ")

        # 3. Persistent Registry
        # We boot with a "boot" context ID until the first render request arrives
        self.registry = SlotRegistry(
            self.pool_map, context_id="boot", static_ratio=self.system_params.get("static_ratio")
        )
