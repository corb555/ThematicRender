from typing import Tuple

from Pipeline.job_control import JobManifest
from Pipeline.worker_contexts import WriterContext, WorkerContext, ReaderContext
from Render.compositing_engine import CompositingEngine
from Render.factor_engine import FactorEngine
from Render.noise_library import NoiseLibrary
from Render.surface_engine import SurfaceEngine
from Render.theme_registry import ThemeRegistry


class RenderStack:
    """
    Orchestrator-side container for rendering logic.
    Handles persistent engines and packages them into worker contexts.
    """

    def __init__(self):
        # Persistent Math Engines
        self.noise_lib = None
        self.theme_reg = None
        self.factor_eng = None
        self.surface_eng = None
        self.compositor = None

    def init_render_engines(self, render_cfg, resources, eng_resources):
        """Initial bootstrap of all render engines."""
        self.surface_eng = SurfaceEngine(render_cfg)
        self.compositor = CompositingEngine()
        self.theme_reg = ThemeRegistry(render_cfg)

        # 1. Noise System
        self.noise_lib = NoiseLibrary(render_cfg, profiles=render_cfg.noises, create_shm=True)
        eng_resources.manage_noise_library(self.noise_lib)

        # 2. Factor System
        self.factor_eng = FactorEngine(
            render_cfg, self.theme_reg, self.noise_lib, render_cfg.factors, resources, None
        )

    def prepare_job_contexts(
            self, manifest: 'JobManifest'
    ) -> Tuple['ReaderContext', 'WorkerContext', 'WriterContext']:
        """
        Synchronizes engines with the manifest and builds the
        serialization-ready contexts for the workers.
        """
        # 1. Logic Sync: Update engines with current job settings
        self.theme_reg.load_metadata(manifest.render_cfg)
        self.factor_eng.cfg = manifest.render_cfg
        self.surface_eng.cfg = manifest.render_cfg

        # 3. Assemble Reader Context
        reader_ctx = ReaderContext(
            render_cfg=manifest.render_cfg, anchor_key=manifest.resources.anchor_key,
            source_paths=manifest.resources.drivers, job_id=manifest.job_id
        )

        # 4. Assemble Renderer Context
        worker_ctx = WorkerContext(
            render_cfg=manifest.render_cfg, themes=self.theme_reg, compositor=self.compositor,
            pipeline=manifest.render_cfg.pipeline, anchor_key=manifest.resources.anchor_key,
            surface_inputs=manifest.resources.surface_inputs, resources=manifest.resources,
            noise_registry=self.noise_lib, job_id=manifest.job_id
        )

        # 5. Assemble Writer Context
        writer_ctx = WriterContext(
            output_path=manifest.temp_out_path, output_profile=manifest.profile,
            write_offset_row=manifest.write_offset[0], write_offset_col=manifest.write_offset[1],
            job_id=manifest.job_id
        )

        return reader_ctx, worker_ctx, writer_ctx
