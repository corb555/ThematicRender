Here is a concise summary for this new session with the technical context of the **Thematic Rendering Daemon**.

---

## **Project Context: Thematic Rendering Daemon**

A high-performance background service for multi-layer spatial compositing. It uses a **Client/Daemon** architecture over
a Unix Domain Socket,
designed to bridge the gap between heavy GIS data and real-time artistic iteration. The Daemon has two major components:
1) the Pipeline Engine/Orchestrator and

2) The Rendering Subsystem. The Pipeline Engine/Orchestrator reads the config, creates and manages shmem, queues, and
   processes, manages handoffs between the rendering processes.
   The Rendering Subsystem has three pieces: The Reader reads in required data, The Renderer follows config instructions
   to create and blend layers, The Writer outputs
   the final blocks to disk. The Renderer should be unaware of the multiprocessing, shmem, and queueing. The pipeline is
   generally unaware of rendering details.
   The Daemon is approximately 38 Python modules each with 100 to 1000 lines of code.

## **Pipeline Overview **

* **Persistent Worker Pool:** Bypasses Python’s 8-second `multiprocessing.spawn` tax. Workers (Readers, Renderers,
  Writer) stay alive for the life of the service.
* **Zero-Copy SHM Pipeline:** Data flows from disk to GDAL to Shared Memory slots using precise slicing (
  `out=shm_buffer[0, :h, :w]`). This
  eliminates NumPy copies and allows ~60ms/tile throughput.
* **Config Driven Pipeline:** Engines (Logic) are separated from Config (YAML/QML). Workers re-instantiate math engines
  locally only when configuration hashes change.
* **Granular Hashing:** The system tracks three distinct hashes to trigger surgical engine rebuilds:
    1. **Geography Hash:** (Files/Paths) Triggers SHM Registry purges.
    2. **Logic Hash:** (Factor Math/YAML) Triggers `FactorEngine` rebuilds.
    3. **Style Hash:** (Colors/QML/Pipeline) Triggers `SurfaceEngine` and LUT updates.

## **Rendering Overview **

### 1. Compositing Pipeline

The system is driven by a user configured compositing sequence:

* **Dynamic Data Drivers:** Ingest any raster data (DEM, Precipitation, Forest Height, Lithology) and
  define custom transformation policies.
* **Logical Blending:** A library of atomic operations including `lerp_surfaces`, `alpha_over`, and
  `lerp_buffers` for sophisticated transitions.
* **Programmable Shading:** Multiplicative hillshade application with highlight/shadow protection logic to
  preserve color vibrancy and "clean" midtones in topographic relief.

### 2. Factor Library

Factors act as the control signals for the entire render, conditioned from physical drivers into 0..1 masks:

* **Apparent Elevation Logic:** Procedural displacement of elevation and moisture inputs to create natural,
  wandering biome contacts and eliminate artificial "bathtub rings."
* **Sensitivity-Weighted Shaping:** Per-factor Scale, Bias, Contrast, and Sensitivity (Gamma) controls to
  tune the "threshold" and "lushness" of environmental transitions.
* **Topological Precedence:** A priority-based "Melt and Claim" system that ensures high-precedence
  features (like Water or Rock) correctly carve through lower-precedence materials (like Forest).

### 3. Surface Library

Surfaces are the "Materials" of the map, synthesized as RGB data through various providers:

* **Ramp Synthesis:** Samples 1D color ramps using physical meters (`elev_m`) for sub-pixel precision,
  eliminating the banding and data loss of traditional 8-bit normalization.
* **Hue Perturbation (Mottle):** Noise-driven RGB hue shifting that adds tactile "surface tooth" and
  geological grit to otherwise flat digital gradients.
* **Geometry Cleanup:** Discrete categorical and continuous data are **materialized** into smooth probability fields,
  turning blocky 30m pixels into elegant, rounded curves.
* **Synthesis & Naturalization:** High-frequency procedural noise is injected to create "clumpy" vegetation, "patchy"
  mineral deposits, and "rippling" water surfaces, simulating the non-uniformity found in nature.

### 4. Noise Library

The engine features a highly configurable noise library for creating unique organic textures:

* **Weighted Gaussian Sigmas:** Define noise profiles using multiple scales of Gaussian-filtered white noise.
  This allows for the blending of high-frequency "grit" and "fuzz" with large, sweeping organic masses.
* **Anisotropic Stretch:** Apply directional stretch parameters to simulate flowing water, sedimentary rock
  layers, or wind-swept vegetation patterns.
* **Real-time Calibration:** Designed for iterative design, allowing users to tune frequency response and
  weights to match specific geological or biological characteristics.

# Sample pipeline:

```YAML
pipeline:
  - desc: Mix ARID_BASE and ARID_RED_BASE using lithology data
    enabled: true
    comp_op: lerp_surfaces
    factor_nm: lith
    input_surfaces: [arid_base, arid_red_base]
    output_surface: arid_composite

  - desc: Create the Canvas buffer with the ARID_COMPOSITE surface
    enabled: true
    comp_op: create_buffer
    input_surfaces: [arid_composite]

  - desc: Diagnostic Blackboard
    enabled: false
    comp_op: create_buffer
    buffer: canvas
    params:
      color: [214, 212, 195]  

  - desc: Add ARID_VEGETATION to the arid region using the forest mask
    enabled: true
    comp_op: lerp
    factor_nm: forest
    input_surfaces: [arid_vegetation]
    scale: 1.1
    contrast: 0.0

  - desc: Mix HUMID_BASE and ARID_RED_BASE using lithology data
    enabled: true
    comp_op: lerp_surfaces
    factor_nm: lith
    scale: 1.0
    input_surfaces: [humid_base, arid_red_base]
    output_surface: humid_composite
```

Process Topology:

- 1 Orchestrator
- N Reader processes
- N Renderer/Worker processes
- 1 Raster Writer

Core Model:
Rendering is tile-based and highly parallel. Each tile may depend on
multiple input driver blocks, and a tile cannot be rendered until all of
its required inputs have been loaded. Because tiles are independent once
their inputs are ready, they may be rendered and written in any order.

When a job begins, the Orchestrator publishes the active job context and
rendering parameters into shared memory, including the current ``job_id``.
Every queue packet carries both an operation code and a ``job_id``. The
``job_id`` increases monotonically for the lifetime of the daemon.

On every queue read, each process validates that the packet ``job_id``
matches the active ``job_id`` stored in shared memory. Packets for older or
invalid jobs are discarded immediately.

Design Notes:

- Only one job is active at a time.
- A job may contain thousands of tiles.
- Tiles may be rendered and written out of order.
- Input blocks are coordinated through queue messages, while bulk tile data
  and job context live in shared memory.
- ``job_id`` validation is the primary safeguard against stale packets from
  earlier jobs.
- rendering is entirely controlled by biome.yml settings.
- the rendering system consists of multiple engines with feature libraries
- there are 4 major engines with feature libraries: factor_engine, compositing_engine, noise_library, surface_engine
- for example, the compositing engine launches actions from the library: create_buffer, lerp,
  multiply, alpha_over, apply_zonal_gradient, write_output

Execution Flow - Happy Path:

1. The client sends ``JOB_REQUEST`` to the Orchestrator.
2. The Orchestrator queues incoming requests and starts the next job when
   idle.
3. The Orchestrator resolves the request into a job manifest and publishes
   the job context into shared memory.
4. Any work packet whose ``job_id`` does not match the shared-memory job
   context is ignored.
5. The Dispatcher primes the pipeline with up to ``max_in_flight`` tiles.
6. For each tile, the Dispatcher emits one or more ``LOAD_BLOCK`` packets
   to Reader processes.
7. Readers load the requested input blocks and emit ``BLOCK_LOADED``.
8. The Dispatcher maintains per-tile dependency state.Once all required blocks for a tile are available, the Dispatcher
   marks
   that tile ready for rendering.
9. The Orchestrator sends the tile to a Renderer as ``RENDER_TILE``.
10. The Renderer produces the final image tile and emits ``WRITE_TILE``.
11. The Writer writes the tile to the output file and emits
    ``TILE_WRITTEN``.
12. The Orchestrator releases the tile's resources and dispatches the next
    tile.
13. The Orchestrator counts ``TILE_WRITTEN``. Once that count matches the total for the
    active job, it sends ``JOB_DONE`` to the Writer.
14. The Writer flushes and closes the output, then emits
    ``TILES_FINALIZED``.
15. The Orchestrator marks the job complete and may begin the next queued
    job.

### **Status**

Multiprocessing Daemon and Editor Client are functional.

### **Roadmap**

We are currently in a "Tock" phase, 1) implementing an error handling strategy, and 2) improving the YAML settings
processing

#### YAML Settings:

* The system properly handles most changes to the YML config, rebuilding appropriate resources.
* The last remaining item is to handle changes to the pipeline which require new resource settings.

#### Error Handling:

THere is a detailed design spec for this.

* **Phase 0 (Visibility): DONE** Forwarding `ErrorPackets` from workers to the Client UI.
* **Phase 1 (Foundation): DONE** Authoritative Job IDs in SHM (`-3` Shutdown, `-2` Cancel, `-1` Idle, `N` Active).
* **Phase 2 (Active Handshake):** Orchestrator `CANCELLING` state; Writer acknowledges with `WRITER_ABORTED` after
  unlinking `.tmp` files. Add States in pipeline.
* **Phase 3 (Watchdog):** Deadlock detection with tiered timeouts (Worker 7s / Orchestrator 10s).
* **Phase 4 (Active Poisoning):** Sequential Shutdown via "Poison Pills" and SHM unlinking.


