## Project Summary

This project is a high-performance spatial compositing engine for generating natural-looking raster imagery from GIS
data.
It combines procedural noise, multi-scale Gaussian filtering, and explicit configuration-driven pipelines to synthesize
visually rich terrain and land-cover surfaces. A configurable blend pipeline defines how driver rasters are loaded,
transformed, and composited into the final image.

## Design

**Explicit configuration**
All processing steps, parameters, and layer definitions are driven by a centralized configuration structure.
Configuration
is fully explicit, and configuration errors are treated as fatal rather than silently falling back to defaults.

**Engine/library pattern**
The system follows an engine/library architecture, with separate components for factors (alpha layers), noise, surfaces,
themes, and compositing.

**Storage model**
Raster blocks are stored in shared memory as 4D arrays in the form:

`(slot, band, height, width)`

**Caching**
The block cache uses a fill-once policy. During the first sequential pass for a file set, blocks are admitted until
cache capacity is
reached. After that, cache membership is frozen and remains valid across reruns. The cache is only invalidated when the
file set changes.
LRU is not effective for this workload because sequential reruns do not exhibit useful temporal locality. At the end of
a
pass, LRU retains the most recently visited blocks from the tail of the raster, but the next pass restarts at the
head, so those cached blocks are evicted before they are reached again.

**Cache ID**

The pipeline operates on distinct regional file sets, such as Sedona, Yosemite, or Yellowstone. For each region, it
typically reads a group of driver files with identical extent, resolution, and tile layout. Cache contents are valid
only within the context of a single region and must be invalidated when the active region changes. For repeated runs
within the same region, the cache remains valid. The cache id will be a hash of sorted driver paths.

**Block ID**

Files within a region are tiled, and the pipeline may operate on either the complete tile set or a sampled subset. The
cache key for a block must therefore be based on the tile’s stable identity within the region, not on its position
within the current run. This ensures that the same tile maps to the same cache entry in both full and partial runs.
The tile loop generates a spatial_key for the Registry based on the window's pixel offsets (e.g., (key, x, y))

**Compute model**
A `render_task` acts as a strict boundary between storage and computation. It rehydrates shared-memory data and converts
it into 2D `(height, width)` arrays before passing them to the rendering engines.

**IPC model**
`WorkPacket` and `ResultPacket` carry only metadata and integer `slot_id` references. No raw NumPy arrays are pickled
or passed between processes.

**Process model**
The system uses a three-stage multiprocessing pipeline:

* **Main process:** coordinator
* **Reader process:** loads raster blocks into shared memory
* **Render processes:** N worker processes perform CPU-intensive compositing
* **Writer process:** a single process that lazy-opens the output in `r+` mode

## Engine Design

**Factor Engine**
The Factor Engine is an orchestration layer that follows a mapped-signal pattern:

`normalize -> blur -> noise -> contrast`

It produces factor rasters that act as alpha masks or control layers throughout the pipeline.

**Surface Engine**
The Surface Engine synthesizes RGB blocks from ramps and modifiers such as mottle. Procedural textures use MD5-stable
`hashlib` offsets so that patterns remain aligned across tile boundaries without visible seams.

**Theme Registry**
The Theme Registry handles categorical rasters using a precedence-based “melt and grow” expansion algorithm. It also
builds RGB lookup tables from QML styles.

**Config Manager**
The Config Manager acts as the single source of truth. In the main process, it primes metadata for a Markdown-based
Pipeline Audit report and then serializes the worker context for the render pool.

## Performance

**Current bottlenecks**

1. **Startup tax** from Python process spawning and library imports
2. **Single-reader constraint** from relying on only one reader process

## Compositing Operations

The compositing system is built around a registry of small, atomic spatial operations. Each operation is registered with
explicit metadata describing its required inputs, attributes, and parameters, allowing the pipeline to validate
configuration before execution.

At runtime, the compositing library applies a sequence of operations to RGB surfaces and intermediate buffers, using
factor rasters as spatial control masks. Core operations include:

* **Buffer creation** to initialize working buffers from source surfaces
* **Surface-to-surface interpolation** to blend major palettes such as arid and humid layers
* **Surface-to-buffer interpolation** to progressively build the final composite
* **Factor-based multiply** to darken or modulate an existing buffer
* **Alpha-over compositing** to place one surface over another using a factor as opacity
* **Buffer-to-buffer interpolation** to merge intermediate results
* **Specular highlight addition** to add controlled reflected-light effects
* **Final output write** to publish the completed buffer as the render result

## Sample Natural Raster Pipeline

The system is highly configurable but as an example, a sample
raster could be built from four primary palettes:

* `arid_base`
* `arid_vegetation`
* `humid_base`
* `humid_vegetation`

Arid and humid surfaces are blended using a precipitation factor.
Base and vegetation surfaces are blended using a forest-canopy factor.
Additional thematic layers are derived from the USGS Landfire categorical raster, including water, glacier,
outwash, volcanic, rock, and playa.  
All layers can have various noise patterns and edge smoothing applied

### Tech Sprints

Refine map rendering with each sprint

* **Validation:** By tuning the map between sprints, we catch "Logic Regressions" (e.g., if a refactor accidentally
  flattens the Gamma curve).
* **Feature Discovery:** Identify new feature requirements with each sprint.

#### Sprint 1: Parallel Readers (CURRENT SPRINT)

* **Goal:** Break the 12-second wall.
* **Impact:** This utilizes the Mac Studio’s SSD bandwidth and CPU cores to handle the "Decompression Tax."
* **Performance:**  Currently all pieces are multiprocessor except Readers. This is the last step to
  a fully multiprocessor system
* Hazards: 1) Ensure queues/shmem are returned to free state at the right time. 2) Ensure completion and shutdown are
  correct

3) Ensure block dimensions are correct at every step. 4) Ensure we are not creating blockers for LRU and for Daemon
   phases.

#### Sprint 2: Settings Migration

* **Goal:** Replace `settings.py` with biome.yml
* **Impact:** This decouples the settings from the code.

#### Sprint 3: The Hot Server (Goal 3: The < 2s Loop)

* **Goal:** Kill the "Launch Tax."
* **Impact:** This is the most significant change in "Feel." The engine becomes a **Daemon**.
* **Workflow:** We save a file $\rightarrow$ the map updates instantly. No more waiting 8 seconds for Python to start.

#### Sprint 4: The GUI (The "Instrument")

* **Goal:** GUI for settings with near real-time feedback.
* **Impact:** This transforms the engine into a **Creative Instrument**. Moving a slider and seeing a canyon wall
  change color immediately is where the true artistic breakthrough happens.
* This will likely be split into multiple sprints

