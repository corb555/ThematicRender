The **Thematic Rendering Daemon** is a high-performance background service for
multi-layer spatial compositing. A separate client application manages render
settings and notifies the daemon over a Unix domain socket whenever a new render
is requested.

Overview:

1. **Throughput:** The daemon is designed for throughput. It uses multiple processes for disk
   reads and tile rendering, with queues carrying control packets and shared
   memory holding tile data and job context. Only one render job is active at a
   time, but a single job may contain thousands of tiles.

2. **Persistent workers to avoid startup latency:** Python multiprocessing with the `spawn` method imposes substantial
   startup overhead,
   approximately 8 seconds. The daemon starts worker processes once during service startup and keeps them alive for the
   life of the service. This avoids
   per-job startup cost and allows the daemon to respond quickly to new render requests. This matters especially for
   small preview renders, which may
   take only 1 to 2 seconds to execute.

3. **Shared-memory caching:** GIS inputs such as DEM and lithology datasets are large and expensive to reload. A
   persistent
   **Shared Memory (SHM) Registry** lets the daemon retain loaded raster blocks across render passes. When a user
   changes styling within the same
   region, the daemon can reuse raster data already in memory instead of rereading it from disk. The cache is
   invalidated for a new job if
   the hash of the input filenames and file times change.

4. **Message Control** All messages have a message ID. All Job messages have a Job-ID.

---

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

---

```mermaid
flowchart TD
A[Client<br/>JOB_REQUEST]
B[Orchestrator<br/>Queue / start job]
C[Orchestrator<br/>Manifest + SHM context]
D[All Processes<br/>Reject stale job_id]

    subgraph TILE_LOOP [Per-Tile Loop]
        E[Dispatcher<br/>Prime tiles]
        F[Dispatcher<br/>LOAD_BLOCK]
        G[Readers<br/>BLOCK_LOADED]
        H[Dispatcher<br/>Tile ready]
        I[Orchestrator<br/>RENDER_TILE]
        J[Renderer<br/>WRITE_TILE]
        K[Writer<br/>TILE_WRITTEN]
        L[Orchestrator<br/>Release tile / next tile]
        E --> F --> G --> H --> I --> J --> K --> L --> E
    end

    M[Orchestrator<br/>JOB_DONE]
    N[Writer<br/>TILES_FINALIZED]
    O[Orchestrator<br/>Finish job / next queued]

    A --> B --> C --> D --> E
    L --> M
    M --> N --> O
   ```

