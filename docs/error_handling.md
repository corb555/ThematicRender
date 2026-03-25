# Design Spec: Error Handling and Lifecycle Management

## 1. Strategy Overview and Purpose

The Rendering system has a Daemon for producing renders and a client for editing settings and displaying results.
The Daemon may encounter deadlocks, errors and warnings during processing.  This document provides the design for handling errors
in a multiprocess Daemon with a separate client. Errors range from fatal system errors to incorrect parameter settings.
These can result in a warning being sent, a job being cancelled, or the Daemon shutting down.
The primary purpose of this error-handling architecture is to ensure system stability, prevent resource leaks, 
and provide fast and clear feedback to the client, including notifying the client of incorrect parameter settings
or missing files.

### 1.1 Job ID
The Daemon runs one job (render) at a time.  It stores the current job_id in 
  Shared Memory `job_id` as the single point of truth. This allows the system to synchronize state across 
multiple processes without the need for complex broadcast signals.  Every time  a task removes a queue item,
it checks the Shared Memory `job_id` and uses the job_id to handle each message appropriately. The orchestrator
receives validates the job_id in received messages where appropriate.

| Job Id | Usage         |
|--------|---------------|
| -3     | SHUTTING_DOWN |
| -2     | JOB_CANCELLED |
| -1     | IDLE          |
| >=0    | Valid Job     |


### 1.2 Shutdown and Cancellation Strategies
Because the Daemon uses multiple processes, there are challengs in synchronizing all the processes.  
Two different strategies are used for shutdown versus job
cancellation to minimize the complexities of synchronization.
The **Daemon** employs a "Passive Invalidation" strategy for job cancellation and 
an "Active Poisoning" strategy for system shutdown. 

### 1.3 Severity Mapping
All errors are propagated from tasks to the Orchestrator via an `ErrorPacket`. The Orchestrator acts based on the assigned severity:
*   **Warning:** Logged and sent to the client; the job continues.
*   **Cancel:** Triggers a Job Cancellation.
*   **Fatal:** Triggers a full system Shutdown.

Components generally detect errors from raised exceptions.

* Warning - A rendering setting which may not be ideal but will produce an accurate raster.  These are directly
handled by the rendering component and sent as warnings to the Client.
* Job Cancel - An error which will prevent correct building of the raster but not impact system integrity. Components
will generally detect these from FileNotFound and ValueError exceptions and send those as Critical to the Orchestrator.
* Fatal - The system cannot continue to process data. Any exception not included above will be assumed to be a
fatal error.


### 1.4 Orchestrator State Machine

| State                         | Event                              | New State         |
|-------------------------------|------------------------------------|-------------------|
| Initial                       |                                    | **IDLE**          |
| **IDLE**                      | `Op.JOB_REQUEST`                   | **RUNNING**       |
| **RUNNING**                   | `Op.JOB_CANCEL` (User)             | **CANCELLING**    |
| **RUNNING**                   | `Op.ERROR (Sev 1)` (Logic Error)   | **CANCELLING**    |
| **RUNNING**                   | `Op.TILES_FINALIZED` (Success)     | **IDLE**          |
| **CANCELLING**                | `Op.WRITER_ABORTED` (Cleanup Done) | **IDLE**          |
| **ANY**                       | `Op.SHUTDOWN`                      | **SHUTTING_DOWN** |
| **ANY**                       | `Op.ERROR (Sev 0)` (Fatal)         | **SHUTTING_DOWN** |
| **RUNNING** or **CANCELLING** | `Watchdog Timeout`                 | **SHUTTING_DOWN** |
| **SHUTTING_DOWN**             | `Process Join `                    | **EXIT**          |
| **SHUTTING_DOWN**             | `Join Timeout`                     | **EXIT**          |

---

## 2. Pipeline Orchestrator (The Authority)
The Orchestrator manages the global state and coordinates the cleanup of the pipeline.

**2.1 Shutdown (Severity 0 / System Command)**
1.  **Transition:** Move to **SHUTTING_DOWN** state.
2.  **State Update:** Sets Shared Memory `job_id` to `"SHUTTING_DOWN"`.
3.  **Poison Pill Distribution:** Queues `Op.SHUTDOWN` to Readers, then Workers, then Writer.
4.  **Wait:** Orchestrator waits for worker processes to `join()` with a 5-second timeout.
5.  **Final Cleanup:** Unlinks all system-level SHM segments (Registry and Noise).
6.  **Client Notification:** Sends `system_offline`.

**2.2 Job Cancel (Severity 1 / User Command)**
1.  **Transition:** Move to **CANCELLING** state.
2.  **State Update:** Sets Shared Memory `job_id` to `"JOB_CANCELLED"`.
3.  **Writer Notification:** Queues `Op.JOB_CANCEL` to `writer_q`.
4.  **Resource Reclamation:** Calls `dispatcher.abort_job()` immediately to release Registry slots in the Orchestrator's memory.
5.  **The Wait (Handshake):** The Orchestrator continues its `run_loop` but **blocks** `_start_next_job` while in the **CANCELLING** state.
6.  **Resolution:** Upon receiving `Op.WRITER_ABORTED`, transition back to **IDLE** and trigger `_start_next_job` if there are pending jobs in the queue.

**2.3 Heartbeat Monitor**
Since the pipeline is designed for high throughput (tiles every ~60ms), a total lack of messages while a job is 
active is a definitive sign of failure.

*   **Last Activity Timer:** The Orchestrator resets a `last_message_ts` every time it receives *any* valid message 
from the `status_q` .
*   **The Threshold:** If the state is `RUNNING` or `CANCELLING` and the timer exceeds a threshold, 
a deadlock is declared.
*   **The Reaction:** A deadlock is always **Severity 0 (Fatal)**. If the pipeline has stopped moving, the internal 
state of the Processes, Queues or SHM is likely compromised, and a full system restart is the only safe recovery.

**2.4 Client Messages**
---

## 3. Reader Process (Passive Input)
Readers are responsible for stopping the flow of raw data into the system.

### 3.1 Shutdown
1.  **Detection:** Receives `Op.SHUTDOWN` from `reader_q`.
2.  **Action:** Closes all process-local Rasterio dataset handles.
3.  **Exit:** Terminates the process immediately.
Note: because it stops reading the queue upon Op.SHUTDOWN message, other queued shutdown messages will go to
the remaining readers.

### 3.2 Job Cancel
1.  **Detection:** On every `read_q.get()`, the Reader compares the packet `job_id` against the Shared Memory `job_id`.
2.  **Action:** If the packet Job ID doesn't match the current SHM ID, the Reader discards the packet.
3.  **Recovery:** The process remains alive and returns to the queue to wait for a valid `job_id`.
Note: when the Reader receives the next LOAD_BLOCK, it will use the context to reinitialize all IO.

---

## 4. Render Process (Passive Compute)
Renderers are responsible for stopping heavy CPU tasks for invalid jobs.

### 4.1 Shutdown
1.  **Detection:** Receives `Op.SHUTDOWN` from `work_q`.
2.  **Action:** Discards transient NumPy arrays and local Workspace engines.
3.  **Exit:** Terminates the process immediately.
Note: because it stops reading the queue upon Op.SHUTDOWN message, other queued shutdown messages will go to
the remaining renderers.

### 4.2 Job Cancel
1.  **Detection:** On every `work_q.get()`, the Renderer compares the packet `job_id` against the Shared Memory `job_id`.
2.  **Action:** If the `job_id` doesn't match the current SHM ID, the Renderer discards the packet.
3.  **Recovery:** The process remains alive and returns to the queue to wait for a valid `job_id`.

---

## 5. Writer Process (Active Cleanup)
The Writer is the sole process responsible for managing the physical integrity of the output file.

### 5.1 Shutdown
1.  **Detection:** Receives `Op.SHUTDOWN` from `writer_q`.
2.  **Action:** 
    *   Flushes any pending buffers to disk.
    *   Closes the Rasterio destination handle.
    *   Unlinks (deletes) the current `.tmp` file to prevent stale artifacts.
3.  **Exit:** Terminates the process.

**5.2 Job Cancel**
1.  **Detection:** Receives `Op.JOB_CANCEL`.
2.  **Action:** 
    *   Immediately closes the Rasterio dataset handle.
    *   Unlinks (deletes) the `.tmp` file.
    *   **Crucial:** Does not flush buffers; it prioritizes immediate deletion of the invalid file.
3.  **Handshake:** Sends `Op.WRITER_ABORTED` back to the Orchestrator.
4.  **Recovery:** Returns to the queue.

---

## 6. Client Communication (Outgoing JSON)
The following messages are dispatched to the client proxy during error/lifecycle events:

*   **Job Completion:**
    `{"msg": "complete", "job_id": "...", "path": "...", "duration": 1.23}`
*   **Job Progress (Heartbeat):**
    `{"msg": "progress", "request_id": "...", "progress": 85, "message": ""}`
*   **Job Error/Cancellation:**
    `{"msg": "error", "job_id": "...", "message": "Stage failure: [Reason]"}`
*   **System Failure:**
    `{"msg": "error", "job_id": "system", "message": "Fatal system error: [Traceback Summary]"}`
*   **System Shutdown:**
    `{"msg": "system_offline", "message": "Daemon shutting down."}`

## Implementation Phases

### Phase 1: The Foundation (States & IDs)
**Goal:** Establish the authoritative state machine and the integer-based Job ID logic.

1.  **Update `JobContextStore`:** Implement the logic to store and retrieve the integer Job IDs (-3 to N) in Shared Memory.
2.  **Formalize Orchestrator States:** Add a `self.state` variable to `PipelineOrchestrator` using an `Enum` (IDLE, RUNNING, CANCELLING, SHUTTING_DOWN).
3.  **Passive Invalidation:** Update the `sync_ctx_for_packet` utility (used by all workers) to strictly compare packet IDs against the current SHM ID. 
    *   *Verification:* Ensure that if you manually set SHM to -1, all Workers immediately start draining/ignoring their queues.

### Phase 3: The Active Handshake (Job Cancellation)
**Goal:** Prevent file locks and Shared Memory slot leaks during a "Cancel" event.

1.  **Orchestrator `abort_job`:** Implement `dispatcher.abort_job()` to iterate through `active_tiles` and call `registry.release()` for every slot. This ensures RAM is freed even if the worker never finishes the tile.
2.  **Writer Cleanup:** Update the Writer to handle `Op.JOB_CANCEL`. It must close the file handle, delete the `.tmp` file, and send `Op.WRITER_ABORTED`.
3.  **Orchestrator CANCELLING State:** Update `_start_next_job` to block if `state == CANCELLING`. Update the loop to transition to `IDLE` only when `WRITER_ABORTED` is received.
    *   *Verification:* Rapidly clicking "Render" then "Cancel" in the Editor should not result in "File in Use" errors or orphaned `.tmp` files.

### Phase 4: The Watchdog (Deadlock Detection)
**Goal:** Ensure the system never hangs indefinitely.

1.  **Activity Tracking:** Add `self.last_activity_ts` to the Orchestrator. Update it on every valid message receipt and every successful job start.
2.  **Watchdog Logic:** In the `run_loop`'s `Empty` exception block (the 0.05s timeout), add the comparison logic. If the threshold is exceeded while `RUNNING`, trigger a Severity 0 error.
3.  **Severity 2 (Warnings):** Implement the logic to pass non-fatal `ErrorPackets` to the client without stopping the pipeline.
    *   *Verification:* Simulate a deadlock (e.g., tell a worker to `sleep(60)`). The Orchestrator should detect the silence and initiate a shutdown/restart.

### Phase 5: Active Poisoning (System Shutdown)
**Goal:** Clean exit and OS resource reclamation.

1.  **Poison Pill Distribution:** Implement the `_handle_shutdown` method to set SHM to -3 and distribute the `Op.SHUTDOWN` envelopes in the correct order (Readers $\rightarrow$ Workers $\rightarrow$ Writer).
2.  **Process Join:** Add the `join()` logic with timeouts to the Orchestrator to ensure child processes are reaped.
3.  **SHM Unlink:** Ensure the final step of the Orchestrator's life is unlinking the `SlotRegistry` and `NoiseLibrary` segments so the OS stays clean.
    *   *Verification:* Running the `shutdown` command should result in a clean terminal exit with no "shared memory segment already exists" errors on the next start.