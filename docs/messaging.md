# Rendering Daemon Messaging and Control Design

## Purpose

This document defines the control-plane design for the Rendering Daemon. It focuses on how the daemon
communicates with the Editor, how commands and responses flow through the system, and how overall
daemon state is managed.

This document does not define raster algorithms, worker internals, cache implementation details, or
Editor GUI behavior beyond control messaging.

## Goals

The design should provide:

* a simple and robust local messaging path between the Editor and the daemon
* a clear separation between socket I/O and render orchestration
* a single owner for overall daemon state
* worker processes that remain isolated from Editor-specific protocols
* a structure that is easy to debug, extend, and reason about

## High-Level Architecture

The daemon consists of:

* one **Coordinator** running in the main process
* worker processes for:

    * **Reader**
    * **Renderer**
    * **Writer**
* two helper threads in the main process:

    * **Command_rcv**
    * **Command_rsp**

The Coordinator is the central orchestrator. It is the sole owner of overall daemon state and is the only component that
makes decisions about request acceptance, request lifecycle, batch execution, and subsystem coordination.

The worker processes perform rendering work only. They have no knowledge of the Editor, socket protocol, or JSON message
format.

The two command threads act only as proxies between the socket and the Coordinator.

## Core Design Principles

### Coordinator owns all overall state

The Coordinator is the only component that owns and mutates overall daemon state.

This includes:

* whether the daemon is idle or busy
* the currently active request
* render request lifecycle
* subsystem configuration lifecycle
* request acceptance, rejection, or cancellation decisions
* generation of Editor-facing responses

No thread or worker process other than the Coordinator may modify overall state.

### Socket threads are proxies only

`Command_rcv` and `Command_rsp` are adapters between the socket and the Coordinator. They do not perform orchestration.

Their responsibilities are intentionally narrow:

* `Command_rcv` receives socket messages, performs straightforward syntax validation, and forwards normalized commands
  to the Coordinator
* `Command_rsp` receives Coordinator-generated responses and writes them to the socket

They do not decide scheduling policy, request policy, or subsystem behavior.

### Workers are protocol-blind

Reader, Renderer, and Writer workers operate only on internal envelopes and work packets. They do not know that an
Editor exists, and they do not know anything about socket format, JSON structure, or request/response protocol.

This keeps the render pipeline isolated from GUI and IPC concerns.

## Components

## Coordinator

The Coordinator runs in the main process and acts as the central event loop and control authority.

### Responsibilities

The Coordinator shall:

* start and manage worker processes
* start and manage `Command_rcv` and `Command_rsp`
* own all overall daemon state
* receive inbound events from `status_queue`
* interpret commands received from the Editor
* validate request semantics beyond syntax
* decide whether requests are accepted, rejected, deferred, or cancelled
* dispatch work and control messages to subsystem queues
* generate all Editor-facing response messages
* send those responses to `command_rsp_queue`

### Non-responsibilities

The Coordinator shall not:

* block directly on socket reads
* write directly to the Editor socket
* perform heavy raster processing

## Command_rcv

`Command_rcv` is a helper thread in the main process.

It is the only component allowed to read from the Editor socket.

It is also the only component allowed to replace or close that socket.

### Responsibilities

`Command_rcv` shall:

* block on socket receive
* parse incoming messages
* perform straightforward syntax validation
* normalize valid messages into internal command envelopes
* forward those envelopes into `status_queue`
* detect socket disconnects and socket-read failures
* forward such failures into `status_queue`
* must use os.unlink(path) before binding

### Validation scope

`Command_rcv` shall perform only lightweight syntax validation, such as:

* message is parseable JSON
* required top-level fields are present
* basic field types are structurally plausible

`Command_rcv` shall not decide whether a request is valid in the broader orchestration sense. That remains the
Coordinator’s responsibility.

### Ownership rule

`Command_rcv` is the only component allowed to:

* read from the socket
* replace the socket
* close the socket

This prevents races in connection ownership.

## Command_rsp

`Command_rsp` is a helper thread in the main process.

It is write-only with respect to the Editor socket.

### Responsibilities

`Command_rsp` shall:

* block on `command_rsp_queue`
* serialize Coordinator-generated response messages
* write those messages to the Editor socket
* report socket write failures to the Coordinator through `status_queue`

### Non-responsibilities

`Command_rsp` shall not:

* read from the socket
* replace or close the socket
* generate response content on its own
* make request or scheduling decisions

### Important routing rule

Responses sent by `Command_rsp` do **not** flow through `status_queue`.

They are generated by the Coordinator and placed directly onto `command_rsp_queue`.

A response may be triggered by an event that arrived through `status_queue`, such as an error from a worker, but the
response itself is an outbound message and does not return through the inbound event path.

## Reader, Renderer, and Writer Workers

The subsystem workers are independent execution units used for rendering work.

Each subsystem has its own multiprocessing queue.

### Responsibilities

Workers shall:

* receive internal commands from their subsystem queue
* process internal envelopes and work packets
* perform their assigned rendering tasks
* report status, completion, and error events to the Coordinator through `status_queue`

### Non-responsibilities

Workers shall not:

* know about the Editor
* know about socket behavior
* know about JSON protocol
* generate Editor-facing responses directly

## Queue Design

## Inbound event bus: `status_queue`

`status_queue` is the Coordinator’s inbound event bus.

All inbound events that require Coordinator attention shall arrive through this queue.

Typical producers include:

* `Command_rcv`
* Reader
* Renderer
* Writer
* `Command_rsp`, but only for failures such as socket write errors

The Coordinator is the sole consumer of `status_queue`.

## Outbound subsystem queues

Each subsystem has its own queue:

* `read_queue`
* `render_queue`
* `write_queue`

These queues are used by the Coordinator to send work and control messages to the corresponding subsystem workers.

## Outbound Editor response queue

`command_rsp_queue` is the Coordinator’s outbound response queue for the Editor.

The Coordinator is the producer.
`Command_rsp` is the consumer.

This queue is used only for outgoing Editor responses.

## Message Flow

## Incoming command flow

1. The Editor sends a socket message.
2. `Command_rcv` receives the message.
3. `Command_rcv` performs basic syntax validation.
4. `Command_rcv` wraps the message as an internal command envelope.
5. `Command_rcv` places that envelope on `status_queue`.
6. The Coordinator receives the envelope from `status_queue`.
7. The Coordinator decides how to handle the request.
8. The Coordinator dispatches work to subsystem queues as needed.

## Outgoing response flow

1. The Coordinator determines that an Editor response should be sent.
2. The Coordinator generates the response envelope.
3. The Coordinator places the response on `command_rsp_queue`.
4. `Command_rsp` reads the response from `command_rsp_queue`.
5. `Command_rsp` serializes and writes the response to the socket.

If socket writing fails, `Command_rsp` reports that failure to the Coordinator via `status_queue`.

## Example control flow

### Start render request

1. Editor sends `start_render`
2. `Command_rcv` receives and syntax-checks the message
3. `Command_rcv` forwards normalized request to `status_queue`
4. Coordinator accepts or rejects the request
5. If accepted, Coordinator configures subsystems and begins dispatching work
6. Worker status and completion messages flow back through `status_queue`
7. Coordinator determines request completion
8. Coordinator generates `complete` or `error`
9. Coordinator sends response to `command_rsp_queue`
10. `Command_rsp` writes response to the Editor

## State Ownership

Overall daemon state shall be owned exclusively by the Coordinator.

This includes:

* current daemon mode
* whether a render request is active
* the active request identifier
* current request parameters
* subsystem readiness
* completion, failure, and recovery state

Neither `Command_rcv`, `Command_rsp`, nor any worker process may mutate overall daemon state.

They may only emit events to the Coordinator or perform directed work assigned by the Coordinator.

## Socket Ownership Rules

Socket ownership is intentionally asymmetric.

### Command_rcv

`Command_rcv` is the sole owner of socket read-side lifecycle.

It alone may:

* read from the socket
* replace the socket
* close the socket

### Command_rsp

`Command_rsp` may only:

* write to the socket

It may not read, replace, or close the socket.

This split prevents ambiguous socket ownership and reduces race conditions during disconnect or reconnect handling.

## Validation Rules

## Syntax validation

Syntax validation is handled by the proxy threads only at a lightweight level.

For inbound commands, `Command_rcv` may validate:

* JSON parseability
* required top-level keys
* presence of command name
* presence of request id
* presence of params object when required

For outbound responses, `Command_rsp` may validate only enough to serialize safely.

## Semantic validation

Semantic validation is owned by the Coordinator.

Examples include:

* whether the daemon is currently able to accept a new request
* whether a command is valid in the current state
* whether a request conflicts with an active request
* whether parameters are acceptable for scheduling and execution

## Failure Handling

## Socket read failures

If `Command_rcv` encounters a read failure, disconnect, or invalid inbound message, it shall forward a normalized error
or disconnect event to `status_queue`.

The Coordinator shall decide how to react.

## Socket write failures

If `Command_rsp` fails to write a response, it shall report the failure to `status_queue`.

The Coordinator shall decide how to react.

## Worker failures

If Reader, Renderer, or Writer fail, they shall report those failures to `status_queue`.

The Coordinator may then generate an Editor-facing `error` response through `command_rsp_queue`.

## Isolation Guarantees

The design intentionally isolates the major concerns:

### Coordinator

Owns policy, state, and orchestration.

### Command_rcv and Command_rsp

Own only socket adaptation and basic syntax handling.

### Workers

Own only rendering-related execution.

This separation reduces coupling and makes the daemon easier to evolve.

## Benefits of This Design

This design provides several advantages:

* the Coordinator remains queue-driven and does not need socket multiplexing logic
* socket handling is isolated into small, comprehensible proxy threads
* all overall state has a single owner
* subsystem workers remain independent of Editor protocol concerns
* message flow is easier to debug because inbound and outbound paths are explicit
* queue boundaries make the system easier to extend with additional commands later

## Daemon Internal Message Categories

    JOB_REQUEST = 0
    JOB_DONE = 1
    JOB_CANCEL = 2
    LOAD_BLOCK = 3
    BLOCK_LOADED = 4
    RENDER_TILE = 5
    WRITE_TILE = 6
    TILE_WRITTEN = 7
    TILES_FINALIZED = 8
    TELEMETRY = 9
    ERROR = 10
    SHUTDOWN = 11

## Editor to Daemon Socket Messages

* render_request

```json
{
            "msg": "render_request",
            "request_id": 12,
            "params": {
              "percent": 0.2,
              "row": 0.1,
              "col": 0.9,
              "config_path": "config/biome.yml",
              "build_dir": "build/Sedona",
              "output_file": "Sedona_biome.tif"
            }
        }
```

## Daemon to Editor Socket Messages

* complete
* error
* telemetry
* progress

```json
{
  "msg": "complete",
  "request_id": 12,
  "path": "build/Rainier/Rainier_biome.tif"
}
```

```json
{
            "msg": "error",
            "request_id": 12,
            "message": "Render failed"
        }
```

```json
{
            "msg": "telemetry",
            "request_id": 12,
            "details": "# details to come"
        }
```

```json
{
            "msg": "progress",
            "request_id": 12,
            "progress": 85,
            "message": ""
        }
```