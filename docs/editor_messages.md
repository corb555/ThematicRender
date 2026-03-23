# Editor–Daemon Messaging Requirements

## Purpose

This document defines the messaging requirements between the Editor and the Raster Builder Daemon.

## Goal

The messaging system shall provide a simple, low-latency, local mechanism for the Editor to request raster renders from the 
Daemon and receive status, completion, and error responses.

The messaging design shall support an interactive workflow in which the user edits settings, saves them, requests a render, 
and sees the resulting raster displayed with minimal delay.

## Scope

This interface covers:

* connection establishment between the Editor and the Daemon
* render request messages from the Editor
* status and completion messages from the Daemon
* error reporting
* message framing and format
* request correlation
* connection and protocol behavior during normal and failure conditions

## Transport

Messaging shall use a local interprocess communication channel based on `QLocalSocket` and `QLocalServer` on
the Editor and socket.AF_UNIX on the Daemon.

The Daemon shall act as the server.
The Editor shall act as the client.

The transport shall be local-machine only. 

The Daemon shall open the socket: /tmp/thematic_render.sock

## Message Model

All messages exchanged between the Editor and the Daemon shall be small control messages. Raster image data shall not be 
sent over the messaging channel.

The messaging channel shall be used for:

* render requests
* progress or state updates
* completion notifications
* error notifications
* optional cancel or shutdown commands 

## Encoding and Framing

Messages shall be encoded as UTF-8 JSON.

Each message shall be framed as a single newline-terminated JSON object.

Each side shall treat one newline-terminated JSON object as one complete message.

The protocol shall not rely on partial-message parsing outside this framing rule.

All messages will include a message type (msg)

## Connection Model

The Editor shall establish a client connection to the Daemon when the View tab needs to issue or receive 
render-related messages.

The Editor may keep the connection open across multiple requests.

The Daemon shall be able to accept a connection from the Editor and remain available for repeated 
request/response cycles.

If the connection is lost, the Editor shall detect the disconnect and report that the Daemon is unavailable.

## Request–Response Pattern

The Editor shall send a render request message when the user clicks **Build**.

The Daemon shall respond asynchronously. It is not required to complete the request before acknowledging receipt of 
the message at the transport level.

## Request Identity

Each render request shall include a `request_id`.

The Daemon shall include the same `request_id` in every response associated with that request.

This requirement applies to:

* progress messages
* completion messages
* error messages
* any future cancellation acknowledgments

The `request_id` allows the Editor to match responses to the correct request and safely ignore stale messages.

### Terminal Response
For each render request, the Daemon must eventually send exactly one terminal response:

* `complete`
* `error`
* `cancelled`, if cancellation is later supported

Any message for that `request_id` sent after a Terminal Response will be discarded.

### Progress / Status
Optional intermediate progress or state messages may be sent before the terminal response.


## Message Types

### Render Request

The Editor shall send a `start_render` command to request a render.

The request shall include a `params` object containing the operational settings needed for that render.

Minimum required parameters:

* `percent`
* `row`
* `col`
* `config_path`
* `build_dir`
* `output_filename`

Optional parameters may be added later, such as build directory or output file name, without changing the overall protocol shape.

Example:

```json
{
  "msg": "start_render",
  "request_id": "req-0001",
  "params": {
    "percent": 0.1,
    "row": 0.5,
    "col": 0.5,
    "config_path": "biome.yml"
  }
}
```

### Progress Message

The Daemon may send a progress status message while work is in progress.

A progress message may include human-readable status text or defined structured fields.
Example:

```json
{
  "msg": "progress",
  "request_id": "req-0001",
  "progress": 0.85,
  "message": "Rendering sample window"
}
```

### Completion Message

When rendering succeeds, the Daemon shall send a `complete` message.

The completion message shall include the output raster path.

Example:

```json
{
  "msg": "complete",
  "request_id": "req-0001",
  "path": "build/Rainier/Rainier_biome.tif"
}
```

### Error Message

If rendering fails, the Daemon shall send an `error` message.

The error message shall include a human-readable description.

Example:

```json
{
  "msg": "error",
  "request_id": "req-0001",
  "message": "Reader failed for tile 17"
}
```

## Editor Requirements

The Editor shall:

* connect to the Daemon over `QLocalSocket`
* send newline-delimited JSON requests
* parse newline-delimited JSON responses
* include a `request_id` in every render request
* disable or otherwise guard the Build action while a request is active, unless overlapping requests are intentionally supported
* treat `complete` and `error` as terminal states for the active request
* ignore messages whose `request_id` does not match the active request
* display an error if the Daemon cannot be reached
* display an error if a malformed or unknown message is received
* use the returned `path` from a `complete` message to load and display the TIFF

The Editor shall not assume that a response will arrive immediately.
The Editor shall remain responsive while waiting for Daemon messages.

## Daemon Requirements

The Daemon shall:

* listen for Editor connections using `socket.AF_UNIX` on the above specified socket name.
* accept newline-delimited JSON requests
* validate incoming messages before acting on them
* reject malformed or incomplete requests with an `error` response when possible
* include the originating `request_id` in every response associated with that request
* send exactly one terminal response for each accepted `start_render` request
* send the output TIFF path in the completion response
* send an error response if the render cannot be started or cannot complete
* remain running across multiple requests unless explicitly shut down

## Validation Requirements

The Daemon shall validate incoming `start_render` messages before starting work.

At minimum, it shall validate:

* that `msg` is recognized
* that `request_id` is present
* that `params` is present
* that required parameter fields are present
* that parameter values are of usable type and range

If validation fails, the Daemon shall return an `error` response and shall not start the render.

## Ordering Requirements

Responses for a single request shall be logically ordered:

* zero or more progress messages
* then one terminal message

No progress message should be sent after a terminal message for the same `request_id`.

## Failure Handling

If the Editor cannot connect to the Daemon, it shall report the connection failure to the user.

If the Daemon disconnects unexpectedly while a request is active, the Editor shall treat the request as failed.

If the Daemon receives malformed JSON, it may close the connection or send an error response. The preferred behavior is to send an error response when the message boundary is intact and the request can still be identified.

If either side receives an unknown message type, it shall treat that as a protocol error.

## Timeout Behavior

The Editor should apply a reasonable timeout policy for detecting a lost or stalled Daemon connection.

The timeout shall be long enough to allow valid renders to complete but short enough to detect a dead Daemon or broken connection.

A timeout shall be treated as a failed request unless a progress policy explicitly resets the timeout window.

## Concurrency Policy

The initial protocol shall assume a single active render request at a time from the Editor.

If the Editor sends a new `start_render` request while another is still active, the following policies shall be 
implemented explicitly:

* reject the new request with an error (Phase-1)
* queue the new request (Future Enhancement)

## Protocol Extensibility

The message format shall allow additional fields to be added later without breaking existing messages.

Future message types may include:

* `cancel_render`
* `cancelled`
* `ping`
* `pong`
* `shutdown`

Unknown extra fields in otherwise valid messages should be ignored unless they conflict with required semantics.

## Security and Trust Boundary

The messaging channel is intended for local communication between trusted processes on the same machine.

No authentication or encryption is required for the initial implementation.

The Daemon shall still validate message structure and required fields to avoid unsafe behavior caused by malformed input.

## Non-Functional Requirements

The messaging system shall be:

* low latency
* event driven
* simple to debug
* robust against malformed messages
* robust against disconnects
* compatible with the Qt event loop
* suitable for repeated interactive use

The protocol should be simple enough that both request and response messages can be logged directly for troubleshooting.

## Minimal Required Protocol Examples

### Start Render

(Needs build dir and output file added)

### Progress

```json
{
  "msg": "progress",
  "request_id": "req-0001",
  "progress": 0.85,
  "message": "Rendering"
}
```
 
```json
{
  "msg": "start_render",
  "request_id": "req-0001",
  "params": {
    "percent": 0.1,
    "row": 0.5,
    "col": 0.5,
    "config_path": "biome.yml",
    "build_dir": "/usr/joe/",
    "output_file": "yosemite.tif"
  }
}
```

### Complete

```json
{
  "msg": "complete",
  "request_id": "req-0001",
  "path": "build/Rainier/Rainier_biome.tif"
}
```

### Error

```json
{
  "msg": "error",
  "request_id": "req-0001",
  "message": "Render failed"
}
```
