import json
import multiprocessing as mp
import os
from queue import Empty
import socket
import threading
from typing import Optional

from Common.ipc_packets import Envelope, Op


class ClientProxy:
    """
    Dedicated networking class for the Unix socket.

    Handles newline-delimited JSON framing and lightweight
    protocol-level validation.
    """

    ACCEPT_TIMEOUT_S = 1.0
    RESPONSE_TIMEOUT_S = 1.0

    def __init__(
            self, socket_path: str, status_q: "mp.Queue", response_q: "mp.Queue", ) -> None:
        self.socket_path = socket_path
        self.status_q = status_q
        self.response_q = response_q

        self.running = False
        self._threads: list[threading.Thread] = []
        self._server_socket: Optional[socket.socket] = None
        self._active_connection: Optional[socket.socket] = None
        self._conn_lock = threading.Lock()

        if response_q is None:
            raise ValueError("none response_queue")

    def start(self) -> None:
        """Initialize the Unix socket and start communication threads."""
        print(f"➡️ [CommandProxy] Opening socket at {self.socket_path}")

        try:
            if os.path.exists(self.socket_path):
                os.remove(self.socket_path)
        except OSError as exc:
            raise RuntimeError(
                f"Failed to remove existing socket file: {self.socket_path}"
            ) from exc

        self.running = True

        rcv_thread = threading.Thread(
            target=self._rcv_loop, name="Socket_Rcv", daemon=True, )
        rsp_thread = threading.Thread(
            target=self._rsp_loop, name="Socket_Rsp", daemon=True, )

        rcv_thread.start()
        rsp_thread.start()
        self._threads = [rcv_thread, rsp_thread]

    def _set_active_connection(self, conn: Optional[socket.socket]) -> None:
        """Replace the current active connection safely."""
        with self._conn_lock:
            old_conn = self._active_connection
            self._active_connection = conn

        if old_conn is not None and old_conn is not conn:
            try:
                old_conn.close()
            except OSError:
                pass

    def _get_active_connection(self) -> Optional[socket.socket]:
        """Return the current active connection safely."""
        with self._conn_lock:
            return self._active_connection

    def _clear_active_connection(self) -> None:
        """Clear and close the current active connection safely."""
        self._set_active_connection(None)

    def _rcv_loop(self) -> None:
        """Listen for NDJSON commands from the Editor."""
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._server_socket = server

        try:
            server.bind(self.socket_path)
            server.listen(1)
            server.settimeout(self.ACCEPT_TIMEOUT_S)

            while self.running:
                try:
                    conn, _ = server.accept()
                except socket.timeout:
                    continue
                except OSError:
                    if self.running:
                        self.status_q.put(
                            Envelope(
                                op=Op.ERROR, payload=(-1, "COMMAND_RCV", "Socket accept failed"), )
                        )
                    break

                self._set_active_connection(conn)
                print("✅ [CommandProxy] Editor connected")

                try:
                    with conn, conn.makefile("r", encoding="utf-8") as f:
                        while self.running:
                            line = f.readline()
                            if not line:
                                break

                            self._handle_incoming_line(line)
                except OSError as exc:
                    self.status_q.put(
                        Envelope(
                            op=Op.ERROR,
                            payload=(-1, "COMMAND_RCV", f"Socket read failed: {exc}"), )
                    )
                finally:
                    self._clear_active_connection()
                    print("⚠️ [CommandProxy] Editor disconnected")

        finally:
            try:
                server.close()
            except OSError:
                pass
            self._server_socket = None

    def _handle_incoming_line(self, line: str) -> None:
        """Parse and validate one inbound JSON line."""
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            print("❌ [CommandProxy] Received malformed JSON")
            self._queue_protocol_error("Protocol error: malformed JSON")
            return

        if not isinstance(data, dict):
            self._queue_protocol_error("Protocol error: message must be a JSON object")
            return

        msg_type = data.get("msg")
        job_id = data.get("job_id")

        if msg_type == "job_request":
            if job_id is None:
                self._queue_protocol_error(
                    "Protocol error: job request missing job_id"
                )
                return
            if "params" not in data:
                self._queue_protocol_error(
                    "Protocol error: job request missing params"
                )
                return

            self.status_q.put(
                Envelope(op=Op.JOB_REQUEST, payload=data)
            )
            return

        if msg_type == "halt":
            self.status_q.put(Envelope(op=Op.JOB_CANCEL))
            return

        print(f"⚠️ [CommandProxy] Unknown message type: {msg_type}")
        self._queue_protocol_error(f"Protocol error: unknown msg type '{msg_type}'")

    def _rsp_loop(self) -> None:
        """Listen to response_queue and send updates back to the Editor."""
        while self.running:
            try:
                payload = self.response_q.get(timeout=self.RESPONSE_TIMEOUT_S)
            except Empty:
                continue
            except (OSError, EOFError):
                # This triggers if the queue handle is closed while we are waiting
                # We exit the loop quietly as the system is shutting down
                break

            conn = self._get_active_connection()
            if conn is None:
                print("⚠️ [CommandProxy] No active connection. Response dropped.")
                continue

            try:
                msg = json.dumps(payload) + "\n"
                conn.sendall(msg.encode("utf-8"))
            except (BrokenPipeError, ConnectionResetError, OSError) as exc:
                self._clear_active_connection()
                print(f"❌ [CommandProxy] Connection lost. Response dropped: {exc}")
                self.status_q.put(
                    Envelope(
                        op=Op.ERROR, payload=(-1, "COMMAND_RSP", f"Socket write failed: {exc}"), )
                )

    def _queue_protocol_error(self, message: str) -> None:
        """Queue a protocol-level error response for the client."""
        self.response_q.put({"msg": "error", "message": message})

    def stop(self) -> None:
        """Stop threads and clean up socket resources."""
        self.running = False

        self._clear_active_connection()

        if self._server_socket is not None:
            try:
                self._server_socket.close()
            except OSError:
                pass

        for thread in self._threads:
            thread.join(timeout=2.0)

        if os.path.exists(self.socket_path):
            try:
                os.remove(self.socket_path)
            except OSError:
                pass
