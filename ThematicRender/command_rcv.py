import json
import os
import socket

from ThematicRender.ipc_packets import Envelope, Op


def command_rcv_loop(socket_path: str, status_queue):
    """
    Dedicated thread for receiving, validating and forwarding commands from the Client.
    """
    # 1. Clean up stale socket from previous runs
    if os.path.exists(socket_path):
        os.unlink(socket_path)

    # 2. Setup Unix Domain Socket
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(socket_path)
    server.listen(1)

    print(f" Daemon listening on {socket_path}")

    try:
        while True:
            # 3. Block until Client connects
            conn, _ = server.accept()
            print("[command_rcv] Client connected")

            with conn:
                # Use makefile to handle the newline-delimited protocol easily
                f = conn.makefile('r', encoding='utf-8')

                for line in f:
                    if not line.strip():
                        continue

                    try:
                        # 4. Syntax Validation (Basic JSON/Field check)
                        data = json.loads(line)
                        msg_type = data.get("msg")
                        job_id = data.get("job_id")

                        if not msg_type or job_id is None:
                            raise ValueError("Missing 'msg' or 'job_id'")

                        # 5. Normalize and Forward to Coordinator
                        match msg_type:
                            case "render_request":
                                status_queue.put(
                                    Envelope(
                                        op=Op.JOB_REQUEST, payload=data
                                        # Pass the full JSON dict as payload
                                    )
                                )
                                print(
                                    "[command_rcv] Received render request. Queued to Status Queue"
                                )

                            case "halt":
                                status_queue.put(Envelope(op=Op.JOB_CANCEL))
                                return  # Exit thread on halt

                            case _:
                                # Report unknown message type to Coordinator
                                print("ERROR Received UNKNOWN request")

                                status_queue.put(
                                    Envelope(
                                        op=Op.ERROR,
                                        payload=(job_id, "SOCKET", f"Unknown msg type: {msg_type}")
                                    )
                                )

                    except json.JSONDecodeError as e:
                        # Protocol failure: Corrupt JSON
                        print(f"[command_rcv] Bad JSON packet {e}")
                        status_queue.put(
                            Envelope(
                                op=Op.ERROR, payload=(-1, "SOCKET", "Malformed JSON received")
                            )
                        )
                    except Exception as e:
                        print(f"[command_rcv] Receive err {e}")

                        # General validation failure
                        status_queue.put(
                            Envelope(
                                op=Op.ERROR, payload=(-1, "SOCKET", str(e))
                            )
                        )

            # If we reach here, the Client disconnected.  # The loop continues to server.accept()
            # to wait for a new connection.

    finally:
        server.close()
        if os.path.exists(socket_path):
            os.unlink(socket_path)
