"""
server.py

Single-thread UDP server with per-peer RDT sessions and a tiny file service.

The server exposes a minimal application-layer file transfer protocol on top
of the custom reliable transport (RDTSession). Each remote UDP address gets
its own RDT session, allowing multiple independent clients to interact at once.

Supported commands (ASCII, newline terminated):
    GET <filename>\n      -> server sends file contents back via RDT
    PUT <filename>\n      -> client uploads raw file bytes immediately after
    (any other text)      -> server responds with "OK: ECHO: <text>"
"""

import argparse
import os
import signal
import socket
import time
from typing import Dict, Tuple

from rdt import RDTSession

BUF = 65535   # max UDP datagram buffer size


def win_udp_no_connreset(sock: socket.socket) -> None:
    """
    Windows-only: disable WSAECONNRESET behavior.

    On Windows, if a UDP packet is sent to an unreachable port, the OS may
    raise WinError 10054 on subsequent recv calls. This helper disables
    that behavior so the server does not crash when a client exits abruptly.
    """
    try:
        sock.ioctl(socket.SIO_UDP_CONNRESET, b"\x00\x00\x00\x00")
    except (AttributeError, OSError):
        # Not Windows or unsupported? Ignore silently.
        pass


def main():
    """
    Main server loop.

    Responsibilities:
      - Bind a UDP socket and listen for incoming datagrams.
      - Maintain a dictionary of per-peer RDTSession objects.
      - Feed raw packets into the proper session via handle_raw().
      - Process fully reassembled application bytes (GET/PUT/etc.).
      - Shut down cleanly on Ctrl+C.
    """
    ap = argparse.ArgumentParser(description="RDT File Server")
    ap.add_argument("--host", default="127.0.0.1", help="IP to bind")
    ap.add_argument("--port", type=int, default=12000, help="UDP port to bind")
    ap.add_argument("--window", type=int, default=8, help="RDT sliding window size")
    ap.add_argument("--timeout", type=float, default=2.0, help="RDT per-packet timeout")
    args = ap.parse_args()

    # Create single UDP socket for all clients
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((args.host, args.port))
    win_udp_no_connreset(sock)     # prevent forced close on Windows
    sock.settimeout(0.2)           # short timeout so loop can progress

    print(f"[SERVER] listening on {args.host}:{args.port}")

    # Per-peer RDT session table: {(ip,port) -> RDTSession}
    sessions: Dict[Tuple[str, int], RDTSession] = {}

    stopping = False   # set to True on Ctrl+C

    def stop_handler(*_):
        """Signal handler: mark server for shutdown and close socket immediately."""
        nonlocal stopping
        stopping = True
        try:
            sock.close()
        except Exception:
            pass
        print("\n[SERVER] stopping...")

    signal.signal(signal.SIGINT, stop_handler)
    try:
        signal.signal(signal.SIGTERM, stop_handler)
    except Exception:
        # SIGTERM not available on Windows
        pass

    try:
        while not stopping:
            # 1) Poll for raw UDP traffic
            try:
                raw, addr = sock.recvfrom(BUF)
            except socket.timeout:
                raw, addr = None, None
            except OSError:
                # socket was closed -> exit loop
                break

            if raw is not None:
                # Create a new RDT session lazily on first packet from client
                if addr not in sessions:
                    sessions[addr] = RDTSession(sock, addr,
                                                window=args.window,
                                                timeout=args.timeout)
                    print(f"[SERVER] new peer {addr}")

                # Pass raw UDP bytes to RDT (could be data or ACK)
                sessions[addr].handle_raw(raw)

            # 2) Check each peer for fully reassembled app data
            for addr, sess in list(sessions.items()):
                app = sess.recv_available()
                if not app:
                    continue  # nothing to process yet

                text = app.decode(errors="ignore")

                # GET COMMAND
                if text.startswith("GET "):
                    line = text.splitlines()[0]
                    _, name = line.split(" ", 1)
                    name = name.strip()
                    if not os.path.isfile(name):
                        sess.send(f"ERROR: File {name} not found".encode())
                        continue
                    with open(name, "rb") as f:
                        data = f.read()
                    print(f"[SERVER] sending {name} ({len(data)}B) to {addr}")
                    sess.send(data)

                # PUT COMMAND
                elif text.startswith("PUT "):
                    line = text.splitlines()[0]
                    _, name = line.split(" ", 1)
                    name = name.strip()
                    print(f"[SERVER] expecting upload -> {name} from {addr}")

                    # Collect file bytes with extended idle timeout
                    chunks = []
                    last = time.monotonic()
                    while time.monotonic() - last < 6.0:  # tolerate slow transfer
                        b = sess.recv_available()
                        if b:
                            chunks.append(b)
                            last = time.monotonic()
                        else:
                            time.sleep(0.05)

                    content = b"".join(chunks)
                    with open(name, "wb") as f:
                        f.write(content)
                    print(f"[SERVER] stored {name} ({len(content)}B) from {addr}")
                    sess.send(f"OK: Stored {name} ({len(content)} bytes)".encode())

                # ANY OTHER TEXT -> ECHO
                else:
                    sess.send(b"OK: ECHO: " + app)

    finally:
        # Graceful shutdown
        for s in sessions.values():
            try:
                s.stop()
            except Exception:
                pass
        try:
            sock.close()
        except Exception:
            pass
        print("[SERVER] Exit complete.")


if __name__ == "__main__":
    main()
