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
from typing import Dict, Tuple, Optional

from rdt import RDTSession

BUF = 65535   # max UDP datagram buffer size

# Idle timeout while receiving a PUT payload (seconds).
PUT_IDLE_TIMEOUT = 6.0


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


class PeerState:
    """
    Per-peer application state.

    - inbuf: accumulates command-line bytes until '\n' is seen
    - mode: None or 'receiving' during PUT
    - filename: target filename during PUT
    - filebuf: bytearray of file content during PUT
    - last_data_ts: last time we received any file bytes (for idle cutoff)
    """

    def __init__(self) -> None:
        self.inbuf = bytearray()
        self.mode: Optional[str] = None
        self.filename: Optional[str] = None
        self.filebuf = bytearray()
        self.last_data_ts: float = 0.0

    def start_put(self, filename: str) -> None:
        self.mode = "receiving"
        self.filename = filename
        self.filebuf = bytearray()
        self.last_data_ts = time.monotonic()

    def on_file_bytes(self, data: bytes) -> None:
        if not data:
            return
        self.filebuf.extend(data)
        self.last_data_ts = time.monotonic()

    def put_done(self) -> Tuple[str, int, bytes]:
        """Finalize PUT state; return (filename, num_bytes, content)."""
        name = self.filename or "upload.bin"
        content = bytes(self.filebuf)
        n = len(content)
        # Reset state for next command
        self.mode = None
        self.filename = None
        self.filebuf.clear()
        return name, n, content


def main():
    """
    Main server loop.

    Responsibilities:
      - Bind a UDP socket and listen for incoming datagrams.
      - Maintain a dictionary of per-peer RDTSession objects + app state.
      - Feed raw packets into the proper session via handle_raw().
      - Parse newline-terminated commands from a per-peer buffer.
      - While in PUT mode, treat all bytes as file data until idle timeout.
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
    # Per-peer app-layer state (command buffer, PUT mode, etc.)
    app_state: Dict[Tuple[str, int], PeerState] = {}

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
                # Create a new RDT session and app state lazily on first packet from client
                if addr not in sessions:
                    sessions[addr] = RDTSession(sock, addr,
                                                window=args.window,
                                                timeout=args.timeout)
                    app_state[addr] = PeerState()
                    print(f"[SERVER] new peer {addr}")

                # Pass raw UDP bytes to RDT (could be data or ACK)
                sessions[addr].handle_raw(raw)

            # 2) Process per-peer application state
            now = time.monotonic()
            for addr, sess in list(sessions.items()):
                st = app_state[addr]

                # If currently receiving a file (PUT mode), treat all bytes as file data
                if st.mode == "receiving":
                    more = sess.recv_available()
                    if more:
                        st.on_file_bytes(more)

                    # Finalize if we've been idle long enough
                    if (now - st.last_data_ts) >= PUT_IDLE_TIMEOUT:
                        filename, nbytes, content = st.put_done()
                        with open(filename, "wb") as f:
                            f.write(content)
                        print(f"[SERVER] stored {filename} ({nbytes}B) from {addr}")
                        sess.send(f"OK: Stored {filename} ({nbytes} bytes)".encode())
                        # Done with this peer for this iteration
                        continue

                # Not in PUT mode: read any app bytes and parse commands
                if st.mode != "receiving":
                    app = sess.recv_available()
                    if not app:
                        continue

                    # Accumulate into command buffer
                    st.inbuf.extend(app)

                    # Process complete lines (commands end with '\n')
                    while True:
                        nl = st.inbuf.find(b"\n")
                        if nl < 0:
                            break  # wait for more bytes to complete a line
                        # Extract one full line (without trailing '\n')
                        line = bytes(st.inbuf[:nl]).decode(errors="ignore").strip()
                        # Drop processed bytes (line + '\n')
                        del st.inbuf[:nl + 1]

                        # Command parsing
                        if line.upper().startswith("GET "):
                            _, name = line.split(" ", 1)
                            name = name.strip()
                            if not os.path.isfile(name):
                                sess.send(f"ERROR: File {name} not found".encode())
                            else:
                                with open(name, "rb") as f:
                                    data = f.read()
                                print(f"[SERVER] sending {name} ({len(data)}B) to {addr}")
                                sess.send(data)

                        elif line.upper().startswith("PUT "):
                            _, name = line.split(" ", 1)
                            name = name.strip()
                            print(f"[SERVER] expecting upload -> {name} from {addr}")
                            st.start_put(name)
                            # Immediately drain any payload that might already be queued
                            more = sess.recv_available()
                            if more:
                                st.on_file_bytes(more)

                        else:
                            # Unknown command -> echo only this line back
                            sess.send(("OK: ECHO: " + line).encode())

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
