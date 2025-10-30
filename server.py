"""
server.py

Single-thread UDP server with per-peer RDT sessions and a tiny file service.

Commands (ASCII lines):
  GET <filename>\n
  PUT <filename>\n  (followed by raw bytes)
"""

import argparse
import os
import signal
import socket
import time
from typing import Dict, Tuple

from rdt import RDTSession

BUF = 65535


def win_udp_no_connreset(sock: socket.socket) -> None:
    """Windows-only: turn off UDP 'connection reset' (ICMP port unreachable -> WSAECONNRESET)."""
    try:
        sock.ioctl(socket.SIO_UDP_CONNRESET, b"\x00\x00\x00\x00")
    except (AttributeError, OSError):
        pass


def main():
    ap = argparse.ArgumentParser(description="RDT File Server")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=12000)
    ap.add_argument("--window", type=int, default=8)
    ap.add_argument("--timeout", type=float, default=2.0)
    args = ap.parse_args()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((args.host, args.port))
    win_udp_no_connreset(sock)
    sock.settimeout(0.2)

    print(f"[SERVER] listening on {args.host}:{args.port}")

    sessions: Dict[Tuple[str, int], RDTSession] = {}
    stopping = False

    def stop_handler(*_):
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
        pass

    try:
        while not stopping:
            # pump network
            try:
                raw, addr = sock.recvfrom(BUF)
            except socket.timeout:
                raw, addr = None, None
            except OSError:
                break
            if raw is not None:
                if addr not in sessions:
                    sessions[addr] = RDTSession(sock, addr, window=args.window, timeout=args.timeout)
                    print(f"[SERVER] new peer {addr}")
                sessions[addr].handle_raw(raw)

            # handle app data
            for addr, sess in list(sessions.items()):
                app = sess.recv_available()
                if not app:
                    continue

                text = app.decode(errors="ignore")
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

                elif text.startswith("PUT "):
                    line = text.splitlines()[0]
                    _, name = line.split(" ", 1)
                    name = name.strip()
                    print(f"[SERVER] expecting upload -> {name} from {addr}")
                    chunks = []
                    last = time.monotonic()
                    # allow longer idle due to low rate
                    while time.monotonic() - last < 6.0:
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

                else:
                    sess.send(b"OK: ECHO: " + app)

    finally:
        for s in sessions.values():
            try:
                s.stop()
            except Exception:
                pass
        try:
            sock.close()
        except Exception:
            pass
        print("[SERVER] bye")


if __name__ == "__main__":
    main()
