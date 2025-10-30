"""
client.py

RDT client for GET/PUT via the emulator.

Examples:
  python client.py --server 127.0.0.1:12000 --emulator 127.0.0.1:10000 GET test.txt
  python client.py --server 127.0.0.1:12000 --emulator 127.0.0.1:10000 PUT sample.bin
"""

import argparse
import os
import socket
import threading
import time

from rdt import RDTSession

BUF = 65535


def win_udp_no_connreset(sock: socket.socket) -> None:
    """Windows-only: turn off UDP 'connection reset' (ICMP port unreachable -> WSAECONNRESET)."""
    try:
        sock.ioctl(socket.SIO_UDP_CONNRESET, b"\x00\x00\x00\x00")
    except (AttributeError, OSError):
        pass


def main():
    ap = argparse.ArgumentParser(description="RDT File Client")
    ap.add_argument("--server", required=True)
    ap.add_argument("--emulator", required=True)
    ap.add_argument("--window", type=int, default=8)
    ap.add_argument("--timeout", type=float, default=2.0)
    ap.add_argument("command", choices=["GET", "PUT"])
    ap.add_argument("filename")
    args = ap.parse_args()

    em_host, em_port = args.emulator.split(":")
    emulator_addr = (em_host, int(em_port))

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", 0))
    win_udp_no_connreset(sock)
    sock.settimeout(0.2)

    sess = RDTSession(sock, emulator_addr, window=args.window, timeout=args.timeout)

    stop_evt = threading.Event()

    def pump():
        while not stop_evt.is_set():
            try:
                raw, _ = sock.recvfrom(BUF)
            except socket.timeout:
                continue
            except OSError:
                break
            sess.handle_raw(raw)

    t = threading.Thread(target=pump, daemon=True)
    t.start()

    if args.command == "GET":
        sess.send(f"GET {args.filename}\n".encode())
        chunks = []
        idle = 0.0
        # allow longer idle due to sub-500 bps and possible loss/retx
        while idle < 8.0:
            time.sleep(0.2)
            b = sess.recv_available()
            if b:
                chunks.append(b)
                idle = 0.0
            else:
                idle += 0.2
        data = b"".join(chunks)
        if data.startswith(b"ERROR"):
            print(data.decode(errors="ignore"))
        elif data:
            out = f"download_{os.path.basename(args.filename)}"
            with open(out, "wb") as f:
                f.write(data)
            print(f"[CLIENT] wrote {out} ({len(data)} bytes)")
        else:
            print("[CLIENT] no data received")

    else:  # PUT
        if not os.path.isfile(args.filename):
            print("[CLIENT] local file not found")
            stop_evt.set()
            sess.stop()
            try:
                sock.close()
            except Exception:
                pass
            return
        with open(args.filename, "rb") as f:
            content = f.read()
        sess.send(f"PUT {os.path.basename(args.filename)}\n".encode())
        time.sleep(0.3)
        sess.send(content)
        chunks = []
        idle = 0.0
        while idle < 8.0:
            time.sleep(0.2)
            b = sess.recv_available()
            if b:
                chunks.append(b)
                idle = 0.0
            else:
                idle += 0.2
        if chunks:
            print(b"".join(chunks).decode(errors="ignore"))
        else:
            print("[CLIENT] no server response")

    # Clean shutdown order to avoid WinError 10038
    sess.stop()
    stop_evt.set()
    time.sleep(0.05)
    try:
        sock.close()
    except Exception:
        pass


if __name__ == "__main__":
    main()
