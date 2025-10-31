"""
client.py

RDT client for GET/PUT via the emulator.

This client sends application-layer commands over a reliable data transfer session (RDTSession),
which itself runs over UDP. The packets are routed through the network emulator, allowing tests of loss,
corruption, duplication, and reordering.

Usage examples:
    python client.py --server 127.0.0.1:12000 --emulator 127.0.0.1:10000 GET test.txt
    python client.py --server 127.0.0.1:12000 --emulator 127.0.0.1:10000 PUT sample.bin

Behavior summary:
    - GET <file>   - request a file from server, store locally as download_<file>
    - PUT <file>   - upload a local file to the server
"""

import argparse
import os
import socket
import threading
import time

from rdt import RDTSession

BUF = 65535   # maximum UDP datagram size


def win_udp_no_connreset(sock: socket.socket) -> None:
    """
    Disable WSAECONNRESET behavior on Windows UDP sockets.

    When a UDP datagram is sent to a port where nothing is listening,
    Windows may raise WinError 10054 on recv calls. This function disables
    that behavior so the client does not crash if the emulator/server closes.
    """
    try:
        sock.ioctl(socket.SIO_UDP_CONNRESET, b"\x00\x00\x00\x00")
    except (AttributeError, OSError):
        # On non-Windows platforms or unsupported stacks, safely ignore.
        pass


def main():
    """
    Entry point for the RDT file client.

    Steps:
      1. Parse command-line arguments.
      2. Bind a local UDP socket.
      3. Create an RDTSession that sends to the emulator (NOT directly to server).
      4. Run a background thread to pump raw UDP packets into the session.
      5. Execute either GET or PUT.
      6. Shutdown cleanly, ensuring retransmission thread exits before closing socket.
    """
    ap = argparse.ArgumentParser(description="RDT File Client")
    ap.add_argument("--server", required=True, help="server host:port (for reference only)")
    ap.add_argument("--emulator", required=True, help="emulator host:port (real peer for UDP)")
    ap.add_argument("--window", type=int, default=8, help="RDT sliding window size")
    ap.add_argument("--timeout", type=float, default=2.0, help="RDT per-packet timeout seconds")
    ap.add_argument("command", choices=["GET", "PUT"], help="operation type")
    ap.add_argument("filename", help="file to GET or PUT")
    args = ap.parse_args()

    # Emulator address (the peer to send datagrams to)
    em_host, em_port = args.emulator.split(":")
    emulator_addr = (em_host, int(em_port))

    # Create local UDP socket, bound to ephemeral port
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", 0))
    win_udp_no_connreset(sock)
    sock.settimeout(0.2)

    # Create reliable session to emulator
    sess = RDTSession(sock, emulator_addr, window=args.window, timeout=args.timeout)

    # Background packet-pumping thread
    stop_evt = threading.Event()

    def pump():
        """
        Continuously read raw UDP datagrams from socket and feed them to RDTSession.
        """
        while not stop_evt.is_set():
            try:
                raw, _ = sock.recvfrom(BUF)
            except socket.timeout:
                continue
            except OSError:
                # Socket closed during shutdown
                break
            sess.handle_raw(raw)

    t = threading.Thread(target=pump, daemon=True)
    t.start()

    #   GET command handler
    if args.command == "GET":
        # Send application command over RDT
        sess.send(f"GET {args.filename}\n".encode())

        chunks = []
        idle = 0.0
        # Allow long idle because rate is < 500bps and emulator may drop packets
        while idle < 8.0:
            time.sleep(0.2)
            b = sess.recv_available()
            if b:
                chunks.append(b)
                idle = 0.0
            else:
                idle += 0.2

        data = b"".join(chunks)

        # Server reported an error
        if data.startswith(b"ERROR"):
            print(data.decode(errors="ignore"))

        # Valid file data received
        elif data:
            out = f"download_{os.path.basename(args.filename)}"
            with open(out, "wb") as f:
                f.write(data)
            print(f"[CLIENT] wrote {out} ({len(data)} bytes)")

        # No data received
        else:
            print("[CLIENT] no data received")

    # PUT command handler
    else:
        if not os.path.isfile(args.filename):
            print("[CLIENT] local file not found")
            stop_evt.set()
            sess.stop()
            try:
                sock.close()
            except Exception:
                pass
            return

        # Read local file
        with open(args.filename, "rb") as f:
            content = f.read()

        # Tell server which file is coming
        sess.send(f"PUT {os.path.basename(args.filename)}\n".encode())
        time.sleep(0.3)   # short delay so server hears header before file bytes

        # Send raw file bytes
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

    # CLEAN SHUTDOWN
    sess.stop()       # stop retransmission thread first
    stop_evt.set()    # stop packet pump
    time.sleep(0.05)  # give thread time to exit
    try:
        sock.close()
    except Exception:
        pass


if __name__ == "__main__":
    main()
