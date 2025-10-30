"""
emulator.py (Windows-hardened)

UDP packet emulator to inject loss, corruption, duplication, and reordering.
Works as a simple A<->Server relay.

Hardenings:
- Disables Windows UDP connreset (ICMP port unreachable -> 10054)
- Never exits on recv/send errors; logs and continues
- Uses select() and timed reordering without extra threads
"""

import argparse
import random
import signal
import socket
import threading
import time
import select

BUF = 65535


def win_udp_no_connreset(sock: socket.socket) -> None:
    """Windows-only: turn off UDP 'connection reset' (ICMP port unreachable -> WSAECONNRESET)."""
    try:
        sock.ioctl(socket.SIO_UDP_CONNRESET, b"\x00\x00\x00\x00")
    except (AttributeError, OSError):
        pass


def corrupt(data: bytes) -> bytes:
    if not data:
        return data
    i = random.randrange(len(data))
    b = bytearray(data)
    b[i] ^= 0xFF
    return bytes(b)


def emulator_loop(sock_a, addr_a, sock_b, addr_b, server_addr, args, stop_evt: threading.Event):
    last_client = None
    reorder_slot = None  # (data, dst_sock, dst_addr, direction, release_time)

    print(f"[EMUL] A={addr_a}, B={addr_b}, server={server_addr}")
    print(f"[EMUL] loss={args.loss:.2f}, corrupt={args.corrupt:.2f}, reorder={args.reorder:.2f}, dup={args.dup:.2f}")

    for s in (sock_a, sock_b):
        s.setblocking(False)

    while not stop_evt.is_set():
        try:
            rlist, _, _ = select.select([sock_a, sock_b], [], [], 0.1)
        except Exception:
            continue

        now = time.monotonic()

        # timed release of delayed packet (no extra threads)
        if reorder_slot and now >= reorder_slot[4]:
            pkt, s2, a2, d2, _ = reorder_slot
            try:
                s2.sendto(pkt, a2)
                print(f"[EMUL] SEND delayed {d2}")
            except OSError as e:
                print(f"[EMUL] delayed send OSError: {e}")
            reorder_slot = None

        if not rlist:
            continue

        for s in rlist:
            try:
                data, src = s.recvfrom(BUF)
            except (BlockingIOError, InterruptedError):
                continue
            except OSError as e:
                print(f"[EMUL] recv OSError: {e}")
                continue

            if not data:
                continue

            if s is sock_a:
                last_client = src
                dst_sock, dst_addr = sock_b, server_addr
                direction = "A->Server"
            else:
                if last_client is None:
                    continue
                dst_sock, dst_addr = sock_a, last_client
                direction = "Server->A"

            # loss
            if random.random() < args.loss:
                print(f"[EMUL] DROP {direction} len={len(data)}")
                continue

            # corruption
            if random.random() < args.corrupt:
                data = corrupt(data)
                print(f"[EMUL] CORRUPT {direction}")

            # duplication
            if random.random() < args.dup:
                try:
                    dst_sock.sendto(data, dst_addr)
                    print(f"[EMUL] DUP {direction}")
                except OSError as e:
                    print(f"[EMUL] dup send OSError: {e}")

            # reordering (delay one packet)
            if reorder_slot is None and random.random() < args.reorder:
                reorder_slot = (data, dst_sock, dst_addr, direction, time.monotonic() + random.uniform(0.05, 0.3))
                print(f"[EMUL] REORDER (hold) {direction}")
            else:
                try:
                    dst_sock.sendto(data, dst_addr)
                except OSError as e:
                    print(f"[EMUL] fwd send OSError {direction}: {e}")
                    continue


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--listen-a", required=True)
    p.add_argument("--listen-b", required=True)
    p.add_argument("--server", required=True)
    p.add_argument("--loss", type=float, default=0.0)
    p.add_argument("--corrupt", type=float, default=0.0)
    p.add_argument("--reorder", type=float, default=0.0)
    p.add_argument("--dup", type=float, default=0.0)
    args = p.parse_args()

    ip_a, port_a = args.listen_a.split(":"); addr_a = (ip_a, int(port_a))
    ip_b, port_b = args.listen_b.split(":"); addr_b = (ip_b, int(port_b))
    srv_ip, srv_port = args.server.split(":"); server_addr = (srv_ip, int(srv_port))

    sock_a = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock_b = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock_a.bind(addr_a)
    sock_b.bind(addr_b)

    # Windows UDP connreset disable
    win_udp_no_connreset(sock_a)
    win_udp_no_connreset(sock_b)

    stop_evt = threading.Event()

    def stop_handler(*_):
        print("\n[EMUL] stopping...")
        stop_evt.set()
        for s in (sock_a, sock_b):
            try:
                s.close()
            except Exception:
                pass

    signal.signal(signal.SIGINT, stop_handler)
    try:
        signal.signal(signal.SIGTERM, stop_handler)
    except Exception:
        pass

    try:
        emulator_loop(sock_a, addr_a, sock_b, addr_b, server_addr, args, stop_evt)
    finally:
        for s in (sock_a, sock_b):
            try:
                s.close()
            except Exception:
                pass
        print("[EMUL] Exit complete.")


if __name__ == "__main__":
    main()
