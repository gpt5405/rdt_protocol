"""
emulator.py

UDP packet emulator to inject loss, corruption, duplication, and reordering.
Works as a simple A<->Server relay.

Module overview
---------------
This tool sits between a client (A) and a real server, forwarding UDP datagrams
in both directions while optionally introducing network impairments. It is used
to test the robustness of a reliable data transfer protocol (e.g., selective
retransmission, checksums, and in-order delivery), by simulating imperfect
network conditions:
  - Loss:      drop selected packets
  - Corruption: flip a random byte inside a packet
  - Duplication: send the same packet twice
  - Reordering: delay one packet briefly to reorder relative to others
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
    """
    Disable Windows-specific UDP "connection reset" behavior for a socket.

    On Windows, when a UDP datagram is sent to a port where nothing is listening,
    the OS may WinError 10054 on subsequent recv calls. This helper disables that behavior.

    Args:
        sock: A bound UDP socket.
    """
    try:
        sock.ioctl(socket.SIO_UDP_CONNRESET, b"\x00\x00\x00\x00")
    except (AttributeError, OSError):
        # Non-Windows or older stacks: the attribute may not exist.
        # If unsupported, proceed without disabling.
        pass


def corrupt(data: bytes) -> bytes:
    """
    Return a copy of 'data' with one randomly chosen byte bit-flipped (XOR 0xFF).

    This simulates payload corruption on the wire.

    Args:
        data: Original datagram bytes.

    Returns:
        Corrupted datagram bytes (or unchanged if empty).
    """
    if not data:
        return data
    i = random.randrange(len(data))   # choose a random byte position
    b = bytearray(data)
    b[i] ^= 0xFF                      # flip all bits in the chosen byte
    return bytes(b)


def emulator_loop(sock_a, addr_a, sock_b, addr_b, server_addr, args, stop_evt: threading.Event):
    """
    Main forwarding loop that applies impairments and relays packets between A and the server.

    Args:
        sock_a: UDP socket bound to the emulator's "A" listening address.
        addr_a: Tuple (ip, port) for the emulator "A" side (for logging).
        sock_b: UDP socket bound to the emulator's "B" listening address.
        addr_b: Tuple (ip, port) for the emulator "B" side (for logging).
        server_addr: Tuple (ip, port) of the actual server destination.
        args: Parsed argparse Namespace containing impairments probabilities.
        stop_evt: Event used to request graceful termination (e.g., on SIGINT).

    Notes:
        - Sockets are set non-blocking and monitored with select.select().
        - All send/recv operations are wrapped to prevent unexpected exceptions
          from exiting the loop; errors are logged and the loop continues.
    """
    last_client = None                # track the last observed client source (A)
    reorder_slot = None               # holds one delayed packet: (data, dst_sock, dst_addr, direction, release_time)

    print(f"[EMUL] A={addr_a}, B={addr_b}, server={server_addr}")
    print(f"[EMUL] loss={args.loss:.2f}, corrupt={args.corrupt:.2f}, reorder={args.reorder:.2f}, dup={args.dup:.2f}")

    for s in (sock_a, sock_b):
        s.setblocking(False)          # non-blocking sockets; poll with select()

    while not stop_evt.is_set():
        try:
            # Wait briefly for readability events on either socket.
            rlist, _, _ = select.select([sock_a, sock_b], [], [], 0.1)
        except Exception:
            # If select() fails transiently, skip this cycle and keep running.
            continue

        now = time.monotonic()

        # If holding a delayed packet for reordering and the delay has elapsed, release it.
        if reorder_slot and now >= reorder_slot[4]:
            pkt, s2, a2, d2, _ = reorder_slot
            try:
                s2.sendto(pkt, a2)
                print(f"[EMUL] SEND delayed {d2}")
            except OSError as e:
                print(f"[EMUL] delayed send OSError: {e}")
            reorder_slot = None

        if not rlist:
            # No readable sockets this iteration; try select() again.
            continue

        for s in rlist:
            try:
                data, src = s.recvfrom(BUF)  # read one datagram
            except (BlockingIOError, InterruptedError):
                # Non-fatal read conditions; just continue polling.
                continue
            except OSError as e:
                # ICMP Port Unreachable can appear here as 10054; log and continue.
                print(f"[EMUL] recv OSError: {e}")
                continue

            if not data:
                # Empty datagram; ignore.
                continue

            # Determine direction and destination for forwarding.
            if s is sock_a:
                # From client-side (A) to server-side.
                last_client = src
                dst_sock, dst_addr = sock_b, server_addr
                direction = "A->Server"
            else:
                # From server-side back to the last observed client.
                if last_client is None:
                    # If no client has been seen yet, we cannot route to A; drop silently.
                    continue
                dst_sock, dst_addr = sock_a, last_client
                direction = "Server->A"

            # Impairments begin
            # Random packet loss.
            if random.random() < args.loss:
                print(f"[EMUL] DROP {direction} len={len(data)}")
                continue

            # Random payload corruption.
            if random.random() < args.corrupt:
                data = corrupt(data)
                print(f"[EMUL] CORRUPT {direction}")

            # Random duplication: send once immediately, continue with main flow as usual.
            if random.random() < args.dup:
                try:
                    dst_sock.sendto(data, dst_addr)
                    print(f"[EMUL] DUP {direction}")
                except OSError as e:
                    print(f"[EMUL] dup send OSError: {e}")

            # Random reordering: hold exactly one packet for a short delay.
            if reorder_slot is None and random.random() < args.reorder:
                reorder_slot = (data, dst_sock, dst_addr, direction, time.monotonic() + random.uniform(0.05, 0.3))
                print(f"[EMUL] REORDER (hold) {direction}")
            else:
                # Normal forwarding path when not delaying for reorder.
                try:
                    dst_sock.sendto(data, dst_addr)
                except OSError as e:
                    print(f"[EMUL] fwd send OSError {direction}: {e}")
                    continue


def main():
    """
    Parse CLI arguments, create/bind emulator sockets, and run the emulator loop.

    Expected usage:
        python emulator.py --listen-a 127.0.0.1:10000 --listen-b 127.0.0.1:10001
            --server 127.0.0.1:12000 --loss 0.1 --corrupt 0.05 --reorder 0.05 --dup 0.02

    Arguments:
        --listen-a: IP:port where the emulator listens for the client ("A") side.
        --listen-b: IP:port where the emulator listens for the server-facing side.
        --server:   IP:port of the actual server that the emulator forwards to.
        --loss:     Probability [0..1] to drop a packet.
        --corrupt:  Probability [0..1] to corrupt a packet.
        --reorder:  Probability [0..1] to hold-and-delay (reorder) a packet.
        --dup:      Probability [0..1] to duplicate a packet (send twice).
    """
    p = argparse.ArgumentParser()
    p.add_argument("--listen-a", required=True)
    p.add_argument("--listen-b", required=True)
    p.add_argument("--server", required=True)
    p.add_argument("--loss", type=float, default=0.0)
    p.add_argument("--corrupt", type=float, default=0.0)
    p.add_argument("--reorder", type=float, default=0.0)
    p.add_argument("--dup", type=float, default=0.0)
    args = p.parse_args()

    # Parse endpoints into (host, port) tuples for binding/forwarding.
    ip_a, port_a = args.listen_a.split(":"); addr_a = (ip_a, int(port_a))
    ip_b, port_b = args.listen_b.split(":"); addr_b = (ip_b, int(port_b))
    srv_ip, srv_port = args.server.split(":"); server_addr = (srv_ip, int(srv_port))

    # Create two UDP sockets: one for the A-side, one for the server-facing side.
    sock_a = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock_b = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock_a.bind(addr_a)
    sock_b.bind(addr_b)

    # Disable ICMP->WSAECONNRESET behavior to avoid 10054 errors.
    win_udp_no_connreset(sock_a)
    win_udp_no_connreset(sock_b)

    stop_evt = threading.Event()

    def stop_handler(*_):
        """
        Handle Ctrl+C to stop the emulator cleanly.
        """
        print("\n[EMUL] stopping...")
        stop_evt.set()
        # Close sockets to unblock any pending select()/recvfrom() calls.
        for s in (sock_a, sock_b):
            try:
                s.close()
            except Exception:
                pass

    # Register signal handlers for graceful stop.
    signal.signal(signal.SIGINT, stop_handler)
    try:
        signal.signal(signal.SIGTERM, stop_handler)
    except Exception:
        pass

    try:
        # Enter the main forwarding loop; returns when stop_evt is set.
        emulator_loop(sock_a, addr_a, sock_b, addr_b, server_addr, args, stop_evt)
    finally:
        # Ensure sockets are closed on exit.
        for s in (sock_a, sock_b):
            try:
                s.close()
            except Exception:
                pass
        print("[EMUL] Exit complete.")


if __name__ == "__main__":
    main()
