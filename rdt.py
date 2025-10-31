"""
rdt.py

Selective Repeat reliable data transfer over UDP.

Key points:
- Reliability: CRC32, per-packet timers, selective retransmissions
- Reordering support with in-order delivery to the app
- Very low send rate without congestion control:
  We enforce < 500 bps by using small payloads (32B) and a fixed 0.6s gap between packets.

Module overview
---------------
This module implements the sender/receiver state machines for a minimal Selective Repeat (SR) protocol over UDP.
It relies on `protocol.Packet` for serialization and integrity (CRC32). The `RDTSession` class encapsulates all per-peer state:

  - Sender side:
      - Sliding window (size = DEFAULT_WINDOW)
      - Per-packet timers and selective retransmissions
      - Throttled send rate (SEND_GAP) to satisfy the sub-500 bps requirement

  - Receiver side:
      - Validates CRC32
      - Sends per-packet ACKs
      - Buffers out-of-order data; delivers in order to the application via a queue
"""

import socket
import threading
import time
from collections import deque
from typing import Dict, Tuple

from protocol import Packet, MAX_PAYLOAD

DEFAULT_TIMEOUT = 2.0     # tolerate emulator delay + low rate
DEFAULT_WINDOW = 8
SEND_GAP = 0.6            # 0.6s gap -> ~427 bps for 32B payloads


def now() -> float:
    """
    Monotonic time helper used for timers (immune to wall-clock changes).

    Returns:
        float: Current monotonic time in seconds.
    """
    return time.monotonic()


class RDTSession:
    """Per-peer Selective Repeat session."""

    def __init__(self, sock: socket.socket, peer: Tuple[str, int],
                 window: int = DEFAULT_WINDOW, timeout: float = DEFAULT_TIMEOUT):
        """
        Initialize session state for a single peer.

        Args:
            sock: UDP socket used for both sending and receiving raw packets.
            peer: (host, port) tuple of the remote endpoint for this session.
            window: Sliding window size (max number of un-ACKed packets).
            timeout: Retransmission timeout (seconds) per sent packet.
        """
        self.sock = sock
        self.peer = peer
        self.window = int(window)
        self.timeout = float(timeout)

        # Sender state
        self.next_seq = 0
        # Map: seq_num -> (serialized_packet_bytes, last_sent_time)
        self.sent: Dict[int, Tuple[bytes, float]] = {}
        self.acked = set()  # set of seq numbers acknowledged by the peer

        # Receiver state
        self.expected = 0                     # next in-order sequence number expected by the app
        self.recv_buf: Dict[int, bytes] = {}  # out-of-order buffer: seq_num -> payload
        self.app_queue = deque()              # in-order delivery queue for the application

        # Concurrency primitives
        self._lock = threading.Lock()
        self._running = True
        self._retx_t = threading.Thread(target=self._retx_loop, daemon=True)
        self._retx_t.start()

    # Sender
    def _retx_loop(self):
        """
        Background retransmission loop.

        Periodically scans the sender's 'sent' map for packets whose timer exceeded `self.timeout`,
        then retransmits them selectively.
        """
        while self._running:
            t = now()
            to_retx = []
            # Identify timed-out packets under lock (no network I/O here).
            with self._lock:
                for seq, (pkt_bytes, last) in list(self.sent.items()):
                    if seq in self.acked:
                        continue  # already acknowledged
                    if (t - last) >= self.timeout:
                        to_retx.append(seq)
            # Perform retransmissions outside the iteration over dict items.
            for seq in to_retx:
                with self._lock:
                    rec = self.sent.get(seq)
                    if not rec:
                        continue  # could have been ACKed concurrently
                    pkt_bytes, _ = rec
                    # Refresh 'last_sent_time' immediately (timer restarts on send attempt).
                    self.sent[seq] = (pkt_bytes, now())
                try:
                    self.sock.sendto(pkt_bytes, self.peer)
                    print(f"[RDT {self.peer}] RETX seq={seq}")
                except OSError as e:
                    # If shutdown started, allow clean exit. Otherwise, log and continue.
                    if not self._running:
                        return
                    print(f"[RDT {self.peer}] RETX send OSError: {e}")
                except Exception as e:
                    print(f"[RDT {self.peer}] RETX send error: {e}")
            time.sleep(0.01)

    def send(self, data: bytes):
        """Reliable send with windowing and fixed inter-packet gap.

        Splits 'data' into chunks of size at most MAX_PAYLOAD, then:
          1) Waits until there is space in the sliding window.
          2) Assigns a new sequence number and packs the chunk into a Packet.
          3) Sends the packet to the peer and starts its retransmission timer.
          4) Sleeps for SEND_GAP to keep the overall rate < 500 bps.

        Args:
            data: Arbitrary bytes to send reliably to the peer.
        """
        offset = 0
        while offset < len(data):
            # Take the next bounded slice to fit the payload limit.
            chunk = data[offset: offset + MAX_PAYLOAD]

            # Wait until the number of in-flight (un-ACKed) packets is below 'window'.
            while True:
                with self._lock:
                    inflight = len([s for s in self.sent if s not in self.acked])
                    if inflight < self.window:
                        seq = self.next_seq
                        self.next_seq += 1
                        break
                time.sleep(0.005)  # brief yield to avoid tight spinning

            # Build the on-wire packet bytes.
            pkt = Packet(seq, ack=False, payload=chunk)
            pkt_bytes = pkt.pack()

            # Perform the actual send to the peer.
            try:
                self.sock.sendto(pkt_bytes, self.peer)
            except Exception as e:
                # Network errors are logged; timer still starts so retx can try again later.
                print(f"[RDT {self.peer}] send error seq={seq}: {e}")

            # Record/refresh the timer after the attempted to send.
            with self._lock:
                self.sent[seq] = (pkt_bytes, now())

            print(f"[RDT {self.peer}] TX seq={seq} len={len(chunk)}")
            offset += len(chunk)

            # Throttle to ensure very low data rate.
            time.sleep(SEND_GAP)

    def _on_ack(self, ack_seq: int):
        """
        Handle an incoming ACK for the given sequence number.

        Removes the sequence from the retransmission map and marks it acknowledged.

        Args:
            ack_seq: Sequence number being acknowledged by the peer.
        """
        with self._lock:
            if ack_seq in self.sent:
                self.acked.add(ack_seq)
                del self.sent[ack_seq]
                print(f"[RDT {self.peer}] ACK for seq={ack_seq}")

    # Receiver
    def _send_ack(self, seq: int):
        """
        Send an ACK packet for the given sequence number.

        Args:
            seq: The sequence number being acknowledged.
        """
        pkt = Packet(seq, ack=True)
        try:
            self.sock.sendto(pkt.pack(), self.peer)
            print(f"[RDT {self.peer}] -> ACK {seq}")
        except OSError as e:
            # During shutdown the socket may be closing; allow graceful exit.
            if not self._running:
                return
            print(f"[RDT {self.peer}] ACK send OSError: {e}")
        except Exception as e:
            print(f"[RDT {self.peer}] ACK send error: {e}")

    def _on_data(self, seq: int, payload: bytes):
        """
        Process a received data packet:
          - Immediately send an ACK for 'seq'.
          - If 'seq' is new and not below the 'expected' pointer, buffer it.
          - Deliver any contiguous data (from 'expected' upward) to the app queue.

        Args:
            seq: Sequence number of the received data packet.
            payload: The data bytes carried by the packet.
        """
        self._send_ack(seq)  # ACK early to suppress sender retransmissions
        with self._lock:
            if seq < self.expected:
                return  # duplicate or already delivered
            if seq in self.recv_buf:
                return  # duplicate (already buffered)
            self.recv_buf[seq] = payload
            # Deliver in order: flush contiguous run starting at 'expected'
            while self.expected in self.recv_buf:
                piece = self.recv_buf.pop(self.expected)
                self.app_queue.append(piece)
                print(f"[RDT {self.peer}] DELIVER seq={self.expected} ({len(piece)}B)")
                self.expected += 1

    # Demux
    def handle_raw(self, raw: bytes):
        """
        Demultiplex a raw UDP datagram into ACK or DATA, verify checksum, and dispatch to the appropriate handler.

        Args:
            raw: Raw datagram bytes as received from the socket.
        """
        try:
            pkt, ok = Packet.unpack(raw)
        except Exception as e:
            # Malformed header or other unpacking failure.
            print(f"[RDT {self.peer}] unpack error: {e}")
            return
        if not ok:
            # CRC mismatch — drop silently after logging.
            print(f"[RDT {self.peer}] checksum BAD (drop)")
            return
        if pkt.flags & 0x01:
            # ACK packet
            self._on_ack(pkt.seq_num)
        else:
            # DATA packet
            self._on_data(pkt.seq_num, pkt.payload)

    # App API
    def recv_available(self) -> bytes:
        """
        Retrieve any in-order bytes available for the application without blocking.

        Returns:
            bytes: Concatenation of all currently queued in-order chunks (maybe empty).
        """
        out = []
        with self._lock:
            while self.app_queue:
                out.append(self.app_queue.popleft())
        return b"".join(out)

    def stop(self):
        """
        Stop the retransmission thread and release resources owned by the session.
        """
        self._running = False
        try:
            self._retx_t.join(timeout=1.0)
        except Exception:
            pass