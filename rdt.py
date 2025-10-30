"""
rdt.py

Selective Repeat reliable data transfer over UDP.

Key points:
- Reliability: CRC32, per-packet timers, selective retransmissions
- Reordering support with in-order delivery to the app
- Very low send rate without congestion control:
  We enforce < 500 bps by using small payloads (32B) and a fixed 0.6s gap between packets.
"""

import socket
import threading
import time
from collections import deque
from typing import Dict, Tuple

from protocol import Packet, MAX_PAYLOAD

DEFAULT_TIMEOUT = 2.0     # generous to tolerate emulator delay + low rate
DEFAULT_WINDOW = 8
SEND_GAP = 0.6            # 0.6s gap -> ~427 bps for 32B payloads


def now() -> float:
    return time.monotonic()


class RDTSession:
    """Per-peer Selective Repeat session."""
    def __init__(self, sock: socket.socket, peer: Tuple[str, int],
                 window: int = DEFAULT_WINDOW, timeout: float = DEFAULT_TIMEOUT):
        self.sock = sock
        self.peer = peer
        self.window = int(window)
        self.timeout = float(timeout)

        # Sender
        self.next_seq = 0
        self.sent: Dict[int, Tuple[bytes, float]] = {}  # seq -> (bytes, last_sent_time)
        self.acked = set()

        # Receiver
        self.expected = 0
        self.recv_buf: Dict[int, bytes] = {}
        self.app_queue = deque()

        self._lock = threading.Lock()
        self._running = True
        self._retx_t = threading.Thread(target=self._retx_loop, daemon=True)
        self._retx_t.start()

    # Sender
    def _retx_loop(self):
        while self._running:
            t = now()
            to_retx = []
            with self._lock:
                for seq, (pkt_bytes, last) in list(self.sent.items()):
                    if seq in self.acked:
                        continue
                    if (t - last) >= self.timeout:
                        to_retx.append(seq)
            for seq in to_retx:
                with self._lock:
                    rec = self.sent.get(seq)
                    if not rec:
                        continue
                    pkt_bytes, _ = rec
                    self.sent[seq] = (pkt_bytes, now())
                try:
                    self.sock.sendto(pkt_bytes, self.peer)
                    print(f"[RDT {self.peer}] RETX seq={seq}")
                except OSError as e:
                    if not self._running:
                        return
                    print(f"[RDT {self.peer}] RETX send OSError: {e}")
                except Exception as e:
                    print(f"[RDT {self.peer}] RETX send error: {e}")
            time.sleep(0.01)

    def send(self, data: bytes):
        """Reliable send with windowing and fixed inter-packet gap."""
        offset = 0
        while offset < len(data):
            chunk = data[offset: offset + MAX_PAYLOAD]

            # wait for space in window
            while True:
                with self._lock:
                    inflight = len([s for s in self.sent if s not in self.acked])
                    if inflight < self.window:
                        seq = self.next_seq
                        self.next_seq += 1
                        break
                time.sleep(0.005)

            pkt = Packet(seq, ack=False, payload=chunk)
            pkt_bytes = pkt.pack()

            # real send
            try:
                self.sock.sendto(pkt_bytes, self.peer)
            except Exception as e:
                print(f"[RDT {self.peer}] send error seq={seq}: {e}")
            # start timer AFTER the real send
            with self._lock:
                self.sent[seq] = (pkt_bytes, now())

            print(f"[RDT {self.peer}] TX seq={seq} len={len(chunk)}")
            offset += len(chunk)

            # enforce very low rate overall
            time.sleep(SEND_GAP)

    def _on_ack(self, ack_seq: int):
        with self._lock:
            if ack_seq in self.sent:
                self.acked.add(ack_seq)
                del self.sent[ack_seq]
                print(f"[RDT {self.peer}] ACK for seq={ack_seq}")

    # Receiver
    def _send_ack(self, seq: int):
        pkt = Packet(seq, ack=True)
        try:
            self.sock.sendto(pkt.pack(), self.peer)
            print(f"[RDT {self.peer}] -> ACK {seq}")
        except OSError as e:
            if not self._running:
                return
            print(f"[RDT {self.peer}] ACK send OSError: {e}")
        except Exception as e:
            print(f"[RDT {self.peer}] ACK send error: {e}")

    def _on_data(self, seq: int, payload: bytes):
        self._send_ack(seq)
        with self._lock:
            if seq < self.expected:
                return
            if seq in self.recv_buf:
                return
            self.recv_buf[seq] = payload
            while self.expected in self.recv_buf:
                piece = self.recv_buf.pop(self.expected)
                self.app_queue.append(piece)
                print(f"[RDT {self.peer}] DELIVER seq={self.expected} ({len(piece)}B)")
                self.expected += 1

    # Demux
    def handle_raw(self, raw: bytes):
        try:
            pkt, ok = Packet.unpack(raw)
        except Exception as e:
            print(f"[RDT {self.peer}] unpack error: {e}")
            return
        if not ok:
            print(f"[RDT {self.peer}] checksum BAD (drop)")
            return
        if pkt.flags & 0x01:
            self._on_ack(pkt.seq_num)
        else:
            self._on_data(pkt.seq_num, pkt.payload)

    # App API
    def recv_available(self) -> bytes:
        out = []
        with self._lock:
            while self.app_queue:
                out.append(self.app_queue.popleft())
        return b"".join(out)

    def stop(self):
        self._running = False
        try:
            self._retx_t.join(timeout=1.0)
        except Exception:
            pass
