"""
protocol.py

Packet format + checksum helpers used by the RDT layer.

Header (big-endian):
- seq_num: 4B unsigned
- flags:   1B  (bit0 = ACK)
- length:  2B  (payload length)
- cksum:   4B  (CRC32 over header_without_cksum + payload)
"""

import struct
import zlib
from typing import Tuple

HEADER_FMT = "!I B H I"
HEADER_SIZE = struct.calcsize(HEADER_FMT)

# Keep packets tiny so fixed SEND_GAP in rdt.py yields < 500 bps overall.
# With MAX_PAYLOAD=32 bytes (256 bits) and 0.6s gap, ~427 bps average.
MAX_PAYLOAD = 32

FLAG_ACK = 0x01


class Packet:
    def __init__(self, seq_num: int, ack: bool, payload: bytes = b""):
        self.seq_num = int(seq_num)
        self.flags = FLAG_ACK if ack else 0
        self.payload = payload or b""
        self.length = len(self.payload)
        self.checksum = 0

    def pack(self) -> bytes:
        head_wo = struct.pack("!I B H", self.seq_num, self.flags, self.length)
        self.checksum = zlib.crc32(head_wo + self.payload) & 0xFFFFFFFF
        return struct.pack(HEADER_FMT, self.seq_num, self.flags, self.length, self.checksum) + self.payload

    @staticmethod
    def unpack(buf: bytes) -> Tuple["Packet", bool]:
        if len(buf) < HEADER_SIZE:
            raise ValueError("buffer too small")
        seq, flags, length, checksum = struct.unpack(HEADER_FMT, buf[:HEADER_SIZE])
        payload = buf[HEADER_SIZE:HEADER_SIZE + length]
        head_wo = struct.pack("!I B H", seq, flags, length)
        good = (zlib.crc32(head_wo + payload) & 0xFFFFFFFF) == checksum
        pkt = Packet(seq, bool(flags & FLAG_ACK), payload)
        pkt.checksum = checksum
        return pkt, good

    def __repr__(self):
        return f"<Packet seq={self.seq_num} ack={(self.flags & FLAG_ACK)!=0} len={self.length} chk=0x{self.checksum:08x}>"
