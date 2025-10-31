"""
protocol.py

This module defines the packet structure used by the Reliable Data Transfer (RDT) protocol.
It provides:

- A fixed binary header format shared by both sender and receiver.
- Support for packing and unpacking packets into raw bytes suitable for UDP.
- A CRC32 checksum for data integrity validation.
- A compact payload limit chosen to enforce low data rate (< 500 bps) when combined
  with the enforced send delay in `rdt.py`.
"""

import struct
import zlib
from typing import Tuple

# Packet format constants
# struct format:  !I B H I
#  - !  -> network byte order (big endian)
#  - I  -> 4-byte unsigned int (sequence number)
#  - B  -> 1 byte (flags: ACK bit)
#  - H  -> 2-byte unsigned short (payload length in bytes)
#  - I  -> 4-byte unsigned int (CRC32 checksum)
HEADER_FMT = "!I B H I"
HEADER_SIZE = struct.calcsize(HEADER_FMT)

# Maximum payload size (in bytes).
# Intentionally very small to force < 500 bps when combined with SEND_GAP in rdt.py.
MAX_PAYLOAD = 32

# Bit mask for the ACK flag inside the flags byte.
FLAG_ACK = 0x01

class Packet:
    """
    Represents a single RDT protocol packet, containing:

    - Sequence number
    - ACK flag (data or acknowledgement)
    - Variable-length payload (0 to MAX_PAYLOAD bytes)
    - CRC32 checksum for full data integrity

    The class supports packing/unpacking into raw bytes suitable for UDP
    transmission.
    """

    def __init__(self, seq_num: int, ack: bool, payload: bytes = b""):
        """
        Initialize a packet instance.

        :param seq_num: Integer sequence number (monotonically increasing per sender)
        :param ack: True if this packet is an ACK, False if carries data
        :param payload: Raw bytes payload (may be empty for ACK-only packets)
        """
        self.seq_num = int(seq_num)
        self.flags = FLAG_ACK if ack else 0  # store ACK bit if needed
        self.payload = payload or b""        # empty payload becomes b"", never None
        self.length = len(self.payload)      # number of payload bytes
        self.checksum = 0                    # filled during packing

    def pack(self) -> bytes:
        """
        Serialize the packet into raw bytes suitable for sending via UDP.
        CRC32 checksum is computed over the header (without checksum field) + payload.

        :return: Raw bytes that represent the packet on the wire.
        """
        # Build header *without* checksum first (used for CRC computation)
        head_wo = struct.pack("!I B H", self.seq_num, self.flags, self.length)

        # Compute checksum over header_without_checksum + payload
        self.checksum = zlib.crc32(head_wo + self.payload) & 0xFFFFFFFF

        # Pack full header including checksum, then append payload
        return struct.pack(HEADER_FMT, self.seq_num, self.flags, self.length, self.checksum) + self.payload

    @staticmethod
    def unpack(buf: bytes) -> Tuple["Packet", bool]:
        """
        Parse raw bytes into a Packet object and verify checksum integrity.

        :param buf: Raw UDP bytes received
        :return: (Packet object, checksum_valid flag)
        :raises ValueError: if buffer is too small to contain a header
        """
        if len(buf) < HEADER_SIZE:
            raise ValueError("buffer too small for packet header")

        # Extract header fields first
        seq, flags, length, checksum = struct.unpack(HEADER_FMT, buf[:HEADER_SIZE])

        # Extract payload portion based on declared length
        payload = buf[HEADER_SIZE:HEADER_SIZE + length]

        # Recompute checksum to compare with received value
        head_wo = struct.pack("!I B H", seq, flags, length)
        good = (zlib.crc32(head_wo + payload) & 0xFFFFFFFF) == checksum

        # Build Packet object from fields
        pkt = Packet(seq, bool(flags & FLAG_ACK), payload)
        pkt.checksum = checksum
        return pkt, good

    def __repr__(self):
        """Readable debug representation of the packet (shown in logging)."""
        return f"<Packet seq={self.seq_num} ack={(self.flags & FLAG_ACK)!=0} len={self.length} chk=0x{self.checksum:08x}>"
