# HW3 - Reliable Data Transfer

This repo contains a working, minimal, **Selective Repeat** RDT over UDP, a network **emulator** that can drop/corrupt/duplicate/reorder packets, and a simple **file transfer** client/server built on the RDT layer.

## Included
- `protocol.py` - Packet format + CRC32 checksum
- `rdt.py` - Selective Repeat RDT (per-packet timers), very low send rate (< 500 bps)
- `emulator.py` - UDP relay that injects loss/corruption/reordering/duplication
- `server.py` - Single-thread RDT file server (GET/PUT)
- `client.py` - RDT client (GET/PUT)
- `requirements.txt` - Package requirements list
- `revisions.txt` - Git logs
- `rdt_report.pdf` - Screenshots showing functional operation
- `sphinx_report.pdf` - Auto-generated code docs PDF

**Python**: 3.9+ recommended. No external dependencies.

---

## Description
- **Reliability**: `rdt.py` implements **Selective Repeat**:
  - sequence numbers grow monotonically
  - receiver buffers out-of-order data, delivers in order
  - per-packet timers trigger retransmissions
  - CRC32 detects corruption; corrupted packets are silently dropped
- **Rate limiting**: Each UDP send sleeps so the average TX rate is ~**400 bits/s** (below the 500 bps guidance).
- **Emulator**: `emulator.py` binds two ports (A: client side, B: server side) and forwards with optional loss/corruption/dup/ reordering.

---

## Run the Demo Locally

### 1) Start the server
```
python server.py --host 127.0.0.1 --port 12000
```
### 2) Start the emulator
```
python emulator.py --listen-a 127.0.0.1:10000 --listen-b 127.0.0.1:10001 --server 127.0.0.1:12000 --loss 0.10 --corrupt 0.05 --reorder 0.05 --dup 0.02
```
### 3) Client GET/PUT commands
```
python client.py --server 127.0.0.1:12000 --emulator 127.0.0.1:10000 GET test.txt
```
writes download_test.txt
```
python client.py --server 127.0.0.1:12000 --emulator 127.0.0.1:10000 PUT sample.bin
```
server responds "OK: Stored sample.bin (N bytes)"

---
## Run the Demo for PDF Report
### 1) Packets Corrupted
Goal: Show `[EMUL] CORRUPT` messages and the transfer still completes (client receives full file).

#### Terminal 1 (Server)
```
python server.py --host 127.0.0.1 --port 12000
```

#### Terminal 2 (Emulator)
```
python emulator.py --listen-a 127.0.0.1:10000 --listen-b 127.0.0.1:10001 --server 127.0.0.1:12000 --corrupt 0.20
```
*(Set only corruption high — 20%)*

#### Terminal 3 (Client)
```
python client.py --server 127.0.0.1:12000 --emulator 127.0.0.1:10000 GET test.txt
```

---
### 2) Packets Lost
Goal: Show `[EMUL] DROP` messages and the transfer still completes (client receives full file).

#### Terminal 1 (Server)
```
python server.py --host 127.0.0.1 --port 12000
```

#### Terminal 2 (Emulator)
```
python emulator.py --listen-a 127.0.0.1:10000 --listen-b 127.0.0.1:10001 --server 127.0.0.1:12000 --loss 0.20
```
*(Set only loss high — 20%)*

#### Terminal 3 (Client)
```
python client.py --server 127.0.0.1:12000 --emulator 127.0.0.1:10000 GET test.txt
```

---
### 3) Packets Reordered
Goal: Show `[EMUL] REORDER` messages and the transfer still completes (client receives full file).

#### Terminal 1 (Server)
```
python server.py --host 127.0.0.1 --port 12000
```

#### Terminal 2 (Emulator)
```
python emulator.py --listen-a 127.0.0.1:10000 --listen-b 127.0.0.1:10001 --server 127.0.0.1:12000 --reorder 0.40
```
*(Set only reorder high — 40%)*

#### Terminal 3 (Client)
```
python client.py --server 127.0.0.1:12000 --emulator 127.0.0.1:10000 GET test.txt
```

---
### 4) Successful File Write/Read (PUT + GET)
Goal: 
- Server show: `[SERVER] stored test.txt (...) bytes` and `[SERVER] sending test.txt (...) bytes`
- Client: `[CLIENT] wrote download_test.txt (...) bytes`

#### Terminal 1 (Server)
```
python server.py --host 127.0.0.1 --port 12000
```

#### Terminal 2 (Emulator)
```
python emulator.py --listen-a 127.0.0.1:10000 --listen-b 127.0.0.1:10001 --server 127.0.0.1:12000
```

#### Terminal 3 (Client - PUT test upload)
```
python client.py --server 127.0.0.1:12000 --emulator 127.0.0.1:10000 PUT test.txt
```

#### Terminal 3 (Client - GET same file)
```
python client.py --server 127.0.0.1:12000 --emulator 127.0.0.1:10000 GET test.txt
```