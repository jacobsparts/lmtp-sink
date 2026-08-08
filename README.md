# lmtp-sink & lmtp-drain

A minimal, robust suite of utilities for handling emergency LMTP message dead-lettering and automatic spool draining:

- `lmtp-sink`: Lightweight LMTP emergency dead-letter daemon that preserves messages to a local spool directory when the primary LMTP server is down.
- `lmtp-drain`: One-shot utility invoked via cron or systemd timers to deliver completed spool records back to the primary LMTP server (e.g. Dovecot) once it becomes available.

---

## 1. lmtp-sink

`lmtp-sink` serves as Postfix's `lmtp_fallback_relay`. It accepts messages over LMTP, writes them durably to disk in a simple format, and returns success to Postfix so mail is not lost during primary endpoint outages.

### Features & Guarantees

- **Emergency Dead-Letter Sink**: Pure dead-letter receiver; no store-and-forward, retries, bounces, or DSN generation.
- **Strict Durability & Atomicity**:
  - Message transactions write incrementally to hidden temporary files (`.YYYYMMDDTHHMMSSffffffZ.tmp`).
  - Flushes buffers and issues `fsync` on the file prior to closing.
  - Atomically renames temporary files to final spool records (`YYYYMMDDTHHMMSSffffffZ.spool`).
  - Issues `fsync` on the spool directory to ensure filesystem metadata durability.
  - Returns `250` success responses to the LMTP client *only* after directory synchronization completes.
- **Free-Space Protection**: Performs a pre-`DATA` filesystem space check (`statvfs`) and rejects `DATA` with `452 4.3.1 Insufficient system storage` if available disk space is below the configured threshold (default 100 MiB).
- **Dot-Stuffing Handling**: Reconstructs original message data by removing LMTP dot-stuffing (`..` -> `.`) while preserving received line endings.

### `lmtp-sink` Usage

```text
lmtp-sink [options]

Options:
  -l, --listen-addr <addr>     Listen address (default: 127.0.0.1)
  -p, --listen-port <port>     Listen port (default: 2526)
  -s, --spool-dir <path>       Spool directory (default: /var/spool/lmtp-sink)
  -m, --min-free-bytes <bytes> Minimum free bytes required (default: 104857600 = 100 MiB)
  -h, --help                   Show help message
```

### Postfix Integration

**Option A: Global Fallback Relay**
Add to `/etc/postfix/main.cf`:

```postfix
lmtp_fallback_relay = 127.0.0.1:2526
```

**Option B: Destination-Specific Transport Fallback (Recommended)**
Add to `/etc/postfix/master.cf`:

```postfix
roundcube-lmtp unix - - y - - lmtp
  -o lmtp_fallback_relay=127.0.0.1:2526
```

And in `/etc/postfix/transport`:

```postfix
roundcube.jphq.net    roundcube-lmtp:10.7.1.3:24
```

Then reload Postfix (`postfix reload`).

---

## 2. lmtp-drain

`lmtp-drain` is a one-shot utility that recovers completed `.spool` records and delivers them to the downstream LMTP host (e.g. Dovecot) over WireGuard or local network.

### Record States & Life-Cycle

- **`YYYYMMDDTHHMMSSffffffZ.spool`**: Pending record. Processed by `lmtp-drain` in chronological order.
- **Deleted**: Successfully delivered to downstream LMTP server (all recipients returned `2xx`). Deleted immediately after delivery.
- **`YYYYMMDDTHHMMSSffffffZ.failed`**: Malformed record or rejected by downstream LMTP server. Renamed to `.failed` for manual inspection; never retried automatically.

### Draining Invariants & Rules

- **Initial Downstream Check**: Validates downstream LMTP availability (`220` greeting & `LHLO`) before modifying or renaming any spool records.
- **Single-Instance Locking**: Uses non-blocking `flock` on `/var/spool/lmtp-sink/.drain.lock`. If another drain process is active, it exits immediately without doing work.
- **Network Failure Retention**: If a network error or socket I/O failure occurs, processing aborts immediately and remaining records stay `.spool` to be retried on the next scheduled run.
- **Rejection & Protocol Failure Handling**: Downstream LMTP rejection (`4xx`/`5xx`), malformed protocol responses, or malformed record headers cause the file to be renamed to `.failed`, after which processing continues with the next record.
- **Dot-Stuffing**: Automatically re-applies LMTP dot-stuffing (`.` -> `..`) when streaming message payload to the downstream host.

### `lmtp-drain` Usage

```text
lmtp-drain [options]

Options:
  -s, --spool-dir <path>       Spool directory (default: /var/spool/lmtp-sink)
  -H, --host <host-or-address> LMTP host (default: 10.7.1.3)
  -p, --port <port>            LMTP port (default: 24)
  -n, --lhlo-name <name>       Client LHLO hostname (default: mail.jacobstoner.com)
  -h, --help                   Show help message
```

### Cron / Systemd Scheduling

Add a cron entry to run `lmtp-drain` periodically (e.g. every 5 minutes):

```cron
*/5 * * * * /usr/local/sbin/lmtp-drain --spool-dir /var/spool/lmtp-sink --host 10.7.1.3 --port 24 --lhlo-name mail.jacobstoner.com
```

---

## 3. Spool Record Format

Spool files reside in `/var/spool/lmtp-sink/`:

```text
MAIL FROM:<sender@example.org> SIZE=12345 BODY=8BITMIME
RCPT TO:<jacob@roundcube.jphq.net>
RCPT TO:<support@roundcube.jphq.net>
RECEIVED AT:2026-03-21T04:15:23.123456Z

[unescaped message bytes]
```

---

## 4. Building & Cross-Compilation

### Native Build

```bash
cargo build --release
```

Produces `target/release/lmtp-sink` and `target/release/lmtp-drain`.

### Static i686 Cross-Compilation (32-bit Linux / musl)

```bash
rustup target add i686-unknown-linux-musl
cargo build --target i686-unknown-linux-musl --release
```

Produces fully static, zero-dependency binaries in `target/i686-unknown-linux-musl/release/`.

---

## License

MIT / Apache 2.0
