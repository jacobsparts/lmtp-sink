# lmtp-sink

`lmtp-sink` is a lightweight, single-threaded LMTP emergency dead-letter sink daemon. It preserves incoming email messages to a local spool directory when a primary LMTP endpoint (such as Dovecot) is unreachable.

It is designed specifically to serve as Postfix's `lmtp_fallback_relay`. It accepts messages, writes them durably to disk in an explicit transaction format, and returns success to Postfix so messages are not lost during primary endpoint outages.

## Features & Guarantees

- **Emergency Dead-Letter Sink**: Pure dead-letter receiver; no store-and-forward, forwarding, retries, bounces, or delivery status notifications (DSNs).
- **Strict Durability & Atomicity**:
  - Message transactions are written incrementally to temporary files (`.YYYYMMDDTHHMMSSffffffZ.tmp`).
  - Flushes buffers and issues `fsync` on the file prior to closing.
  - Atomically renames temporary files to final spool records (`YYYYMMDDTHHMMSSffffffZ.spool`).
  - Issues `fsync` on the spool directory to ensure filesystem directory entry durability.
  - Returns `250` success responses to the LMTP client *only* after directory synchronization completes.
- **Free-Space Protection**: Performs a pre-`DATA` filesystem space check (`statvfs`) and rejects `DATA` with `452 4.3.1 Insufficient system storage` if available disk space is below the configured threshold (default 100 MiB).
- **Dot-Stuffing Handling**: Reconstructs original message data by removing LMTP dot-stuffing (`..` -> `.`) while preserving received line endings.
- **Zero External Dependencies**: Can be compiled as a static i686/x86_64 binary using `musl`, running efficiently on unprivileged system accounts with low memory footprint (< 1 MB).

## Spool Record Format

Spool files reside directly in the configured spool directory with microsecond UTC timestamps:

```text
/var/spool/lmtp-sink/20260321T041523123456Z.spool
```

Each completed record contains envelope preamble metadata followed by the message body:

```text
MAIL FROM:<sender@example.org> SIZE=12345 BODY=8BITMIME
RCPT TO:<jacob@roundcube.jphq.net>
RCPT TO:<support@roundcube.jphq.net>
RECEIVED AT:2026-03-21T04:15:23.123456Z

[unescaped message bytes]
```

## Command Line Usage

```text
lmtp-sink [options]

Options:
  -l, --listen-addr <addr>     Listen address (default: 127.0.0.1)
  -p, --listen-port <port>     Listen port (default: 2526)
  -s, --spool-dir <path>       Spool directory (default: /var/spool/lmtp-sink)
  -m, --min-free-bytes <bytes> Minimum free bytes required (default: 104857600 = 100 MiB)
  -h, --help                   Show help message
```

## Postfix Integration

To configure Postfix to use `lmtp-sink` as a fallback relay when your primary LMTP server (e.g. Dovecot) is unavailable, add the following to `/etc/postfix/main.cf`:

```postfix
lmtp_fallback_relay = inet:127.0.0.1:2526
```

Then reload Postfix:

```bash
postfix reload
```

## Building

### Prerequisites

- Rust (cargo 1.70+)

### Native Build

```bash
cargo build --release
```

The compiled binary will be placed at `target/release/lmtp-sink`.

### Static i686 Cross-Compilation (for 32-bit Linux)

```bash
rustup target add i686-unknown-linux-musl
cargo build --target i686-unknown-linux-musl --release
```

The resulting binary at `target/i686-unknown-linux-musl/release/lmtp-sink` is a 100% statically linked 32-bit ELF executable that runs on any i686 Linux system.

## License

MIT / Apache 2.0
