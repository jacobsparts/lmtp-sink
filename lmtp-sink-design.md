# LMTP Emergency Sink Design Specification

## 1. Purpose

The LMTP emergency sink is a small, local service that preserves messages when the primary LMTP endpoint is unreachable.

It is a dead-letter sink, not a store-and-forward mail server. After accepting and storing a message, it does not:

- Retry delivery
- Forward the message
- Expire or delete the message
- Generate delivery-status notifications
- Validate recipients
- Deduplicate messages
- Send alerts
- Provide automatic recovery

Recovery, if ever required, is manual.

## 2. Delivery topology

Normal delivery:

    Postfix LMTP client -> Dovecot

Emergency delivery:

    Postfix LMTP client -> local sink at 127.0.0.1:2526 -> flat spool directory

Postfix uses the local sink as the `lmtp_fallback_relay` for the primary LMTP

The sink binds only to:

    127.0.0.1:2526

It is not reachable from any external interface.

## 3. Scope

The sink shall:

1. Implement enough LMTP to receive messages from the local Postfix LMTP client.
2. Accept every syntactically valid envelope sender.
3. Accept every syntactically valid envelope recipient.
4. Receive the message sent with `DATA`.
5. Remove LMTP dot-stuffing from the message data.
6. Store one transaction record in a flat directory.
7. Durably commit the record before reporting success.
8. Return one final LMTP status for each accepted recipient.
9. Continue accepting later transactions until the client disconnects or issues `QUIT`.

The sink shall be single-threaded and process one connection at a time.

## 4. Explicit non-goals

The sink shall not implement:

- SMTP service
- Public network access
- TLS
- Authentication
- Recipient lookup
- Domain lookup
- DNS or MX routing
- Message forwarding
- Delivery retries
- Queue scheduling
- Queue expiration
- Bounce generation
- DSN generation
- Deduplication
- Maildir semantics
- Message indexing
- Message deletion
- Alerting
- Administrative UI
- Configurable message-size limits
- Configurable recipient-count limits
- Application-level traffic quotas

Postfix is responsible for enforcing its normal upstream message-size policy before attempting LMTP delivery.

## 5. Spool directory

The default spool directory shall be:

    /var/spool/lmtp-sink/

All completed records shall be ordinary files directly inside this directory. No `tmp`, `new`, or `cur` subdirectories are required.

Temporary files used during atomic creation shall also reside in this directory and shall have names beginning with a dot.

The service shall run as an unprivileged account with write access to this directory.

## 6. Free-space requirement

The sink shall not begin receiving message data unless the filesystem containing the spool directory has at least:

    100 MiB = 104857600 bytes

available to the service.

The free-space check shall occur immediately before the sink sends the `354` response to `DATA`.

If less than 100 MiB is available, the sink shall reject `DATA` with a temporary failure:

    452 4.3.1 Insufficient system storage

No spool file shall be created for that transaction.

A successful pre-`DATA` free-space check does not replace normal write-error handling. If a write later fails because the filesystem becomes full or for any other reason, the sink shall report temporary failure after `DATA` and shall not report successful delivery.

## 7. Filename format

Completed records shall use a UTC timestamp as the filename:

    YYYYMMDDTHHMMSSffffffZ.spool

Example:

    20260321T041523123456Z.spool

The timestamp includes microseconds and is generated when the spool transaction begins.

Temporary files shall use the corresponding hidden name:

    .YYYYMMDDTHHMMSSffffffZ.tmp

The implementation shall create temporary files with exclusive-create semantics.

If a timestamp filename already exists, the sink shall generate a new current UTC timestamp and retry. It shall never overwrite an existing temporary or completed spool file.

UUIDs and random filename components are not required.

## 8. Stored transaction format

Each completed spool file shall contain:

    MAIL FROM:<sender@example.org>
    RCPT TO:<jacob@roundcube.jphq.net>
    RCPT TO:<support@roundcube.jphq.net>
    RECEIVED AT:2026-03-21T04:15:23.123456Z

    [unescaped message bytes]

The exact layout is:

1. One `MAIL FROM:` line containing the complete argument from the accepted LMTP `MAIL FROM` command.
2. One `RCPT TO:` line for each accepted recipient, in command order, containing the complete argument from the accepted `RCPT TO` command.
3. One `RECEIVED AT:` line containing the UTC receipt time in ISO 8601 format with six fractional-second digits and a trailing `Z`.
4. One empty line.
5. The message bytes reconstructed from the LMTP `DATA` section after removal of LMTP dot-stuffing and exclusion of the final dot-terminator line.

Example with a null envelope sender:

    MAIL FROM:<>
    RCPT TO:<jacob@roundcube.jphq.net>
    RECEIVED AT:2026-03-21T04:15:23.123456Z

    [unescaped message bytes]

The sink shall preserve any parameters present in the accepted envelope command arguments. For example:

    MAIL FROM:<sender@example.org> SIZE=12345 BODY=8BITMIME
    RCPT TO:<jacob@roundcube.jphq.net> NOTIFY=FAILURE

The custom envelope preamble is not part of the original email message. The stored format is deliberately private and need not conform to mbox, Maildir, or RFC message-file conventions.

## 9. Message-data handling

LMTP `DATA` is line-oriented.

The sink shall:

- Read data until a line containing only `.` followed by the LMTP line ending.
- Exclude that terminator line from the stored message.
- Convert each data line beginning with `..` to a line beginning with `.`.
- Preserve all other message data.
- Preserve message line endings consistently as received from the LMTP client.
- Write message data incrementally to disk rather than retaining the complete message in memory.

The sink stores the reconstructed message, not a literal bidirectional LMTP session transcript.

## 10. Atomicity and durability

The sink shall not report successful delivery until the complete transaction record has been committed.

For each transaction it shall:

1. Generate the timestamp filename.
2. Exclusively create the hidden temporary file in the spool directory.
3. Write the envelope preamble.
4. Write the unescaped message data incrementally.
5. Flush userspace buffers.
6. Call `fsync()` on the temporary file.
7. Close the temporary file.
8. Atomically rename it to the final `.spool` filename in the same directory.
9. Call `fsync()` on the spool directory.
10. Return one successful LMTP final response for every accepted recipient.

If any create, write, flush, synchronization, close, or rename operation fails:

- The sink shall not return a successful status.
- It shall return one temporary-failure status for every accepted recipient.
- It shall close and remove the temporary file when possible.
- An unremovable temporary file may remain for manual inspection.
- It shall never rename a known-incomplete transaction to a `.spool` filename.

A completed `.spool` file is therefore the service's indication that it accepted ownership of the transaction.

## 11. LMTP protocol behavior

### 11.1 Greeting

On connection, the sink shall send:

    220 2.0.0 lmtp-sink ready

### 11.2 LHLO

The client must issue `LHLO` before beginning a mail transaction.

The sink shall return a valid multiline `250` response. It shall advertise only capabilities it actually implements.

No optional extension needs to be advertised.

A minimal response is:

    250-lmtp-sink
    250 8BITMIME

`8BITMIME` may be omitted if the implementation does not need or advertise it. The sink shall not advertise `PIPELINING`, `CHUNKING`, `DSN`, `SMTPUTF8`, `STARTTLS`, or authentication unless those features are deliberately implemented later.

### 11.3 MAIL FROM

After `LHLO`, the sink shall accept a syntactically valid `MAIL FROM` command, including:

    MAIL FROM:<>

It shall preserve the complete accepted argument for the spool preamble.

Success response:

    250 2.1.0 Sender accepted

A new `MAIL FROM` command starts a new transaction and clears any prior transaction state that was not committed.

### 11.4 RCPT TO

After `MAIL FROM`, the sink shall accept every syntactically valid `RCPT TO` command.

It shall preserve each complete accepted argument in command order.

Success response:

    250 2.1.5 Recipient accepted

At least one accepted recipient is required before `DATA`.

### 11.5 DATA

After at least one accepted recipient, `DATA` triggers the free-space check.

If at least 100 MiB is available, the sink shall respond:

    354 Send message data; end with <CRLF>.<CRLF>

It shall then receive and persist the transaction as specified above.

After a successful durable commit, LMTP requires one final response for each accepted recipient. For example, with two recipients:

    250 2.0.0 Stored as 20260321T041523123456Z.spool
    250 2.0.0 Stored as 20260321T041523123456Z.spool

If persistence fails after `354`, it shall return one temporary response for each accepted recipient:

    451 4.3.0 Unable to store message

After all final per-recipient responses have been sent, transaction state shall be cleared.

### 11.6 RSET

`RSET` shall discard the current uncommitted envelope state and return:

    250 2.0.0 Reset

If no `DATA` transfer is in progress, no completed spool file is affected.

### 11.7 NOOP

`NOOP` shall return:

    250 2.0.0 OK

### 11.8 QUIT

`QUIT` shall return:

    221 2.0.0 Bye

The sink shall then close the connection.

### 11.9 Unsupported or malformed commands

Unsupported commands shall receive:

    502 5.5.1 Command not implemented

Malformed commands shall receive:

    501 5.5.2 Syntax error

Commands issued out of sequence shall receive:

    503 5.5.1 Bad sequence of commands

## 12. Transaction and connection behavior

The sink shall support multiple sequential transactions on one connection.

It need not support concurrent connections. While one client is connected, later connection attempts may wait in the operating system's listen queue.

If a client disconnects:

- Before `DATA`, the current envelope state is discarded.
- During `DATA`, the incomplete temporary file is closed and removed when possible.
- After the record has been committed but before the final status is received by Postfix, the completed file remains in the spool.

The last case can cause Postfix to retry and produce another spool file. This is acceptable. No deduplication is required.

## 13. Error policy

Errors before `DATA` shall use an appropriate `4xx` or `5xx` LMTP response.

Errors after the sink has accepted `DATA` shall produce one final response per accepted recipient.

Storage-related errors are temporary and shall use `4xx`, so Postfix retains responsibility for the message.

The sink shall return `250` only when the completed `.spool` file has been durably committed.

The process shall log protocol and storage errors through standard error or the system journal, but no alerting mechanism is part of this design.

## 14. redacted

## 15. Process model

The implementation shall:

- Run as a dedicated unprivileged user.
- Bind only to `127.0.0.1`.
- Listen on TCP port `2526`.
- Use a single-threaded blocking or event-loop process.
- Handle one active connection at a time.
- Write only to the configured spool directory.
- Require no outbound network access.
- Require no database.

Configuration may be limited to constants or command-line options for:

- Listen address
- Listen port
- Spool directory
- Minimum free bytes

Defaults:

    listen address: 127.0.0.1
    listen port: 2526
    spool directory: /var/spool/lmtp-sink
    minimum free bytes: 104857600

## 16. Manual recovery contract

No recovery utility is included in the sink.
Completed files shall not be modified or deleted automatically.

## 17. Acceptance criteria

The implementation is complete when all of the following are demonstrated:

1. It listens only on `127.0.0.1:2526`.
2. Postfix can complete an LMTP transaction with it.
3. It accepts a null or non-null envelope sender.
4. It accepts multiple recipients.
5. It creates exactly one completed spool file per successful transaction.
6. The file contains the required envelope preamble and message.
7. Dot-stuffed message lines are stored correctly unescaped.
8. It sends one final LMTP response per recipient.
9. It does not return success before file and directory synchronization completes.
10. A write failure produces temporary failure rather than success.
11. Less than 100 MiB free space causes `DATA` to be rejected temporarily.
12. Existing spool files are never overwritten.
13. A disconnect during `DATA` does not produce a completed `.spool` file.
14. Multiple sequential transactions on one connection produce separate files.
15. No forwarding, retry, expiration, deduplication, recipient validation, or alerting behavior exists.
