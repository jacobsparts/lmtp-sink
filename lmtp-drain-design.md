# LMTP Sink Drainer Design Specification

## 1. Purpose

The LMTP sink drainer is a small, one-shot utility that recovers completed sink records after the downstream Dovecot LMTP host becomes available again.

The intended topology is:

    Postfix -> Dovecot LMTP at <dovecot-host>:24

When Dovecot is unreachable:

    Postfix -> local lmtp-sink -> /var/spool/lmtp-sink/*.spool

Recovery path:

    periodic cron or systemd timer
        -> lmtp-drain
        -> Dovecot LMTP at <dovecot-host>:24

The drainer exists specifically to mitigate an unavailable downstream host. Once Postfix has routed a message to `<private-mail-domain>`, that message is expected to be deliverable by Dovecot.

The drainer is not a general-purpose MTA or SMTP-style deferred-delivery queue. It does not interpret LMTP rejections as reasons to schedule repeated delivery attempts. Only network failures leave a record pending for a later run.

Each invocation scans the spool once and exits. A scheduler invokes it again later.

## 2. Record states

The filename suffix is the complete persistent state:

    YYYYMMDDTHHMMSSffffffZ.spool
    YYYYMMDDTHHMMSSffffffZ.failed

Meanings:

- `.spool`: pending automatic delivery.
- `.failed`: the record was malformed or the responsive downstream LMTP server did not accept it; manual inspection is required.
- absent: delivery succeeded and the original record was deleted.

The drainer shall scan only `.spool` files.

A `.failed` file shall never be retried automatically, modified, or deleted by the drainer.

No sidecar state, retry counter, recipient status file, database, or delivery history is required.

## 3. Primary behavioral contract

The drainer shall:

1. Acquire the spool lock.
2. Scan and sort completed `.spool` records.
3. Exit successfully without contacting Dovecot if no records exist.
4. Establish a real LMTP session with Dovecot before processing records.
5. Exit immediately without changing any records if Dovecot cannot be reached or the LMTP session is interrupted by a network error.
6. Parse and submit each record using its stored envelope sender and recipients.
7. Rename a malformed record to `.failed` and continue.
8. Rename a record to `.failed` when the responsive LMTP server returns any non-success or invalid protocol response, then continue with later records.
9. Delete a `.spool` file only after every recipient receives an unambiguous final `2xx` LMTP response.
10. Abort and exit immediately on any local filesystem error.

A network failure leaves the current record as `.spool` for the next scheduled run.

A downstream LMTP rejection is not retried. It changes the record to `.failed`.

## 4. Explicit non-goals

The drainer shall not implement:

- SMTP delivery
- SMTP-style deferred delivery
- DNS or MX routing
- Alternate destinations
- Recipient rewriting
- Sender rewriting
- Message modification
- Queue expiration
- Bounce generation
- DSN generation
- Deduplication
- Per-recipient persistent delivery state
- Retry counters
- Retry scheduling within the process
- A resident daemon or event loop
- Parallel delivery
- Alerting
- A database
- A web interface
- Maildir semantics
- Automatic handling of `.failed` records
- Cleanup of incomplete hidden sink files
- Recovery from local filesystem errors within the same run

Scheduling is external, normally through cron or a systemd timer.

## 5. Default configuration

Defaults:

    spool directory:  /var/spool/lmtp-sink
    LMTP host:        <dovecot-host> (Required CLI parameter)
    LMTP port:        24
    client LHLO name: <mail-server-hostname> (default: dynamically detected system hostname)

Suggested command-line interface:

    lmtp-drain [options]

    -s, --spool-dir <path>
    -H, --host <host-or-address>
    -p, --port <port>
    -n, --lhlo-name <name>
    -h, --help

The program shall perform one scan and exit.

## 6. Input filename selection

The drainer shall examine only direct children of the configured spool directory whose names:

- do not begin with `.`;
- end with `.spool`; and
- refer to regular files.

It shall not recurse into subdirectories.

The sink's completed filename format is:

    YYYYMMDDTHHMMSSffffffZ.spool

Example:

    20260321T041523123456Z.spool

Eligible filenames shall be sorted lexicographically before delivery attempts. This is chronological for the sink's timestamp filenames.

Files ending in `.failed`, hidden temporary files, the lock file, directories, symlinks, and unrelated filenames shall be ignored.

The filename is otherwise an opaque record identifier.

## 7. Single-instance locking

Only one drainer process shall operate on a spool directory at a time.

Before scanning, the drainer shall acquire a non-blocking exclusive advisory lock associated with:

    /var/spool/lmtp-sink/.drain.lock

If another process holds the lock, the new invocation shall log that another drain is active and exit successfully without processing files.

The lock file is not a message and shall not be removed during normal draining.

Failure to open, create, or lock the lock file is a local filesystem error. The drainer shall log the error and exit immediately.

The sink does not need to participate in this lock. It publishes completed records by atomic rename, and the drainer examines only final `.spool` files.

## 8. Stored transaction format

Each input record has this layout:

    MAIL FROM:<sender@example.org> SIZE=488 BODY=8BITMIME
    RCPT TO:<jacob@<private-mail-domain>>
    RCPT TO:<support@<private-mail-domain>>
    RECEIVED AT:2026-03-21T04:15:23.123456Z

    [unescaped message bytes]

The preamble consists of:

1. Exactly one `MAIL FROM:` line.
2. One or more consecutive `RCPT TO:` lines.
3. Exactly one `RECEIVED AT:` line.
4. One empty line.
5. All remaining bytes as the message.

The message begins immediately after the empty separator line. Message headers that begin with `MAIL FROM:`, `RCPT TO:`, or `RECEIVED AT:` are message data and shall not be interpreted as envelope metadata.

The parser shall preserve the message bytes. It shall not parse, regenerate, or normalize RFC message headers or MIME content.

## 9. Envelope parsing

The sink stores the complete arguments from the original LMTP commands. Examples:

    MAIL FROM:<sender@example.org> SIZE=488 BODY=8BITMIME
    MAIL FROM:<>
    RCPT TO:<jacob@<private-mail-domain>>
    RCPT TO:<support@<private-mail-domain>> NOTIFY=FAILURE

The drainer shall extract the path enclosed by the first `<` and its matching `>` from each envelope line.

Examples:

    MAIL FROM:<sender@example.org> SIZE=488 BODY=8BITMIME
        -> <sender@example.org>

    MAIL FROM:<>
        -> <>

    RCPT TO:<jacob@<private-mail-domain>> NOTIFY=FAILURE
        -> <jacob@<private-mail-domain>>

The initial implementation shall use the extracted reverse-path and forward-path values for delivery and shall not replay stored ESMTP parameters.

A null reverse-path shall be transmitted as:

    MAIL FROM:<>

Reasons for omitting stored parameters include:

- `SIZE` describes the original submission and need not be replayed.
- `BODY=8BITMIME` is unnecessary for this private recovery delivery.
- DSN parameters such as `NOTIFY` and `ORCPT` shall not be sent unless deliberately implemented later.

## 10. Record validation

Before transmission, the drainer shall validate that a record contains:

- exactly one first-line `MAIL FROM:` field;
- at least one `RCPT TO:` field;
- exactly one `RECEIVED AT:` field after the recipients;
- an empty separator line;
- a syntactically extractable `<...>` path in the sender field; and
- a syntactically extractable `<...>` path in every recipient field.

The message portion may be empty.

The `RECEIVED AT:` value is informational and need not be parsed as a timestamp.

A record that can be read successfully but fails format validation is malformed, not a filesystem error.

On malformed input:

1. Log the filename and validation reason.
2. Atomically rename the file from `.spool` to `.failed`.
3. Continue with the next eligible record.

The rename shall not overwrite an existing `.failed` file. A rename collision or rename failure is a filesystem error and shall abort the run immediately.

## 11. Initial downstream availability check

If at least one `.spool` record exists, the drainer shall establish the actual LMTP session that will be used for delivery:

1. Connect to the configured host and port.
2. Read a valid `220` greeting.
3. Send:

       LHLO <mail-server-hostname>

4. Read a valid complete multiline `250` response.

A separate connect-and-disconnect probe shall not be used. The validated connection is the delivery connection, avoiding a race between a probe and actual delivery.

The downstream is considered unavailable when an operating-system or socket I/O error prevents completion of these steps, including:

- connection refused;
- no route to host;
- connect timeout;
- read timeout;
- write timeout;
- connection reset;
- broken pipe; or
- unexpected EOF.

On such a network error:

- Leave every record unchanged.
- Close the connection.
- Exit immediately with a nonzero status.
- Retry on the next scheduled invocation.

If the host responds but does not provide a valid LMTP `220` greeting and complete `250` response to `LHLO`, no record transaction has begun. The drainer shall leave all records unchanged, close the connection, log the protocol error, and exit immediately with a nonzero status.

The client shall not use TLS or authentication. The destination is the private Dovecot LMTP endpoint over WireGuard.

## 12. Connection model

The implementation may use one LMTP connection per record. This is the preferred simple model.

For the first record, the connection created by the initial availability check shall be used.

After a record succeeds or is marked `.failed`, the drainer may close that connection. Before transmitting the next valid record, it shall establish and validate a new LMTP connection.

If any later connection attempt encounters a network error, the drainer shall leave the next pending record as `.spool`, stop processing, and exit immediately.

A malformed local record may be marked `.failed` without opening a new LMTP connection.

## 13. LMTP envelope transaction

After a successful greeting and `LHLO`, send:

    MAIL FROM:<extracted reverse-path>

A `2xx` response is required.

For each stored recipient, in original order, send:

    RCPT TO:<extracted forward-path>

Every recipient must receive a `2xx` response before the drainer sends `DATA`.

If Dovecot returns a syntactically valid non-`2xx` response to `MAIL FROM` or any `RCPT TO` command:

- Do not send `DATA`.
- Close the connection.
- Atomically rename the current record from `.spool` to `.failed`.
- Continue with the next record using a new connection.

This applies equally to `4xx` and `5xx` responses. The drainer intentionally does not implement SMTP-style temporary-failure retry policy.

If a network error occurs while sending a command or reading its response:

- Leave the current record as `.spool`.
- Abort the entire run immediately.

If the server remains connected but sends a malformed or unexpected protocol response:

- Rename the current record to `.failed`.
- Close the connection.
- Continue with the next record using a new connection.

## 14. Message transmission and dot-stuffing

After all recipients have been accepted, send:

    DATA

A valid `354` response is required.

A valid non-`354` LMTP response is a downstream rejection:

- Rename the current record to `.failed`.
- Close the connection.
- Continue with the next record.

The stored message is already unescaped. While transmitting it, the drainer shall:

- send the message as LMTP DATA;
- add one extra dot to every message line whose first byte is `.`;
- preserve all other message bytes and line boundaries;
- ensure the LMTP terminator begins at the start of a new line; and
- terminate DATA with a line containing only `.` using CRLF framing.

Conceptually:

    stored line:  .leading-dot
    transmitted: ..leading-dot

The drainer shall stream the message from disk rather than requiring the complete record in memory.

If the stored message does not end in a line ending, the drainer shall send `\r\n` before the final dot terminator. This framing CRLF is transport syntax and is not written back to the spool file.

Any local file read error is a filesystem error:

- Abort the entire run immediately.
- Leave the current record as `.spool`.
- Do not process later records.

Any socket write error is a network error:

- Abort the entire run immediately.
- Leave the current record as `.spool`.

## 15. LMTP final responses

LMTP returns one final response after DATA for every recipient accepted by `RCPT TO`.

If the record contains two accepted recipients, the drainer shall read exactly two complete final responses.

A record succeeds only when:

- the connection remains usable through all expected responses;
- exactly one final response is received for every recipient; and
- every final response is `2xx`.

If every final response is `2xx`, delete the `.spool` file.

If one or more valid final responses are `4xx` or `5xx`:

- Consume the expected final response for every recipient unless a network error interrupts the response sequence.
- Rename the complete record to `.failed`.
- Close the connection.
- Continue with the next record.

If the connected server returns an invalid response or an unexpected response count without a socket I/O failure:

- Rename the record to `.failed`.
- Close the connection.
- Continue with the next record.

If a timeout, EOF, reset, or other network error occurs before all expected final responses are received:

- Leave the record as `.spool`.
- Abort the entire run immediately.
- Retry it on the next scheduled invocation.

Successful deletion shall not depend on receiving a response to `QUIT`.

## 16. Multiple recipients and partial success

The spool format permits multiple recipients in one record. LMTP may return different final results for different recipients.

The drainer maintains no per-recipient persistent state.

If all final recipient responses are `2xx`, the record is deleted.

If Dovecot returns a mix of success and rejection responses, for example:

    250 2.0.0 recipient one delivered
    550 5.1.1 recipient two rejected

the entire record shall be renamed `.failed` and shall not be retried automatically.

This prevents knowingly redelivering the record to recipients that already succeeded. Manual recovery can inspect the LMTP log and decide whether and how to recover rejected recipients.

No deduplication or sidecar recipient state is required.

## 17. Ambiguous network failure

An unavoidable ambiguity exists when:

1. The drainer transmits the message.
2. Dovecot stores it.
3. The network connection fails before the drainer receives every final success response.

This is classified as a network failure.

The drainer shall:

- Leave the record as `.spool`.
- Abort the run.
- Retry the record on the next invocation.

This may create a duplicate. The design deliberately favors possible duplicate delivery over deleting a message without confirmed acceptance.

## 18. Filesystem error policy

Filesystem errors are fatal to the current invocation.

Examples include failure to:

- open or read the spool directory;
- create or lock `.drain.lock`;
- list or inspect an entry;
- open or read a `.spool` file;
- verify that an entry is a regular file;
- rename `.spool` to `.failed`; or
- delete a successfully delivered `.spool` file.

On any filesystem error:

1. Log the filename or path and the operating-system error.
2. Stop processing immediately.
3. Close the LMTP connection if one is open.
4. Exit with a nonzero status.

The drainer shall not attempt local repair, alternate naming, sidecar creation, rollback, or continued processing after a filesystem error.

If Dovecot accepted a record but deletion fails, the file may remain `.spool` and may be delivered again on a later invocation. This exceptional duplicate risk is accepted in exchange for the minimal filesystem policy.

The drainer shall never truncate or rewrite a spool record.

## 19. Failed-record rename

When a malformed record or non-network LMTP error occurs, rename:

    YYYYMMDDTHHMMSSffffffZ.spool

to:

    YYYYMMDDTHHMMSSffffffZ.failed

The rename shall:

- occur in the same spool directory;
- be atomic;
- preserve file contents unchanged; and
- never overwrite an existing file.

After a successful rename, the drainer shall proceed normally to the next `.spool` record.

If the target `.failed` path already exists or the rename otherwise fails, this is a filesystem error and the drainer shall abort immediately.

The failure reason is written to standard error or the system journal. It is not added to the record.

## 20. Processing policy within one run

The drainer shall:

1. Acquire the lock.
2. Scan and sort eligible `.spool` files.
3. Exit successfully if none exist.
4. Establish the initial LMTP connection and validate the greeting and `LHLO`.
5. Process records in sorted order.
6. Delete successful records.
7. Rename malformed or LMTP-rejected records to `.failed` and continue.
8. Abort immediately on any network or filesystem error.

Records created by the sink after the initial scan may wait until the next scheduled invocation.

A malformed or rejected record shall not block later records after it has been successfully renamed `.failed`.

A network error stops the run because downstream availability is no longer established.

A filesystem error stops the run because safe local state changes are no longer established.

For each attempted record, log one concise outcome:

    delivered and deleted
    malformed; renamed to .failed
    LMTP rejected; renamed to .failed
    network error; retained as .spool; aborting
    filesystem error; aborting

## 21. Exit status

The process exit status shall be:

- `0` when no records exist;
- `0` when another drainer owns the lock and this invocation intentionally does no work;
- `0` when all pending records are either delivered and deleted or successfully classified as `.failed`;
- nonzero after any network error;
- nonzero after any filesystem error.

A record renamed to `.failed` is considered completely classified for automatic processing. Its presence does not by itself make the completed run fail.

## 22. Timeout policy

Network operations shall have finite timeouts.

Suggested defaults:

    connect timeout:   30 seconds
    LMTP read timeout: 5 minutes
    LMTP write timeout: 5 minutes

A timeout is a network error. The current record remains `.spool`, the run aborts, and a later scheduled invocation retries it.

These are operation timeouts, not message expiration or SMTP-style retry scheduling.

## 23. Scheduler

A typical cron entry is:

    */5 * * * * /usr/local/sbin/lmtp-drain --spool-dir /var/spool/lmtp-sink --host <dovecot-host> --port 24 --lhlo-name <mail-server-hostname>

The utility's single-instance lock makes overlapping invocations harmless.

The cron job should run as the same unprivileged `lmtp-sink` account that owns the spool directory and records.

An equivalent systemd oneshot service and timer may be used instead of cron.

## 24. Interaction with the sink

The sink writes hidden temporary files and atomically renames completed transactions to `.spool`.

The drainer:

- ignores hidden temporary files;
- sees only completed `.spool` files;
- ignores `.failed` files;
- does not lock the sink;
- does not block new sink deliveries;
- does not alter sink configuration; and
- does not submit a record that has not reached its final `.spool` name.

A new sink record appearing after the initial scan waits until the next run.

## 25. Security and privileges

The drainer shall:

- run as the unprivileged `lmtp-sink` user;
- read, rename, and delete files only in the configured spool directory;
- connect only to the explicitly configured LMTP host and port;
- perform no shell command execution;
- perform no DNS or MX routing unless a hostname is explicitly configured;
- treat spool contents as untrusted input for parsing purposes;
- avoid following symlinks as spool records; and
- avoid logging complete message contents.

The drainer needs outbound network access to the Dovecot WireGuard address.

## 26. Logging

Logging to standard error or the system journal is sufficient.

For successful delivery, log:

- filename;
- destination;
- recipient count; and
- that the file was deleted.

For a failed record, log:

- filename;
- LMTP stage or validation stage;
- LMTP response code and text or validation reason; and
- that the file was renamed to `.failed`.

For a network or filesystem error, log:

- filename or path when known;
- operation being performed;
- error summary; and
- that the run is aborting.

Envelope sender, recipient addresses, and message content need not be logged.

Alerting is outside scope.

## 27. Sample record

A sample input record is provided at:

    /root/lmtp-sink-0.1.0/sample.spool

Its envelope contains one sender and two recipients. Its message contains a line beginning with a literal dot so recovery testing can verify outbound dot-stuffing.

The sample is documentation and test input. It shall not be copied into the live spool except during a controlled test.

## 28. Acceptance criteria

The implementation is complete when all of the following are demonstrated:

1. An empty spool causes a successful no-op exit without contacting Dovecot.
2. Only completed regular `*.spool` files are selected.
3. `.failed`, hidden, symlink, directory, and unrelated entries are ignored.
4. Files are attempted in lexicographic order.
5. A second concurrent invocation exits without processing.
6. If Dovecot cannot be connected to, no record is changed and the run exits nonzero.
7. The initial reachability check uses the actual LMTP delivery connection.
8. A valid `220` greeting and multiline `250` response to `LHLO` are required.
9. The sample record parses into one sender, two recipients, a receipt value, and the original message.
10. A null envelope sender is transmitted correctly.
11. Multiple recipients are submitted in original order.
12. Stored envelope parameters are not replayed.
13. Message bytes are preserved except for required LMTP framing.
14. Lines beginning with a dot are correctly dot-stuffed.
15. One final LMTP response is read for every accepted recipient unless a network error interrupts the sequence.
16. A file is deleted only when every recipient receives a final `2xx` response.
17. Any valid `4xx` or `5xx` response causes the record to be renamed `.failed`.
18. An invalid LMTP response from a connected, responsive server causes the record to be renamed `.failed`.
19. A malformed spool record is renamed `.failed`.
20. A `.failed` record is not retried automatically.
21. A malformed or rejected record does not block later records.
22. Partial recipient success causes the complete record to be renamed `.failed`.
23. A network error leaves the current record `.spool`, aborts the run, and is retried next time.
24. An ambiguous post-DATA network failure leaves the record `.spool`.
25. Any filesystem error aborts the run immediately.
26. A successful `.failed` rename preserves the record contents.
27. Existing `.failed` files are never overwritten.
28. Successful delivery deletes the original `.spool` file.
29. No SMTP-style retry classification, queue expiration, DSN generation, deduplication, or alerting is implemented.
30. Repeated scheduler runs retry only records that remain `.spool`.

## 29. Governing rule

The drainer's policy is:

> Retry only after network failure. Mark malformed or LMTP-rejected records `.failed`. Delete only after every recipient is unambiguously accepted. Abort immediately on filesystem errors.

This policy reflects the system invariant that messages routed to the private `<private-mail-domain>` Dovecot destination are expected to be deliverable once that downstream host is reachable.
