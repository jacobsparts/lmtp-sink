use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::process;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Config {
    pub spool_dir: PathBuf,
    pub host: String,
    pub port: u16,
    pub lhlo_name: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            spool_dir: PathBuf::from("/var/spool/lmtp-sink"),
            host: "10.7.1.3".to_string(),
            port: 24,
            lhlo_name: "mail.jacobstoner.com".to_string(),
        }
    }
}

fn parse_args() -> Config {
    let mut config = Config::default();
    let args: Vec<String> = env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-s" | "--spool-dir" => {
                if i + 1 < args.len() {
                    config.spool_dir = PathBuf::from(&args[i + 1]);
                    i += 1;
                } else {
                    eprintln!("Error: --spool-dir requires an argument");
                    process::exit(1);
                }
            }
            "-H" | "--host" => {
                if i + 1 < args.len() {
                    config.host = args[i + 1].clone();
                    i += 1;
                } else {
                    eprintln!("Error: --host requires an argument");
                    process::exit(1);
                }
            }
            "-p" | "--port" => {
                if i + 1 < args.len() {
                    config.port = args[i + 1].parse().unwrap_or_else(|_| {
                        eprintln!("Error: Invalid port number");
                        process::exit(1);
                    });
                    i += 1;
                } else {
                    eprintln!("Error: --port requires an argument");
                    process::exit(1);
                }
            }
            "-n" | "--lhlo-name" => {
                if i + 1 < args.len() {
                    config.lhlo_name = args[i + 1].clone();
                    i += 1;
                } else {
                    eprintln!("Error: --lhlo-name requires an argument");
                    process::exit(1);
                }
            }
            "-h" | "--help" => {
                println!("Usage: lmtp-drain [options]");
                println!("Options:");
                println!("  -s, --spool-dir <path>       Spool directory (default: /var/spool/lmtp-sink)");
                println!("  -H, --host <host-or-address> LMTP host (default: 10.7.1.3)");
                println!("  -p, --port <port>            LMTP port (default: 24)");
                println!("  -n, --lhlo-name <name>       Client LHLO hostname (default: mail.jacobstoner.com)");
                println!("  -h, --help                   Show this help message");
                process::exit(0);
            }
            other => {
                eprintln!("Unknown argument: {}", other);
                process::exit(1);
            }
        }
        i += 1;
    }
    config
}

struct LockFile {
    _file: File,
}

fn acquire_spool_lock(spool_dir: &Path) -> Result<Option<LockFile>, String> {
    let lock_path = spool_dir.join(".drain.lock");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .map_err(|e| {
            format!(
                "Filesystem error: failed to open lock file {:?}: {}",
                lock_path, e
            )
        })?;

    let res = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if res != 0 {
        let err = io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::EWOULDBLOCK) || err.raw_os_error() == Some(libc::EAGAIN)
        {
            return Ok(None);
        }
        return Err(format!(
            "Filesystem error: failed to lock {:?}: {}",
            lock_path, err
        ));
    }

    Ok(Some(LockFile { _file: file }))
}

fn find_spool_records(spool_dir: &Path) -> Result<Vec<PathBuf>, String> {
    let entries = fs::read_dir(spool_dir)
        .map_err(|e| format!("Filesystem error reading directory {:?}: {}", spool_dir, e))?;

    let mut records = Vec::new();
    for entry_res in entries {
        let entry = entry_res.map_err(|e| {
            format!(
                "Filesystem error inspecting entry in {:?}: {}",
                spool_dir, e
            )
        })?;
        let path = entry.path();
        let filename = match path.file_name().and_then(|s| s.to_str()) {
            Some(name) => name,
            None => continue,
        };

        if filename.starts_with('.') || !filename.ends_with(".spool") {
            continue;
        }

        let symlink_meta = fs::symlink_metadata(&path)
            .map_err(|e| format!("Filesystem error inspecting metadata for {:?}: {}", path, e))?;
        if symlink_meta.file_type().is_symlink() || !symlink_meta.is_file() {
            continue;
        }

        records.push(path);
    }

    records.sort();
    Ok(records)
}

#[derive(Debug)]
struct ParsedRecord {
    sender_path: String,     // e.g. "<sender@example.org>" or "<>"
    rcpt_paths: Vec<String>, // e.g. ["<user@example.com>"]
    header_byte_len: u64,    // offset in bytes where message content begins
}

fn extract_bracketed_path(line: &str) -> Option<String> {
    let start = line.find('<')?;
    let end = line[start..].find('>')? + start;
    Some(line[start..=end].to_string())
}

fn parse_record(path: &Path) -> Result<Result<ParsedRecord, String>, io::Error> {
    let mut file = File::open(path)?;
    let mut reader = BufReader::new(&mut file);

    let mut sender_path: Option<String> = None;
    let mut rcpt_paths: Vec<String> = Vec::new();
    let mut received_at: Option<String> = None;

    let mut bytes_read_total: u64 = 0;
    let mut lines_parsed = 0;

    loop {
        let mut line_buf = Vec::new();
        let n = reader.read_until(b'\n', &mut line_buf)?;
        if n == 0 {
            // EOF before empty separator line
            return Ok(Err("Unexpected EOF in envelope preamble".to_string()));
        }
        bytes_read_total += n as u64;
        lines_parsed += 1;

        let line_str = String::from_utf8_lossy(&line_buf);
        let trimmed = line_str.trim_end_matches(['\r', '\n']);

        if trimmed.is_empty() {
            // Empty line separator reached
            break;
        }

        if lines_parsed == 1 {
            if !trimmed.starts_with("MAIL FROM:") {
                return Ok(Err("First line is not MAIL FROM:".to_string()));
            }
            match extract_bracketed_path(trimmed) {
                Some(p) => sender_path = Some(p),
                None => return Ok(Err("No valid <...> path in MAIL FROM:".to_string())),
            }
        } else if sender_path.is_some() && received_at.is_none() && trimmed.starts_with("RCPT TO:")
        {
            match extract_bracketed_path(trimmed) {
                Some(p) => rcpt_paths.push(p),
                None => return Ok(Err("No valid <...> path in RCPT TO:".to_string())),
            }
        } else if sender_path.is_some()
            && !rcpt_paths.is_empty()
            && trimmed.starts_with("RECEIVED AT:")
        {
            received_at = Some(trimmed["RECEIVED AT:".len()..].to_string());
        } else {
            return Ok(Err(format!("Unexpected preamble line: {}", trimmed)));
        }
    }

    if sender_path.is_none() || rcpt_paths.is_empty() || received_at.is_none() {
        return Ok(Err("Incomplete envelope preamble metadata".to_string()));
    }

    Ok(Ok(ParsedRecord {
        sender_path: sender_path.unwrap(),
        rcpt_paths,
        header_byte_len: bytes_read_total,
    }))
}

fn mark_record_failed(spool_path: &Path) -> Result<(), String> {
    let filename = spool_path.file_name().unwrap().to_str().unwrap();
    let failed_filename = format!("{}.failed", filename.strip_suffix(".spool").unwrap());
    let failed_path = spool_path.with_file_name(&failed_filename);

    if failed_path.exists() {
        return Err(format!(
            "Filesystem error: destination {:?} already exists",
            failed_path
        ));
    }

    fs::rename(spool_path, &failed_path).map_err(|e| {
        format!(
            "Filesystem error: failed to rename {:?} to {:?}: {}",
            spool_path, failed_path, e
        )
    })
}

#[derive(Debug)]
enum ResponseError {
    Network(String),
    Protocol(String),
}

impl std::fmt::Display for ResponseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResponseError::Network(s) => write!(f, "{}", s),
            ResponseError::Protocol(s) => write!(f, "{}", s),
        }
    }
}

struct LmtpClient {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
}

impl LmtpClient {
    fn connect(config: &Config) -> Result<Self, String> {
        let addr_str = format!("{}:{}", config.host, config.port);
        let addrs: Vec<_> = addr_str
            .to_socket_addrs()
            .map_err(|e| format!("Failed to resolve {}: {}", addr_str, e))?
            .collect();

        if addrs.is_empty() {
            return Err(format!("No IP address resolved for {}", addr_str));
        }

        let stream = TcpStream::connect_timeout(&addrs[0], Duration::from_secs(30))
            .map_err(|e| format!("Network error connecting to {}: {}", addr_str, e))?;

        stream
            .set_read_timeout(Some(Duration::from_secs(300)))
            .map_err(|e| format!("Network error setting read timeout: {}", e))?;
        stream
            .set_write_timeout(Some(Duration::from_secs(300)))
            .map_err(|e| format!("Network error setting write timeout: {}", e))?;

        let reader_stream = stream
            .try_clone()
            .map_err(|e| format!("Network error cloning stream: {}", e))?;
        let reader = BufReader::new(reader_stream);

        let mut client = LmtpClient { stream, reader };
        client.handshake(&config.lhlo_name)?;
        Ok(client)
    }

    fn read_response(&mut self) -> Result<(u16, String), ResponseError> {
        let mut full_resp = String::new();
        loop {
            let mut line = String::new();
            let n = self.reader.read_line(&mut line).map_err(|e| {
                ResponseError::Network(format!("Network error reading LMTP response: {}", e))
            })?;
            if n == 0 {
                return Err(ResponseError::Network(
                    "Network error: unexpected EOF reading response".to_string(),
                ));
            }

            full_resp.push_str(&line);
            if line.len() >= 4 {
                let code_str = &line[..3];
                let sep = line.as_bytes()[3];
                if let Ok(code) = code_str.parse::<u16>() {
                    if sep == b' ' {
                        return Ok((code, full_resp));
                    } else if sep == b'-' {
                        continue;
                    }
                }
            }
            return Err(ResponseError::Protocol(format!(
                "Malformed LMTP response: {}",
                line.trim()
            )));
        }
    }

    fn handshake(&mut self, lhlo_name: &str) -> Result<(), String> {
        let (code, greeting) = self
            .read_response()
            .map_err(|e| format!("Downstream greeting error: {}", e))?;
        if code != 220 {
            return Err(format!("Downstream rejected greeting: {}", greeting.trim()));
        }

        let lhlo_cmd = format!("LHLO {}\r\n", lhlo_name);
        self.stream
            .write_all(lhlo_cmd.as_bytes())
            .map_err(|e| format!("Network error writing LHLO: {}", e))?;
        self.stream
            .flush()
            .map_err(|e| format!("Network error flushing LHLO: {}", e))?;

        let (code, resp) = self
            .read_response()
            .map_err(|e| format!("Downstream LHLO response error: {}", e))?;
        if code != 250 {
            return Err(format!("Downstream rejected LHLO: {}", resp.trim()));
        }
        Ok(())
    }
}

enum DeliverResult {
    Success,
    LmtpRejected(String),
    NetworkError(String),
    FilesystemError(String),
}

fn deliver_record(
    client: &mut LmtpClient,
    record_path: &Path,
    parsed: &ParsedRecord,
) -> DeliverResult {
    // 1. MAIL FROM
    let mail_cmd = format!("MAIL FROM:{}\r\n", parsed.sender_path);
    if let Err(e) = client.stream.write_all(mail_cmd.as_bytes()) {
        return DeliverResult::NetworkError(format!("Network error writing MAIL FROM: {}", e));
    }
    if let Err(e) = client.stream.flush() {
        return DeliverResult::NetworkError(format!("Network error flushing MAIL FROM: {}", e));
    }

    match client.read_response() {
        Ok((code, resp)) => {
            if !(200..300).contains(&code) {
                return DeliverResult::LmtpRejected(format!("MAIL FROM rejected: {}", resp.trim()));
            }
        }
        Err(ResponseError::Protocol(msg)) => return DeliverResult::LmtpRejected(msg),
        Err(ResponseError::Network(msg)) => return DeliverResult::NetworkError(msg),
    }

    // 2. RCPT TO
    for rcpt in &parsed.rcpt_paths {
        let rcpt_cmd = format!("RCPT TO:{}\r\n", rcpt);
        if let Err(e) = client.stream.write_all(rcpt_cmd.as_bytes()) {
            return DeliverResult::NetworkError(format!("Network error writing RCPT TO: {}", e));
        }
        if let Err(e) = client.stream.flush() {
            return DeliverResult::NetworkError(format!("Network error flushing RCPT TO: {}", e));
        }

        match client.read_response() {
            Ok((code, resp)) => {
                if !(200..300).contains(&code) {
                    return DeliverResult::LmtpRejected(format!(
                        "RCPT TO {} rejected: {}",
                        rcpt,
                        resp.trim()
                    ));
                }
            }
            Err(ResponseError::Protocol(msg)) => return DeliverResult::LmtpRejected(msg),
            Err(ResponseError::Network(msg)) => return DeliverResult::NetworkError(msg),
        }
    }

    // 3. DATA
    if let Err(e) = client.stream.write_all(b"DATA\r\n") {
        return DeliverResult::NetworkError(format!("Network error writing DATA: {}", e));
    }
    if let Err(e) = client.stream.flush() {
        return DeliverResult::NetworkError(format!("Network error flushing DATA: {}", e));
    }

    match client.read_response() {
        Ok((code, resp)) => {
            if code != 354 {
                return DeliverResult::LmtpRejected(format!("DATA rejected: {}", resp.trim()));
            }
        }
        Err(ResponseError::Protocol(msg)) => return DeliverResult::LmtpRejected(msg),
        Err(ResponseError::Network(msg)) => return DeliverResult::NetworkError(msg),
    }

    // 4. Stream Message Body
    let mut file = match File::open(record_path) {
        Ok(f) => f,
        Err(e) => {
            return DeliverResult::FilesystemError(format!(
                "Filesystem error opening record {:?}: {}",
                record_path, e
            ))
        }
    };

    if let Err(e) = io::Seek::seek(&mut file, io::SeekFrom::Start(parsed.header_byte_len)) {
        return DeliverResult::FilesystemError(format!(
            "Filesystem error seeking body in {:?}: {}",
            record_path, e
        ));
    }

    let mut reader = BufReader::new(file);
    let mut data_buf = Vec::new();
    let mut last_byte_written: Option<u8> = None;

    loop {
        data_buf.clear();
        match reader.read_until(b'\n', &mut data_buf) {
            Ok(0) => break, // EOF
            Ok(_) => {
                let payload = if data_buf.starts_with(b".") {
                    // Dot-stuffing
                    let mut stuffed = Vec::with_capacity(data_buf.len() + 1);
                    stuffed.push(b'.');
                    stuffed.extend_from_slice(&data_buf);
                    stuffed
                } else {
                    data_buf.clone()
                };

                if !payload.is_empty() {
                    last_byte_written = Some(payload[payload.len() - 1]);
                }

                if let Err(e) = client.stream.write_all(&payload) {
                    return DeliverResult::NetworkError(format!(
                        "Network error writing message body: {}",
                        e
                    ));
                }
            }
            Err(e) => {
                return DeliverResult::FilesystemError(format!(
                    "Filesystem error reading message body from {:?}: {}",
                    record_path, e
                ))
            }
        }
    }

    // Ensure terminator is on a fresh line
    if let Some(b) = last_byte_written {
        if b != b'\n' {
            if let Err(e) = client.stream.write_all(b"\r\n") {
                return DeliverResult::NetworkError(format!(
                    "Network error framing terminator: {}",
                    e
                ));
            }
        }
    }

    // Terminating dot
    if let Err(e) = client.stream.write_all(b".\r\n") {
        return DeliverResult::NetworkError(format!(
            "Network error sending DATA terminator: {}",
            e
        ));
    }
    if let Err(e) = client.stream.flush() {
        return DeliverResult::NetworkError(format!(
            "Network error flushing DATA terminator: {}",
            e
        ));
    }

    // 5. Read final responses (one per recipient)
    let mut all_ok = true;
    let mut reject_msg = String::new();

    for _ in 0..parsed.rcpt_paths.len() {
        match client.read_response() {
            Ok((code, resp)) => {
                if !(200..300).contains(&code) {
                    all_ok = false;
                    if !reject_msg.is_empty() {
                        reject_msg.push_str("; ");
                    }
                    reject_msg.push_str(resp.trim());
                }
            }
            Err(ResponseError::Protocol(msg)) => {
                all_ok = false;
                if !reject_msg.is_empty() {
                    reject_msg.push_str("; ");
                }
                reject_msg.push_str(&msg);
            }
            Err(ResponseError::Network(msg)) => return DeliverResult::NetworkError(msg),
        }
    }

    if all_ok {
        DeliverResult::Success
    } else {
        DeliverResult::LmtpRejected(format!(
            "Downstream rejected final delivery: {}",
            reject_msg
        ))
    }
}

fn main() {
    let config = parse_args();

    if !config.spool_dir.exists() {
        eprintln!(
            "Filesystem error: Spool directory {:?} does not exist",
            config.spool_dir
        );
        process::exit(1);
    }

    // Section 7: Acquire lock file
    let _lock = match acquire_spool_lock(&config.spool_dir) {
        Ok(Some(lock)) => lock,
        Ok(None) => {
            eprintln!("Another lmtp-drain process is active; exiting.");
            process::exit(0);
        }
        Err(e) => {
            eprintln!("{}", e);
            process::exit(1);
        }
    };

    // Section 6: Find and sort .spool records
    let records = match find_spool_records(&config.spool_dir) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("{}", e);
            process::exit(1);
        }
    };

    if records.is_empty() {
        process::exit(0);
    }

    let destination_str = format!("{}:{}", config.host, config.port);

    // Section 11: Establish and validate initial downstream LMTP connection BEFORE processing any records
    let mut initial_client = match LmtpClient::connect(&config) {
        Ok(c) => Some(c),
        Err(e) => {
            eprintln!(
                "Downstream LMTP host {} unavailable ({}); aborting without modifying records",
                destination_str, e
            );
            process::exit(1);
        }
    };

    for record_path in records {
        let filename = record_path.file_name().unwrap().to_str().unwrap();

        // Parse record
        let parsed = match parse_record(&record_path) {
            Ok(Ok(p)) => p,
            Ok(Err(reason)) => {
                eprintln!(
                    "Record {:?}: malformed ({}); renaming to .failed",
                    filename, reason
                );
                if let Err(e) = mark_record_failed(&record_path) {
                    eprintln!("{}", e);
                    process::exit(1);
                }
                continue;
            }
            Err(e) => {
                eprintln!(
                    "Filesystem error reading record {:?}: {}; aborting",
                    filename, e
                );
                process::exit(1);
            }
        };

        // Use pre-validated initial connection for first valid record, or connect anew
        let mut client = match initial_client.take() {
            Some(c) => c,
            None => match LmtpClient::connect(&config) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!(
                        "Record {:?}: network error ({}); retained as .spool; aborting",
                        filename, e
                    );
                    process::exit(1);
                }
            },
        };

        match deliver_record(&mut client, &record_path, &parsed) {
            DeliverResult::Success => {
                if let Err(e) = fs::remove_file(&record_path) {
                    eprintln!(
                        "Filesystem error removing delivered file {:?}: {}; aborting",
                        record_path, e
                    );
                    process::exit(1);
                }
                eprintln!(
                    "Record {:?}: delivered to {} ({} recipients); deleted",
                    filename,
                    destination_str,
                    parsed.rcpt_paths.len()
                );
            }
            DeliverResult::LmtpRejected(reason) => {
                eprintln!(
                    "Record {:?}: LMTP rejected ({}); renaming to .failed",
                    filename, reason
                );
                if let Err(e) = mark_record_failed(&record_path) {
                    eprintln!("{}", e);
                    process::exit(1);
                }
            }
            DeliverResult::NetworkError(reason) => {
                eprintln!(
                    "Record {:?}: network error ({}); retained as .spool; aborting",
                    filename, reason
                );
                process::exit(1);
            }
            DeliverResult::FilesystemError(reason) => {
                eprintln!("Record {:?}: {}; aborting", filename, reason);
                process::exit(1);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::thread;

    fn setup_mock_dovecot(
        behavior: Arc<std::sync::Mutex<Vec<String>>>,
    ) -> (Config, String, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();

        let spool_dir = std::env::temp_dir().join(format!("drain_test_{}", port));
        let _ = fs::remove_dir_all(&spool_dir);
        fs::create_dir_all(&spool_dir).unwrap();

        let config = Config {
            spool_dir: spool_dir.clone(),
            host: "127.0.0.1".to_string(),
            port,
            lhlo_name: "mail.example.com".to_string(),
        };

        let handle = thread::spawn(move || {
            for mut stream in listener.incoming().flatten() {
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let _ = stream.write_all(b"220 2.0.0 mock dovecot ready\r\n");
                let _ = stream.flush();

                let mut rcpt_count = 0;

                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap_or(0) == 0 {
                        break;
                    }
                    let trimmed = line.trim();
                    let upper = trimmed.to_ascii_uppercase();

                    let mut beh = behavior.lock().unwrap();
                    let response_override = if !beh.is_empty() {
                        Some(beh.remove(0))
                    } else {
                        None
                    };

                    if let Some(resp) = response_override {
                        if resp == "DROP" {
                            break;
                        }
                        let _ = stream.write_all(resp.as_bytes());
                        let _ = stream.flush();
                        continue;
                    }

                    if upper.starts_with("LHLO") {
                        let _ = stream.write_all(b"250-mock\r\n250 8BITMIME\r\n");
                        let _ = stream.flush();
                    } else if upper.starts_with("MAIL FROM:") {
                        let _ = stream.write_all(b"250 2.1.0 OK\r\n");
                        let _ = stream.flush();
                    } else if upper.starts_with("RCPT TO:") {
                        rcpt_count += 1;
                        let _ = stream.write_all(b"250 2.1.5 OK\r\n");
                        let _ = stream.flush();
                    } else if upper == "DATA" {
                        let _ = stream.write_all(b"354 Start mail input\r\n");
                        let _ = stream.flush();

                        // Read body until dot
                        loop {
                            let mut body_line = String::new();
                            if reader.read_line(&mut body_line).unwrap_or(0) == 0 {
                                break;
                            }
                            if body_line == ".\r\n" || body_line == ".\n" {
                                break;
                            }
                        }

                        // Write final 250s
                        for _ in 0..rcpt_count {
                            let _ = stream.write_all(b"250 2.0.0 Delivered\r\n");
                        }
                        let _ = stream.flush();
                    } else if upper == "QUIT" {
                        let _ = stream.write_all(b"221 2.0.0 Bye\r\n");
                        let _ = stream.flush();
                        break;
                    }
                }
            }
        });

        thread::sleep(Duration::from_millis(50));
        let addr = format!("127.0.0.1:{}", port);
        (config, addr, handle)
    }

    #[test]
    fn test_sample_spool_parsing_and_delivery() {
        let behavior = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (config, _addr, _handle) = setup_mock_dovecot(behavior);

        let sample_path = Path::new("sample.spool");
        let parsed_res = parse_record(sample_path).unwrap();
        let parsed = parsed_res.expect("sample.spool should be valid");

        assert_eq!(parsed.sender_path, "<sender@example.org>");
        assert_eq!(
            parsed.rcpt_paths,
            vec![
                "<jacob@example.org>".to_string(),
                "<support@example.org>".to_string()
            ]
        );

        // Copy sample.spool to test spool directory
        let spool_file = config.spool_dir.join("20260321T041523123456Z.spool");
        fs::copy(sample_path, &spool_file).unwrap();

        let _lock = acquire_spool_lock(&config.spool_dir).unwrap().unwrap();
        let records = find_spool_records(&config.spool_dir).unwrap();
        assert_eq!(records.len(), 1);

        let mut client = LmtpClient::connect(&config).unwrap();
        let res = deliver_record(&mut client, &spool_file, &parsed);
        assert!(matches!(res, DeliverResult::Success));

        fs::remove_file(&spool_file).unwrap();
        let _ = fs::remove_dir_all(&config.spool_dir);
    }

    #[test]
    fn test_lock_collision() {
        let spool_dir = std::env::temp_dir().join("drain_lock_test");
        let _ = fs::remove_dir_all(&spool_dir);
        fs::create_dir_all(&spool_dir).unwrap();

        let lock1 = acquire_spool_lock(&spool_dir).unwrap();
        assert!(lock1.is_some());

        let lock2 = acquire_spool_lock(&spool_dir).unwrap();
        assert!(lock2.is_none());

        drop(lock1);
        let _ = fs::remove_dir_all(&spool_dir);
    }

    #[test]
    fn test_lmtp_rejection_renames_to_failed() {
        let behavior = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (config, _addr, _handle) = setup_mock_dovecot(behavior.clone());

        let sample_path = Path::new("sample.spool");
        let parsed = parse_record(sample_path).unwrap().unwrap();

        let spool_file = config.spool_dir.join("20260321T041523123456Z.spool");
        fs::copy(sample_path, &spool_file).unwrap();

        let mut client = LmtpClient::connect(&config).unwrap();

        // Push 550 rejection for the upcoming MAIL FROM command
        behavior
            .lock()
            .unwrap()
            .push("550 5.1.1 User unknown\r\n".to_string());

        let res = deliver_record(&mut client, &spool_file, &parsed);
        assert!(matches!(res, DeliverResult::LmtpRejected(_)));

        mark_record_failed(&spool_file).unwrap();
        assert!(!spool_file.exists());
        assert!(config
            .spool_dir
            .join("20260321T041523123456Z.failed")
            .exists());

        let _ = fs::remove_dir_all(&config.spool_dir);
    }

    #[test]
    fn test_malformed_lmtp_response_becomes_rejected() {
        let behavior = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (config, _addr, _handle) = setup_mock_dovecot(behavior.clone());

        let sample_path = Path::new("sample.spool");
        let parsed = parse_record(sample_path).unwrap().unwrap();

        let spool_file = config.spool_dir.join("20260321T041523123456Z.spool");
        fs::copy(sample_path, &spool_file).unwrap();

        let mut client = LmtpClient::connect(&config).unwrap();

        // Push non-LMTP response for MAIL FROM
        behavior
            .lock()
            .unwrap()
            .push("this is not LMTP\r\n".to_string());

        let res = deliver_record(&mut client, &spool_file, &parsed);
        assert!(matches!(res, DeliverResult::LmtpRejected(_)));

        let _ = fs::remove_dir_all(&config.spool_dir);
    }

    #[test]
    fn test_malformed_spool_record() {
        let spool_dir = std::env::temp_dir().join("drain_malformed_test");
        let _ = fs::remove_dir_all(&spool_dir);
        fs::create_dir_all(&spool_dir).unwrap();

        let spool_file = spool_dir.join("20260321T000000000000Z.spool");
        fs::write(&spool_file, "INVALID METADATA HEADER\n\n").unwrap();

        let parsed_res = parse_record(&spool_file).unwrap();
        assert!(parsed_res.is_err());

        mark_record_failed(&spool_file).unwrap();
        assert!(!spool_file.exists());
        assert!(spool_dir.join("20260321T000000000000Z.failed").exists());

        let _ = fs::remove_dir_all(&spool_dir);
    }
}
