use std::env;
use std::ffi::CString;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::process;

use chrono::Utc;

#[derive(Debug, Clone)]
pub struct Config {
    pub listen_addr: String,
    pub listen_port: u16,
    pub spool_dir: PathBuf,
    pub min_free_bytes: u64,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            listen_addr: "127.0.0.1".to_string(),
            listen_port: 2526,
            spool_dir: PathBuf::from("/var/spool/lmtp-sink"),
            min_free_bytes: 104_857_600, // 100 MiB
        }
    }
}

fn parse_args() -> Config {
    let mut config = Config::default();
    let args: Vec<String> = env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-l" | "--listen-addr" => {
                if i + 1 < args.len() {
                    config.listen_addr = args[i + 1].clone();
                    i += 1;
                } else {
                    eprintln!("Error: --listen-addr requires an argument");
                    process::exit(1);
                }
            }
            "-p" | "--listen-port" => {
                if i + 1 < args.len() {
                    config.listen_port = args[i + 1].parse().unwrap_or_else(|_| {
                        eprintln!("Error: Invalid port number");
                        process::exit(1);
                    });
                    i += 1;
                } else {
                    eprintln!("Error: --listen-port requires an argument");
                    process::exit(1);
                }
            }
            "-s" | "--spool-dir" => {
                if i + 1 < args.len() {
                    config.spool_dir = PathBuf::from(&args[i + 1]);
                    i += 1;
                } else {
                    eprintln!("Error: --spool-dir requires an argument");
                    process::exit(1);
                }
            }
            "-m" | "--min-free-bytes" => {
                if i + 1 < args.len() {
                    config.min_free_bytes = args[i + 1].parse().unwrap_or_else(|_| {
                        eprintln!("Error: Invalid minimum free bytes");
                        process::exit(1);
                    });
                    i += 1;
                } else {
                    eprintln!("Error: --min-free-bytes requires an argument");
                    process::exit(1);
                }
            }
            "-h" | "--help" => {
                println!("Usage: lmtp-sink [options]");
                println!("Options:");
                println!("  -l, --listen-addr <addr>     Listen address (default: 127.0.0.1)");
                println!("  -p, --listen-port <port>     Listen port (default: 2526)");
                println!("  -s, --spool-dir <path>       Spool directory (default: /var/spool/lmtp-sink)");
                println!(
                    "  -m, --min-free-bytes <bytes> Min free bytes required (default: 104857600)"
                );
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

fn get_available_bytes(path: &Path) -> io::Result<u64> {
    let c_path = CString::new(path.as_os_str().as_bytes())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    let res = unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) };
    if res != 0 {
        return Err(io::Error::last_os_error());
    }
    let block_size = if stat.f_frsize > 0 {
        stat.f_frsize
    } else {
        stat.f_bsize
    };
    #[allow(clippy::unnecessary_cast)]
    Ok((stat.f_bavail as u64) * (block_size as u64))
}

fn sync_dir(dir_path: &Path) -> io::Result<()> {
    let dir = File::open(dir_path)?;
    dir.sync_all()?;
    Ok(())
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum SessionState {
    Initial,
    Idle,
    MailReceived,
    RcptReceived,
}

struct TransactionState {
    mail_from_arg: Option<String>,
    rcpt_to_args: Vec<String>,
}

impl TransactionState {
    fn new() -> Self {
        TransactionState {
            mail_from_arg: None,
            rcpt_to_args: Vec::new(),
        }
    }

    fn reset(&mut self) {
        self.mail_from_arg = None;
        self.rcpt_to_args.clear();
    }
}

fn handle_connection(mut stream: TcpStream, config: &Config) -> io::Result<()> {
    let peer_addr = stream.peer_addr().ok();
    let reader_stream = stream.try_clone()?;
    let mut reader = BufReader::new(reader_stream);

    // Send LMTP Greeting
    stream.write_all(b"220 2.0.0 lmtp-sink ready\r\n")?;
    stream.flush()?;

    let mut session_state = SessionState::Initial;
    let mut tx = TransactionState::new();

    let mut line_buf = Vec::new();

    loop {
        line_buf.clear();
        let bytes_read = reader.read_until(b'\n', &mut line_buf)?;
        if bytes_read == 0 {
            // Client disconnected
            break;
        }

        let raw_line = String::from_utf8_lossy(&line_buf);
        let trimmed_line = raw_line.trim_end_matches(['\r', '\n']);

        if trimmed_line.is_empty() {
            stream.write_all(b"501 5.5.2 Syntax error\r\n")?;
            stream.flush()?;
            continue;
        }

        // Split command verb and argument
        let (verb, arg_opt) = match trimmed_line.find(' ') {
            Some(idx) => (&trimmed_line[..idx], Some(trimmed_line[idx + 1..].trim())),
            None => (trimmed_line, None),
        };

        let verb_upper = verb.to_ascii_uppercase();

        match verb_upper.as_str() {
            "LHLO" => {
                session_state = SessionState::Idle;
                tx.reset();
                stream.write_all(b"250-lmtp-sink\r\n250 8BITMIME\r\n")?;
                stream.flush()?;
            }
            "MAIL" => {
                if session_state == SessionState::Initial {
                    stream.write_all(b"503 5.5.1 Bad sequence of commands\r\n")?;
                    stream.flush()?;
                    continue;
                }

                // Check for "FROM:" prefix
                let full_cmd_upper = trimmed_line.to_ascii_uppercase();
                if !full_cmd_upper.starts_with("MAIL FROM:") {
                    stream.write_all(b"501 5.5.2 Syntax error\r\n")?;
                    stream.flush()?;
                    continue;
                }

                let mail_arg = trimmed_line["MAIL FROM:".len()..].trim();
                if !mail_arg.starts_with('<') || !mail_arg.contains('>') {
                    stream.write_all(b"501 5.5.2 Syntax error\r\n")?;
                    stream.flush()?;
                    continue;
                }

                tx.reset();
                tx.mail_from_arg = Some(mail_arg.to_string());
                session_state = SessionState::MailReceived;

                stream.write_all(b"250 2.1.0 Sender accepted\r\n")?;
                stream.flush()?;
            }
            "RCPT" => {
                if session_state != SessionState::MailReceived
                    && session_state != SessionState::RcptReceived
                {
                    stream.write_all(b"503 5.5.1 Bad sequence of commands\r\n")?;
                    stream.flush()?;
                    continue;
                }

                let full_cmd_upper = trimmed_line.to_ascii_uppercase();
                if !full_cmd_upper.starts_with("RCPT TO:") {
                    stream.write_all(b"501 5.5.2 Syntax error\r\n")?;
                    stream.flush()?;
                    continue;
                }

                let rcpt_arg = trimmed_line["RCPT TO:".len()..].trim();
                if !rcpt_arg.starts_with('<') || !rcpt_arg.contains('>') {
                    stream.write_all(b"501 5.5.2 Syntax error\r\n")?;
                    stream.flush()?;
                    continue;
                }

                tx.rcpt_to_args.push(rcpt_arg.to_string());
                session_state = SessionState::RcptReceived;

                stream.write_all(b"250 2.1.5 Recipient accepted\r\n")?;
                stream.flush()?;
            }
            "DATA" => {
                if session_state != SessionState::RcptReceived || tx.rcpt_to_args.is_empty() {
                    stream.write_all(b"503 5.5.1 Bad sequence of commands\r\n")?;
                    stream.flush()?;
                    continue;
                }

                if arg_opt.is_some() {
                    stream.write_all(b"501 5.5.2 Syntax error\r\n")?;
                    stream.flush()?;
                    continue;
                }

                // Check free space on spool directory filesystem
                let avail_bytes = match get_available_bytes(&config.spool_dir) {
                    Ok(b) => b,
                    Err(e) => {
                        eprintln!("Error checking free space on {:?}: {}", config.spool_dir, e);
                        0
                    }
                };

                if avail_bytes < config.min_free_bytes {
                    eprintln!(
                        "Rejecting DATA: available space {} bytes < required {} bytes",
                        avail_bytes, config.min_free_bytes
                    );
                    stream.write_all(b"452 4.3.1 Insufficient system storage\r\n")?;
                    stream.flush()?;
                    continue;
                }

                // Accept DATA mode
                stream.write_all(b"354 Send message data; end with <CRLF>.<CRLF>\r\n")?;
                stream.flush()?;

                // Process DATA content
                let data_res = receive_and_store_data(&mut reader, &tx, config);
                match data_res {
                    Ok(spool_filename) => {
                        for _ in &tx.rcpt_to_args {
                            let resp = format!("250 2.0.0 Stored as {}\r\n", spool_filename);
                            stream.write_all(resp.as_bytes())?;
                        }
                        stream.flush()?;
                    }
                    Err(DataError::PostData(e)) => {
                        eprintln!("Failed to store message after DATA terminator: {}", e);
                        for _ in &tx.rcpt_to_args {
                            stream.write_all(b"451 4.3.0 Unable to store message\r\n")?;
                        }
                        stream.flush()?;
                    }
                    Err(DataError::MidData(e)) => {
                        eprintln!("Error during DATA transfer ({}), closing connection", e);
                        tx.reset();
                        break;
                    }
                }

                // Reset transaction state after DATA attempt
                tx.reset();
                session_state = SessionState::Idle;
            }
            "RSET" => {
                tx.reset();
                if session_state != SessionState::Initial {
                    session_state = SessionState::Idle;
                }
                stream.write_all(b"250 2.0.0 Reset\r\n")?;
                stream.flush()?;
            }
            "NOOP" => {
                stream.write_all(b"250 2.0.0 OK\r\n")?;
                stream.flush()?;
            }
            "QUIT" => {
                stream.write_all(b"221 2.0.0 Bye\r\n")?;
                stream.flush()?;
                break;
            }
            "VRFY" | "EXPN" | "ETRN" | "STARTTLS" | "AUTH" | "HELO" | "EHLO" | "BDAT" => {
                stream.write_all(b"502 5.5.1 Command not implemented\r\n")?;
                stream.flush()?;
            }
            _ => {
                stream.write_all(b"502 5.5.1 Command not implemented\r\n")?;
                stream.flush()?;
            }
        }
    }

    if let Some(_addr) = peer_addr {
        // Disconnected
    }
    Ok(())
}

#[derive(Debug)]
enum DataError {
    MidData(io::Error),
    PostData(io::Error),
}

fn receive_and_store_data<R: BufRead>(
    reader: &mut R,
    tx: &TransactionState,
    config: &Config,
) -> Result<String, DataError> {
    let now = Utc::now();
    let iso8601_time = now.format("%Y-%m-%dT%H:%M:%S.%6fZ").to_string();

    let mail_from = tx.mail_from_arg.as_deref().unwrap_or("<>");

    // Exclusive create of temp file + ensure final .spool path does not exist
    let mut retries = 0;
    let (temp_path, spool_filename, spool_path, mut temp_file) = loop {
        let ts_now = Utc::now();
        let timestamp_str = ts_now.format("%Y%m%dT%H%M%S%6fZ").to_string();
        let tmp_name = format!(".{}.tmp", timestamp_str);
        let final_name = format!("{}.spool", timestamp_str);

        let tmp_path = config.spool_dir.join(&tmp_name);
        let final_path = config.spool_dir.join(&final_name);

        if final_path.exists() {
            retries += 1;
            if retries > 100 {
                return Err(DataError::PostData(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "Failed to generate unique spool filename",
                )));
            }
            std::thread::sleep(std::time::Duration::from_micros(10));
            continue;
        }

        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp_path)
        {
            Ok(file) => break (tmp_path, final_name, final_path, file),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                retries += 1;
                if retries > 100 {
                    return Err(DataError::PostData(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "Failed to generate unique temp filename",
                    )));
                }
                std::thread::sleep(std::time::Duration::from_micros(10));
            }
            Err(e) => return Err(DataError::PostData(e)),
        }
    };

    // Phase 1: Write preamble & read message data from client socket until terminator line
    let phase1_res = (|| -> io::Result<()> {
        writeln!(temp_file, "MAIL FROM:{}", mail_from)?;
        for rcpt in &tx.rcpt_to_args {
            writeln!(temp_file, "RCPT TO:{}", rcpt)?;
        }
        writeln!(temp_file, "RECEIVED AT:{}", iso8601_time)?;
        writeln!(temp_file)?; // Empty line separating preamble and body

        let mut data_buf = Vec::new();
        loop {
            data_buf.clear();
            let n = reader.read_until(b'\n', &mut data_buf)?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Client disconnected during DATA transfer",
                ));
            }

            if data_buf == b".\r\n" || data_buf == b".\n" {
                break;
            }

            let payload = if data_buf.starts_with(b"..") {
                &data_buf[1..]
            } else {
                &data_buf[..]
            };

            temp_file.write_all(payload)?;
        }
        Ok(())
    })();

    if let Err(e) = phase1_res {
        drop(temp_file);
        let _ = fs::remove_file(&temp_path);
        return Err(DataError::MidData(e));
    }

    // Phase 2: Flush, fsync, close, atomic rename, and directory fsync
    let phase2_res = (|| -> io::Result<()> {
        temp_file.flush()?;
        temp_file.sync_all()?;
        drop(temp_file);

        fs::rename(&temp_path, &spool_path)?;
        sync_dir(&config.spool_dir)?;
        Ok(())
    })();

    if let Err(e) = phase2_res {
        let _ = fs::remove_file(&temp_path);
        Err(DataError::PostData(e))
    } else {
        Ok(spool_filename)
    }
}

fn main() {
    let config = parse_args();

    if let Err(e) = fs::create_dir_all(&config.spool_dir) {
        eprintln!(
            "Failed to create spool directory {:?}: {}",
            config.spool_dir, e
        );
        process::exit(1);
    }

    let listen_str = format!("{}:{}", config.listen_addr, config.listen_port);
    let listener = match TcpListener::bind(&listen_str) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("Failed to bind to {}: {}", listen_str, e);
            process::exit(1);
        }
    };

    eprintln!(
        "lmtp-sink running on {} (spool dir: {:?}, min free space: {} bytes)",
        listen_str, config.spool_dir, config.min_free_bytes
    );

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                if let Err(e) = handle_connection(stream, &config) {
                    eprintln!("Connection error: {}", e);
                }
            }
            Err(e) => {
                eprintln!("Accept error: {}", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;
    use std::thread;
    use std::time::Duration;

    fn setup_test_server(min_free: u64) -> (Config, String) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let spool_dir = std::env::temp_dir().join(format!("lmtp_test_{}", port));
        let _ = fs::remove_dir_all(&spool_dir);
        fs::create_dir_all(&spool_dir).unwrap();

        let config = Config {
            listen_addr: "127.0.0.1".to_string(),
            listen_port: port,
            spool_dir: spool_dir.clone(),
            min_free_bytes: min_free,
        };

        let server_config = config.clone();
        thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let _ = handle_connection(stream, &server_config);
            }
        });

        // Give listener thread a moment
        thread::sleep(Duration::from_millis(50));
        let addr = format!("127.0.0.1:{}", port);
        (config, addr)
    }

    #[test]
    fn test_full_transaction() {
        let (config, addr) = setup_test_server(100_000);
        let stream = TcpStream::connect(&addr).unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut writer = stream;

        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        assert!(line.starts_with("220 "));

        // LHLO
        writer
            .write_all(
                b"LHLO client.example.com
",
            )
            .unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.starts_with("250-"));
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.starts_with("250 "));

        // MAIL FROM
        writer
            .write_all(
                b"MAIL FROM:<sender@example.com> SIZE=100
",
            )
            .unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.starts_with("250 "));

        // RCPT TO 1
        writer
            .write_all(
                b"RCPT TO:<rcpt1@example.com>
",
            )
            .unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.starts_with("250 "));

        // RCPT TO 2
        writer
            .write_all(
                b"RCPT TO:<rcpt2@example.com> NOTIFY=FAILURE
",
            )
            .unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.starts_with("250 "));

        // DATA
        writer
            .write_all(
                b"DATA
",
            )
            .unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.starts_with("354 "));

        // Send message content with dot stuffing
        writer
            .write_all(
                b"From: sender@example.com
Subject: Test

..Leading dot
Normal line
.
",
            )
            .unwrap();

        // Expect 2 x 250 responses (one per recipient)
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.starts_with("250 2.0.0 Stored as "));
        let spool_name1 = line
            .trim()
            .strip_prefix("250 2.0.0 Stored as ")
            .unwrap()
            .to_string();

        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.starts_with("250 2.0.0 Stored as "));

        // QUIT
        writer
            .write_all(
                b"QUIT
",
            )
            .unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.starts_with("221 "));

        // Verify file stored in spool dir
        let spool_path = config.spool_dir.join(&spool_name1);
        assert!(spool_path.exists());

        let contents = fs::read_to_string(&spool_path).unwrap();
        assert!(contents.contains(
            "MAIL FROM:<sender@example.com> SIZE=100
"
        ));
        assert!(contents.contains(
            "RCPT TO:<rcpt1@example.com>
"
        ));
        assert!(contents.contains(
            "RCPT TO:<rcpt2@example.com> NOTIFY=FAILURE
"
        ));
        assert!(contents.contains("RECEIVED AT:"));
        assert!(contents.contains(
            ".Leading dot
Normal line
"
        ));

        let _ = fs::remove_dir_all(&config.spool_dir);
    }

    #[test]
    fn test_null_sender() {
        let (config, addr) = setup_test_server(100_000);
        let stream = TcpStream::connect(&addr).unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut writer = stream;

        let mut line = String::new();
        reader.read_line(&mut line).unwrap(); // 220

        writer
            .write_all(
                b"LHLO localhost
",
            )
            .unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();

        writer
            .write_all(
                b"MAIL FROM:<>
",
            )
            .unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.starts_with("250 "));

        writer
            .write_all(
                b"RCPT TO:<user@example.com>
",
            )
            .unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.starts_with("250 "));

        writer
            .write_all(
                b"DATA
",
            )
            .unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.starts_with("354 "));

        writer
            .write_all(
                b"Test message
.
",
            )
            .unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.starts_with("250 2.0.0 Stored as "));
        let spool_name = line
            .trim()
            .strip_prefix("250 2.0.0 Stored as ")
            .unwrap()
            .to_string();

        let contents = fs::read_to_string(config.spool_dir.join(spool_name)).unwrap();
        assert!(contents.contains(
            "MAIL FROM:<>
"
        ));

        let _ = fs::remove_dir_all(&config.spool_dir);
    }

    #[test]
    fn test_insufficient_storage() {
        // Require 100 TB of free space so check fails
        let (config, addr) = setup_test_server(100_000_000_000_000_000);
        let stream = TcpStream::connect(&addr).unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut writer = stream;

        let mut line = String::new();
        reader.read_line(&mut line).unwrap();

        writer
            .write_all(
                b"LHLO localhost
",
            )
            .unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();

        writer
            .write_all(
                b"MAIL FROM:<a@b.com>
",
            )
            .unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();

        writer
            .write_all(
                b"RCPT TO:<c@d.com>
",
            )
            .unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();

        writer
            .write_all(
                b"DATA
",
            )
            .unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.starts_with("452 "));

        let _ = fs::remove_dir_all(&config.spool_dir);
    }

    #[test]
    fn test_bad_sequence() {
        let (config, addr) = setup_test_server(100_000);
        let stream = TcpStream::connect(&addr).unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut writer = stream;

        let mut line = String::new();
        reader.read_line(&mut line).unwrap();

        // MAIL FROM before LHLO
        writer
            .write_all(
                b"MAIL FROM:<a@b.com>
",
            )
            .unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.starts_with("503 "));

        writer
            .write_all(
                b"LHLO localhost
",
            )
            .unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();

        // RCPT TO before MAIL FROM
        writer
            .write_all(
                b"RCPT TO:<c@d.com>
",
            )
            .unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.starts_with("503 "));

        // DATA before RCPT TO
        writer
            .write_all(
                b"DATA
",
            )
            .unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.starts_with("503 "));

        let _ = fs::remove_dir_all(&config.spool_dir);
    }

    #[test]
    fn test_disconnect_during_data() {
        let (config, addr) = setup_test_server(100_000);
        let stream = TcpStream::connect(&addr).unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut writer = stream;

        let mut line = String::new();
        reader.read_line(&mut line).unwrap();

        writer
            .write_all(
                b"LHLO localhost
",
            )
            .unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();

        writer
            .write_all(
                b"MAIL FROM:<a@b.com>
",
            )
            .unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();

        writer
            .write_all(
                b"RCPT TO:<c@d.com>
",
            )
            .unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();

        writer
            .write_all(
                b"DATA
",
            )
            .unwrap();
        line.clear();
        reader.read_line(&mut line).unwrap();
        assert!(line.starts_with("354 "));

        // Send partial data and drop connection without terminator
        writer.write_all(b"Incomplete message...\r\n").unwrap();
        drop(writer);
        drop(reader);

        for _ in 0..50 {
            let entries: Vec<_> = fs::read_dir(&config.spool_dir).unwrap().collect();
            if entries.is_empty() {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }

        let entries: Vec<_> = fs::read_dir(&config.spool_dir).unwrap().collect();
        assert_eq!(entries.len(), 0);

        let _ = fs::remove_dir_all(&config.spool_dir);
    }
}
