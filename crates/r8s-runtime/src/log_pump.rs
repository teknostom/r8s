//! CRI-style log pipeline.
//!
//! Containerd writes container stdout/stderr to two named pipes. Two reader
//! threads pump those pipes line by line into a single merged log file using
//! the kubelet CRI line format:
//!
//! ```text
//! <RFC3339Nano> <stdout|stderr> <P|F> <content>\n
//! ```
//!
//! `F` marks a full line (the container wrote a `\n`); `P` marks a partial
//! fragment that the next entry on the same stream will continue.

use std::ffi::CString;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct LogPaths {
    pub stdout_fifo: PathBuf,
    pub stderr_fifo: PathBuf,
    pub log_file: PathBuf,
}

impl LogPaths {
    pub fn for_container(log_dir: &Path, id: &str) -> Self {
        Self {
            stdout_fifo: log_dir.join(format!("{id}.stdout.fifo")),
            stderr_fifo: log_dir.join(format!("{id}.stderr.fifo")),
            log_file: log_dir.join(format!("{id}.log")),
        }
    }

    /// Best-effort cleanup of fifos and log file.
    pub fn remove(&self) {
        let _ = std::fs::remove_file(&self.stdout_fifo);
        let _ = std::fs::remove_file(&self.stderr_fifo);
        let _ = std::fs::remove_file(&self.log_file);
    }
}

/// Create the fifos and start two reader threads. The threads block on opening
/// the fifos for read until containerd opens them for write — so this must be
/// called *before* `tasks.create()`.
pub fn start(paths: &LogPaths) -> std::io::Result<()> {
    if let Some(parent) = paths.log_file.parent() {
        std::fs::create_dir_all(parent)?;
    }
    // Truncate any prior log so we don't blend old + new bytes on restart.
    // (--previous needs us to rotate instead of truncate; that's a separate task.)
    let _ = std::fs::remove_file(&paths.log_file);

    mkfifo(&paths.stdout_fifo, 0o600)?;
    mkfifo(&paths.stderr_fifo, 0o600)?;

    let log_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&paths.log_file)?;
    let writer: Arc<Mutex<std::fs::File>> = Arc::new(Mutex::new(log_file));

    spawn_pump(paths.stdout_fifo.clone(), "stdout", writer.clone());
    spawn_pump(paths.stderr_fifo.clone(), "stderr", writer);
    Ok(())
}

fn mkfifo(path: &Path, mode: u32) -> std::io::Result<()> {
    // Remove a stale fifo from a prior run.
    let _ = std::fs::remove_file(path);
    let c_path = CString::new(path.as_os_str().as_bytes())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    let ret = unsafe { libc::mkfifo(c_path.as_ptr(), mode as libc::mode_t) };
    if ret != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

fn spawn_pump(fifo: PathBuf, stream: &'static str, writer: Arc<Mutex<std::fs::File>>) {
    std::thread::Builder::new()
        .name(format!("log-pump-{stream}"))
        .spawn(move || pump_loop(fifo, stream, writer))
        .expect("failed to spawn log pump thread");
}

fn pump_loop(fifo: PathBuf, stream: &'static str, writer: Arc<Mutex<std::fs::File>>) {
    // Blocks until containerd opens the writer end. Returns Err if the fifo
    // was removed before that happened (cleanup raced us).
    let file = match std::fs::OpenOptions::new().read(true).open(&fifo) {
        Ok(f) => f,
        Err(e) => {
            tracing::warn!(?fifo, %stream, error = %e, "log pump: open failed");
            return;
        }
    };
    let mut reader = BufReader::new(file);
    let mut buf = Vec::with_capacity(4096);
    loop {
        buf.clear();
        match reader.read_until(b'\n', &mut buf) {
            Ok(0) => break, // writer closed: container exited
            Ok(_) => {
                let full = buf.last() == Some(&b'\n');
                let content: &[u8] = if full {
                    &buf[..buf.len() - 1]
                } else {
                    &buf[..]
                };
                if let Err(e) = write_entry(&writer, stream, full, content) {
                    tracing::warn!(%stream, error = %e, "log pump: write failed");
                    return;
                }
            }
            Err(e) => {
                tracing::warn!(%stream, error = %e, "log pump: read failed");
                return;
            }
        }
    }
}

fn write_entry(
    writer: &Mutex<std::fs::File>,
    stream: &str,
    full: bool,
    content: &[u8],
) -> std::io::Result<()> {
    let ts = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
    let tag = if full { 'F' } else { 'P' };
    // Build the full record once and emit with a single write_all under the
    // mutex so stdout/stderr lines never interleave mid-record.
    let mut out = Vec::with_capacity(content.len() + 64);
    out.extend_from_slice(ts.as_bytes());
    out.push(b' ');
    out.extend_from_slice(stream.as_bytes());
    out.push(b' ');
    out.push(tag as u8);
    out.push(b' ');
    out.extend_from_slice(content);
    out.push(b'\n');
    let mut file = writer.lock().expect("log writer mutex poisoned");
    file.write_all(&out)
}
