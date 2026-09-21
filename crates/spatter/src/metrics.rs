//! Process-wide counters and an opt-in Prometheus text endpoint.
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

pub(crate) static TASKS: AtomicU64 = AtomicU64::new(0);
pub(crate) static FAILURES: AtomicU64 = AtomicU64::new(0);
pub(crate) static RETRIES: AtomicU64 = AtomicU64::new(0);
pub(crate) static TASK_US: AtomicU64 = AtomicU64::new(0);
pub(crate) static SENT: AtomicU64 = AtomicU64::new(0);
pub(crate) static RECEIVED: AtomicU64 = AtomicU64::new(0);
pub(crate) static SPILLED: AtomicU64 = AtomicU64::new(0);
pub(crate) static SPILL_READ: AtomicU64 = AtomicU64::new(0);
pub(crate) static DISCONNECTS: AtomicU64 = AtomicU64::new(0);
pub(crate) static SHUFFLE_RECORDS: AtomicU64 = AtomicU64::new(0);

pub(crate) fn job_id() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

/// Prometheus counters for this process. Concurrent reads are not an atomic snapshot.
pub fn render() -> String {
    let mut out = String::new();
    for (name, counter) in [
        ("task_attempts_total", &TASKS),
        ("task_failures_total", &FAILURES),
        ("task_retries_total", &RETRIES),
        ("task_duration_microseconds_total", &TASK_US),
        ("network_sent_bytes_total", &SENT),
        ("network_received_bytes_total", &RECEIVED),
        ("spill_written_bytes_total", &SPILLED),
        ("spill_read_bytes_total", &SPILL_READ),
        ("dispatch_disconnects_total", &DISCONNECTS),
        ("shuffle_map_records_total", &SHUFFLE_RECORDS),
    ] {
        out.push_str(&format!(
            "# TYPE spatter_{name} counter\nspatter_{name} {}\n",
            counter.load(Ordering::Relaxed)
        ));
    }
    out
}

pub(crate) fn task<T>(
    job: u64,
    partition: usize,
    attempt: u32,
    compute: impl FnOnce() -> crate::Result<T>,
) -> crate::Result<T> {
    start_task("dispatch", job, partition, attempt);
    let start = Instant::now();
    let result = crate::exec::catch_compute_result(partition, compute);
    observe_task(
        "dispatch",
        job,
        partition,
        attempt,
        start.elapsed(),
        result.is_ok(),
    );
    result
}

pub(crate) fn start_task(scope: &str, job: u64, partition: usize, attempt: u32) {
    TASKS.fetch_add(1, Ordering::Relaxed);
    if attempt > 0 {
        RETRIES.fetch_add(1, Ordering::Relaxed);
    }
    if std::env::var_os("SPATTER_TASK_LOG").is_some() {
        let rank = std::env::var("SPATTER_RANK")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        log_line(format!("{{\"event\":\"task_started\",\"scope\":\"{scope}\",\"pid\":{},\"rank\":{rank},\"job\":{job},\"partition\":{partition},\"attempt\":{attempt}}}", std::process::id()));
    }
}

pub(crate) fn observe_task(
    scope: &str,
    job: u64,
    partition: usize,
    attempt: u32,
    duration: Duration,
    success: bool,
) {
    let us = duration.as_micros().min(u64::MAX as u128) as u64;
    TASK_US.fetch_add(us, Ordering::Relaxed);
    if !success {
        FAILURES.fetch_add(1, Ordering::Relaxed);
    }
    if std::env::var_os("SPATTER_TASK_LOG").is_some() {
        let rank = std::env::var("SPATTER_RANK")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        log_line(format!("{{\"event\":\"task_finished\",\"scope\":\"{scope}\",\"pid\":{},\"rank\":{rank},\"job\":{job},\"partition\":{partition},\"attempt\":{attempt},\"duration_us\":{us},\"success\":{success}}}", std::process::id()));
    }
}

pub(crate) fn log_line(mut line: String) {
    line.push('\n');
    // One short write prevents separate ranks interleaving format fragments
    // when stderr is a shared pipe (records stay below PIPE_BUF).
    let _ = std::io::stderr().lock().write_all(line.as_bytes());
}

/// A small HTTP server serving `GET /metrics`. Dropping it closes the listener
/// and joins its thread. Counters are process-wide; binding is explicit.
pub struct MetricsServer {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl MetricsServer {
    pub fn bind(addr: SocketAddr) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr)?;
        let addr = listener.local_addr()?;
        listener.set_nonblocking(true)?;
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = Arc::clone(&stop);
        let thread = thread::Builder::new().name("spatter-metrics".into()).spawn(move || {
            while !stopping.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let _ = stream.set_read_timeout(Some(Duration::from_millis(200)));
                        let _ = stream.set_write_timeout(Some(Duration::from_millis(200)));
                        let mut request = Vec::new();
                        let mut byte = [0];
                        let deadline = Instant::now() + Duration::from_millis(200);
                        while request.len() < 1024 && Instant::now() < deadline && stream.read_exact(&mut byte).is_ok() {
                            request.push(byte[0]);
                            if request.ends_with(b"\r\n\r\n") { break; }
                        }
                        let (status, body) = if request.starts_with(b"GET /metrics HTTP/1.1\r\n") || request.starts_with(b"GET /metrics HTTP/1.0\r\n") {
                            ("200 OK", render())
                        } else { ("404 Not Found", "not found\n".into()) };
                        let response = format!("HTTP/1.1 {status}\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                        let _ = stream.write_all(response.as_bytes());
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => thread::sleep(Duration::from_millis(10)),
                    Err(_) => break,
                }
            }
        })?;
        Ok(Self {
            addr,
            stop,
            thread: Some(thread),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }
}

impl Drop for MetricsServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
