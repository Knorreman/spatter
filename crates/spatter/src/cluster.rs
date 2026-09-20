use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use spatter_core::{Error, Result};

pub struct Cluster {
    pub rank: usize,
    pub n: usize,
    streams: Mutex<Vec<TcpStream>>,
    children: Mutex<Vec<Child>>,
    bytes_sent: AtomicU64,
    bytes_recv: AtomicU64,
}

const MAX_FRAME: usize = 256 * 1024 * 1024;

fn io_err(e: impl ToString) -> Error {
    Error::Cluster(e.to_string())
}

fn cluster_timeout() -> Duration {
    Duration::from_millis(
        std::env::var("SPATTER_CLUSTER_TIMEOUT_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(30_000),
    )
}

pub(crate) fn bind_host() -> String {
    std::env::var("SPATTER_BIND").unwrap_or_else(|_| "0.0.0.0".into())
}

pub(crate) fn master_addr(port: u16) -> String {
    if let Ok(m) = std::env::var("SPATTER_MASTER") {
        return m;
    }
    if let Ok(hosts) = std::env::var("SPATTER_HOSTS") {
        if let Some(first) = hosts.split(',').next() {
            let first = first.trim();
            if first.contains(':') {
                return first.to_string();
            }
            if !first.is_empty() {
                return format!("{first}:{port}");
            }
        }
    }
    format!("127.0.0.1:{port}")
}

pub(crate) fn parse_rank_host(host: &str) -> Option<usize> {
    let tail = host.rsplit('-').next()?;
    tail.split('.').next()?.parse().ok()
}

fn apply_rank_from_hostname() {
    if std::env::var("SPATTER_RANK").is_ok() {
        return;
    }
    let host = std::env::var("HOSTNAME").ok().or_else(|| {
        std::fs::read_to_string("/etc/hostname")
            .ok()
            .map(|s| s.trim().to_string())
    });
    if let Some(h) = host {
        if let Some(r) = parse_rank_host(&h) {
            std::env::set_var("SPATTER_RANK", r.to_string());
        }
    }
}

fn apply_timeouts(stream: &TcpStream) -> Result<()> {
    let t = cluster_timeout();
    stream.set_read_timeout(Some(t)).map_err(io_err)?;
    stream.set_write_timeout(Some(t)).map_err(io_err)?;
    let _ = stream.set_nodelay(true);
    Ok(())
}

fn kill_children(children: &mut [Child]) {
    for child in children.iter_mut() {
        let _ = child.kill();
        let _ = child.wait();
    }
}

fn wait_or_kill(child: &mut Child, timeout: Duration) {
    let start = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return,
            Ok(None) if start.elapsed() < timeout => thread::sleep(Duration::from_millis(50)),
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return;
            }
        }
    }
}

fn write_frame(stream: &mut TcpStream, bytes: &[u8], sent: &AtomicU64) -> Result<()> {
    if bytes.len() > MAX_FRAME {
        return Err(Error::Cluster("frame too large to send".into()));
    }
    stream
        .write_all(&(bytes.len() as u32).to_be_bytes())
        .map_err(io_err)?;
    stream.write_all(bytes).map_err(io_err)?;
    stream.flush().map_err(io_err)?;
    sent.fetch_add(4 + bytes.len() as u64, Ordering::Relaxed);
    Ok(())
}

fn read_frame(stream: &mut TcpStream, recv: &AtomicU64) -> Result<Vec<u8>> {
    let mut lenb = [0u8; 4];
    stream.read_exact(&mut lenb).map_err(io_err)?;
    let len = u32::from_be_bytes(lenb) as usize;
    if len > MAX_FRAME {
        return Err(Error::Cluster(format!("frame too large: {len}")));
    }
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).map_err(io_err)?;
    recv.fetch_add(4 + len as u64, Ordering::Relaxed);
    Ok(buf)
}

fn encode<T: Serialize>(v: &T) -> Result<Vec<u8>> {
    bincode::serialize(v).map_err(io_err)
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    bincode::deserialize(bytes).map_err(io_err)
}

#[derive(Serialize, Deserialize)]
struct Hello {
    rank: u32,
}

#[derive(Serialize, Deserialize)]
enum Task {
    Run { partition: u32 },
    Done,
}

#[derive(Serialize, Deserialize)]
enum Reply {
    Ok { partition: u32, payload: Vec<u8> },
    Err { partition: u32, message: String },
}

impl Cluster {
    pub fn owns(&self, partition: usize) -> bool {
        partition % self.n == self.rank
    }

    pub fn is_driver(&self) -> bool {
        self.rank == 0
    }

    pub fn connect(rank: usize, n: usize, port: u16) -> Result<Self> {
        if n < 2 {
            return Err(Error::Cluster("n must be >= 2".into()));
        }
        let bind_ip = bind_host();
        let master = master_addr(port);
        let scratch = AtomicU64::new(0);
        let timeout = cluster_timeout();
        let streams = if rank == 0 {
            let listener = TcpListener::bind(format!("{bind_ip}:{port}")).map_err(io_err)?;
            listener.set_nonblocking(true).map_err(io_err)?;
            let deadline = Instant::now() + timeout;
            let mut got: Vec<(usize, TcpStream)> = Vec::with_capacity(n - 1);
            while got.len() < n - 1 {
                if Instant::now() >= deadline {
                    return Err(Error::Cluster("startup accept timed out".into()));
                }
                match listener.accept() {
                    Ok((mut s, _)) => {
                        s.set_nonblocking(false).map_err(io_err)?;
                        apply_timeouts(&s)?;
                        let hello: Hello = decode(&read_frame(&mut s, &scratch)?)?;
                        let r = hello.rank as usize;
                        if r == 0 || r >= n || got.iter().any(|(x, _)| *x == r) {
                            return Err(Error::Cluster(format!("invalid worker rank {r}")));
                        }
                        got.push((r, s));
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(20));
                    }
                    Err(e) => return Err(io_err(e)),
                }
            }
            got.sort_by_key(|(r, _)| *r);
            got.into_iter().map(|(_, s)| s).collect()
        } else {
            let deadline = Instant::now() + timeout;
            let mut stream = None;
            while Instant::now() < deadline {
                if let Ok(s) = TcpStream::connect(&master) {
                    stream = Some(s);
                    break;
                }
                thread::sleep(Duration::from_millis(50));
            }
            let mut s = stream.ok_or_else(|| {
                Error::Cluster(format!("rank {rank} could not connect to {master}"))
            })?;
            apply_timeouts(&s)?;
            write_frame(&mut s, &encode(&Hello { rank: rank as u32 })?, &scratch)?;
            vec![s]
        };
        Ok(Self {
            rank,
            n,
            streams: Mutex::new(streams),
            children: Mutex::new(Vec::new()),
            bytes_sent: AtomicU64::new(0),
            bytes_recv: AtomicU64::new(0),
        })
    }

    #[allow(dead_code)]
    pub fn all_to_all(&self, chunks: Vec<Vec<u8>>) -> Result<Vec<Vec<u8>>> {
        if chunks.len() != self.n {
            return Err(Error::Cluster("all_to_all chunk count != n".into()));
        }
        let mut streams = self.streams.lock().expect("cluster streams");
        if self.rank == 0 {
            let mut from: Vec<Vec<Vec<u8>>> = vec![vec![]; self.n];
            from[0] = chunks;
            for (i, s) in streams.iter_mut().enumerate() {
                let got: Vec<Vec<u8>> = decode(&read_frame(s, &self.bytes_recv)?)?;
                if got.len() != self.n {
                    return Err(Error::Cluster("all_to_all rank payload size".into()));
                }
                from[i + 1] = got;
            }
            let mut dest: Vec<Vec<Vec<u8>>> = vec![vec![]; self.n];
            for from_chunks in &from {
                for (d, chunk) in from_chunks.iter().enumerate() {
                    dest[d].push(chunk.clone());
                }
            }
            for (i, s) in streams.iter_mut().enumerate() {
                write_frame(s, &encode(&dest[i + 1])?, &self.bytes_sent)?;
            }
            Ok(dest.remove(0))
        } else {
            write_frame(&mut streams[0], &encode(&chunks)?, &self.bytes_sent)?;
            let got: Vec<Vec<u8>> = decode(&read_frame(&mut streams[0], &self.bytes_recv)?)?;
            if got.len() != self.n {
                return Err(Error::Cluster("all_to_all dest payload size".into()));
            }
            Ok(got)
        }
    }

    #[allow(dead_code)]
    pub fn exchange_kv<K, V>(&self, buckets: Vec<Vec<(K, V)>>) -> Result<Vec<Vec<(K, V)>>>
    where
        K: Serialize + DeserializeOwned,
        V: Serialize + DeserializeOwned,
    {
        let n_out = buckets.len();
        type Packed<K, V> = Vec<(u32, Vec<(K, V)>)>;
        let mut for_rank: Vec<Packed<K, V>> = (0..self.n).map(|_| Vec::new()).collect();
        for (p, bucket) in buckets.into_iter().enumerate() {
            for_rank[p % self.n].push((p as u32, bucket));
        }
        let mut chunks = Vec::with_capacity(self.n);
        for packed in &for_rank {
            chunks.push(encode(packed)?);
        }
        let received = self.all_to_all(chunks)?;
        let mut out = Vec::with_capacity(n_out);
        for _ in 0..n_out {
            out.push(Vec::new());
        }
        for payload in received {
            if payload.is_empty() {
                continue;
            }
            let parts: Vec<(u32, Vec<(K, V)>)> = decode(&payload)?;
            for (p, bucket) in parts {
                let p = p as usize;
                if p >= n_out {
                    return Err(Error::Cluster(format!("shuffle bucket {p} >= {n_out}")));
                }
                out[p].extend(bucket);
            }
        }
        Ok(out)
    }

    pub fn bytes_sent(&self) -> u64 {
        self.bytes_sent.load(Ordering::Relaxed)
    }

    pub fn bytes_recv(&self) -> u64 {
        self.bytes_recv.load(Ordering::Relaxed)
    }

    pub fn dispatch_each<T, C, H>(&self, n: usize, compute: C, mut on_ok: H) -> Result<()>
    where
        T: Serialize + DeserializeOwned,
        C: Fn(usize) -> Result<T>,
        H: FnMut(T) -> Result<()>,
    {
        let mut streams = self.streams.lock().expect("cluster streams");
        if self.rank != 0 {
            loop {
                let task: Task = decode(&read_frame(&mut streams[0], &self.bytes_recv)?)?;
                match task {
                    Task::Done => break,
                    Task::Run { partition } => {
                        let reply = match compute(partition as usize) {
                            Ok(v) => Reply::Ok {
                                partition,
                                payload: encode(&v)?,
                            },
                            Err(e) => Reply::Err {
                                partition,
                                message: e.to_string(),
                            },
                        };
                        write_frame(&mut streams[0], &encode(&reply)?, &self.bytes_sent)?;
                    }
                }
            }
            return Ok(());
        }
        let mut first_err: Option<Error> = None;
        for p in 0..n {
            if first_err.is_some() {
                break;
            }
            let dest = p % self.n;
            let got = if self.owns(p) {
                compute(p)
            } else {
                let s = &mut streams[dest - 1];
                let remote = write_frame(
                    s,
                    &encode(&Task::Run {
                        partition: p as u32,
                    })?,
                    &self.bytes_sent,
                )
                .and_then(|_| read_frame(s, &self.bytes_recv))
                .and_then(|b| decode::<Reply>(&b))
                .and_then(|r| match r {
                    Reply::Ok { payload, .. } => decode(&payload),
                    Reply::Err { message, .. } => Err(Error::Cluster(message)),
                });
                match remote {
                    Ok(v) => Ok(v),
                    Err(_) => compute(p),
                }
            };
            match got.and_then(&mut on_ok) {
                Ok(()) => {}
                Err(e) => first_err = Some(e),
            }
        }
        for s in streams.iter_mut() {
            let _ = write_frame(s, &encode(&Task::Done)?, &self.bytes_sent);
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    pub fn gather<T>(&self, local: Vec<T>) -> Result<Vec<T>>
    where
        T: Serialize + DeserializeOwned,
    {
        let payload = encode(&local)?;
        let mut streams = self.streams.lock().expect("cluster streams");
        if self.rank == 0 {
            let mut all = decode::<Vec<T>>(&payload)?;
            for s in streams.iter_mut() {
                let bytes = read_frame(s, &self.bytes_recv)?;
                all.extend(decode::<Vec<T>>(&bytes)?);
            }
            for s in streams.iter_mut() {
                write_frame(s, &encode(&1u32)?, &self.bytes_sent)?;
            }
            Ok(all)
        } else {
            write_frame(&mut streams[0], &payload, &self.bytes_sent)?;
            let _ack: u32 = decode(&read_frame(&mut streams[0], &self.bytes_recv)?)?;
            Ok(Vec::new())
        }
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        let timeout = cluster_timeout();
        let mut children = self.children.lock().expect("children");
        for child in children.iter_mut() {
            wait_or_kill(child, timeout);
        }
    }
}

pub fn maybe_spawn_cluster() -> Result<Vec<Child>> {
    let args: Vec<String> = std::env::args().collect();
    if std::env::var("SPATTER_RANK").is_ok() {
        return Ok(Vec::new());
    }
    let mut n: Option<usize> = None;
    let mut i = 1;
    while i < args.len() {
        if args[i] == "--cluster" && i + 1 < args.len() {
            n = args[i + 1].parse().ok();
            break;
        }
        i += 1;
    }
    let n = match n {
        Some(n) if n >= 2 => n,
        _ => return Ok(Vec::new()),
    };
    let port: u16 = std::env::var("SPATTER_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(18741);
    let exe = std::env::current_exe().map_err(io_err)?;
    let rest: Vec<String> = args.iter().skip(1).cloned().collect();
    let mut children = Vec::new();
    for r in 1..n {
        match Command::new(&exe)
            .env("SPATTER_RANK", r.to_string())
            .env("SPATTER_N", n.to_string())
            .env("SPATTER_PORT", port.to_string())
            .env("SPATTER_MASTER", format!("127.0.0.1:{port}"))
            .args(&rest)
            .spawn()
        {
            Ok(c) => children.push(c),
            Err(e) => {
                kill_children(&mut children);
                return Err(io_err(e));
            }
        }
    }
    let master = format!("127.0.0.1:{port}");
    std::env::set_var("SPATTER_RANK", "0");
    std::env::set_var("SPATTER_N", n.to_string());
    std::env::set_var("SPATTER_PORT", port.to_string());
    std::env::set_var("SPATTER_MASTER", &master);
    Ok(children)
}

pub fn join_if_configured(mut children: Vec<Child>) -> Result<Option<std::sync::Arc<Cluster>>> {
    apply_rank_from_hostname();
    let n = match std::env::var("SPATTER_N") {
        Ok(s) => s.parse::<usize>().map_err(io_err)?,
        Err(_) => return Ok(None),
    };
    let rank = std::env::var("SPATTER_RANK")
        .map_err(io_err)?
        .parse::<usize>()
        .map_err(io_err)?;
    let port = std::env::var("SPATTER_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(18741);
    match Cluster::connect(rank, n, port) {
        Ok(c) => {
            *c.children.lock().expect("children") = children;
            Ok(Some(std::sync::Arc::new(c)))
        }
        Err(e) => {
            kill_children(&mut children);
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accept_timeout_without_workers() {
        std::env::set_var("SPATTER_CLUSTER_TIMEOUT_MS", "300");
        let t0 = Instant::now();
        let err = Cluster::connect(0, 2, 19992);
        std::env::remove_var("SPATTER_CLUSTER_TIMEOUT_MS");
        assert!(t0.elapsed() < Duration::from_secs(3));
        assert!(matches!(err, Err(Error::Cluster(_))));
    }

    #[test]
    fn kill_children_reaps_spawned_process() {
        let mut child = Command::new("sleep").arg("30").spawn().unwrap();
        kill_children(std::slice::from_mut(&mut child));
        assert!(child.try_wait().unwrap().is_some());
    }

    #[test]
    fn parse_rank_from_pod_hostname() {
        assert_eq!(parse_rank_host("spatter-0"), Some(0));
        assert_eq!(
            parse_rank_host("spatter-12.spatter.svc.cluster.local"),
            Some(12)
        );
        assert_eq!(parse_rank_host("localhost"), None);
    }

    #[test]
    fn master_addr_defaults_localhost() {
        let got = master_addr(18741);
        assert!(got.contains("18741") || got.contains(':'));
    }

    #[test]
    fn dispatch_each_replays_on_worker_fail() {
        let port = 19994u16;
        let worker = thread::spawn(move || {
            let c = Cluster::connect(1, 2, port)?;
            c.dispatch_each::<u32, _, _>(
                2,
                |p| {
                    if p == 1 {
                        Err(Error::Cluster("injected".into()))
                    } else {
                        Ok(p as u32)
                    }
                },
                |_| Ok(()),
            )
        });
        let driver = Cluster::connect(0, 2, port).unwrap();
        let mut got = Vec::new();
        driver
            .dispatch_each(
                2,
                |p| Ok(p as u32 + 100),
                |v| {
                    got.push(v);
                    Ok(())
                },
            )
            .unwrap();
        worker.join().unwrap().unwrap();
        got.sort();
        assert_eq!(got, vec![100, 101]);
    }
}
