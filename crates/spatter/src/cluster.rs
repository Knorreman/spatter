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
    let _span = crate::profile::Span::new("network_write");
    if bytes.len() > MAX_FRAME {
        return Err(Error::Cluster("frame too large to send".into()));
    }
    stream
        .write_all(&(bytes.len() as u32).to_be_bytes())
        .map_err(io_err)?;
    stream.write_all(bytes).map_err(io_err)?;
    stream.flush().map_err(io_err)?;
    sent.fetch_add(4 + bytes.len() as u64, Ordering::Relaxed);
    crate::metrics::SENT.fetch_add(4 + bytes.len() as u64, Ordering::Relaxed);
    Ok(())
}

fn read_frame(stream: &mut TcpStream, recv: &AtomicU64) -> Result<Vec<u8>> {
    let _span = crate::profile::Span::new("network_read_wait");
    let mut lenb = [0u8; 4];
    stream.read_exact(&mut lenb).map_err(io_err)?;
    let len = u32::from_be_bytes(lenb) as usize;
    if len > MAX_FRAME {
        return Err(Error::Cluster(format!("frame too large: {len}")));
    }
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).map_err(io_err)?;
    recv.fetch_add(4 + len as u64, Ordering::Relaxed);
    crate::metrics::RECEIVED.fetch_add(4 + len as u64, Ordering::Relaxed);
    Ok(buf)
}

fn encode<T: Serialize>(v: &T) -> Result<Vec<u8>> {
    let _span = crate::profile::Span::new("encode");
    bincode::serialize(v).map_err(io_err)
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    let _span = crate::profile::Span::new("decode");
    bincode::deserialize(bytes).map_err(io_err)
}

#[derive(Serialize, Deserialize)]
struct Hello {
    rank: u32,
}

#[derive(Serialize, Deserialize)]
enum Task {
    Run { partition: u32, job: u64, task: u64 },
    Done,
}

#[derive(Serialize, Deserialize)]
enum Reply {
    Ok { partition: u32, payload: Vec<u8> },
    Err { partition: u32, message: String },
}

impl Reply {
    fn partition(&self) -> u32 {
        match self {
            Reply::Ok { partition, .. } | Reply::Err { partition, .. } => *partition,
        }
    }
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
        C: Fn(usize) -> Result<T> + Sync,
        H: FnMut(T) -> Result<()>,
    {
        // Two outstanding partitions per rank. Finish and drain each window
        // before submitting another, including when callbacks fail.
        let window = self.n.saturating_mul(2).max(1);
        let job = crate::metrics::job_id();
        if n == 0 {
            return self.dispatch_window(job, 0, 0, compute, on_ok);
        }
        for start in (0..n).step_by(window) {
            self.dispatch_window(
                job,
                start,
                (n - start).min(window),
                |p| compute(start + p),
                &mut on_ok,
            )?;
        }
        Ok(())
    }

    fn dispatch_window<T, C, H>(
        &self,
        job: u64,
        offset: usize,
        n: usize,
        compute: C,
        mut on_ok: H,
    ) -> Result<()>
    where
        T: Serialize + DeserializeOwned,
        C: Fn(usize) -> Result<T> + Sync,
        H: FnMut(T) -> Result<()>,
    {
        let mut streams = self.streams.lock().expect("cluster streams");
        if self.rank != 0 {
            let (task_tx, task_rx) = std::sync::mpsc::sync_channel::<Task>(2);
            let (reply_tx, reply_rx) = std::sync::mpsc::sync_channel::<Vec<u8>>(2);
            let stream = streams[0]
                .try_clone()
                .map_err(|e| Error::Cluster(format!("stream clone: {e}")))?;
            let task_rx = std::sync::Arc::new(std::sync::Mutex::new(task_rx));
            let task_tx_reader = task_tx.clone();
            drop(task_tx);
            thread::scope(|scope| {
                let compute = &compute;
                let mut reader_stream = stream;
                scope.spawn(move || {
                    while let Ok(bytes) = read_frame(&mut reader_stream, &self.bytes_recv) {
                        match decode::<Task>(&bytes) {
                            Ok(Task::Done) | Err(_) => break,
                            Ok(task) => {
                                if task_tx_reader.send(task).is_err() {
                                    break;
                                }
                            }
                        }
                    }
                });
                let n_computer = spatter_core::default_parallelism().clamp(1, 2);
                let mut writer_stream = streams[0]
                    .try_clone()
                    .map_err(|e| Error::Cluster(format!("stream clone: {e}")))
                    .expect("stream clone");
                let writer = scope.spawn(move || {
                    while let Ok(bytes) = reply_rx.recv() {
                        if write_frame(&mut writer_stream, &bytes, &self.bytes_sent).is_err() {
                            break;
                        }
                    }
                });
                for _ in 0..n_computer {
                    let task_rx = std::sync::Arc::clone(&task_rx);
                    let reply_tx = reply_tx.clone();
                    scope.spawn(move || loop {
                        let task = {
                            let guard = task_rx.lock().expect("task rx");
                            match guard.recv() {
                                Ok(t) => t,
                                Err(_) => break,
                            }
                        };
                        let reply = match task {
                            Task::Done => break,
                            Task::Run {
                                partition,
                                job,
                                task,
                            } => {
                                match crate::metrics::task(job, task as usize, 0, || {
                                    compute(partition as usize)
                                }) {
                                    Ok(v) => match encode(&v) {
                                        Ok(payload) => Reply::Ok { partition, payload },
                                        Err(e) => Reply::Err {
                                            partition,
                                            message: e.to_string(),
                                        },
                                    },
                                    Err(e) => Reply::Err {
                                        partition,
                                        message: e.to_string(),
                                    },
                                }
                            }
                        };
                        if reply_tx
                            .send(encode(&reply).expect("reply encode"))
                            .is_err()
                        {
                            break;
                        }
                    });
                }
                drop(reply_tx);
                let _ = writer.join();
            });
            return Ok(());
        }
        let mut first_err: Option<Error> = None;
        let mut pending: Vec<(usize, usize)> = Vec::new();
        let mut sent: Vec<usize> = vec![0; streams.len()];
        for p in 0..n {
            if first_err.is_some() {
                break;
            }
            if self.owns(p) {
                match crate::metrics::task(job, offset + p, 0, || compute(p)).and_then(&mut on_ok) {
                    Ok(()) => {}
                    Err(e) => first_err = Some(e),
                }
            } else {
                let w = p % self.n - 1;
                let s = &mut streams[w];
                if write_frame(
                    s,
                    &encode(&Task::Run {
                        partition: p as u32,
                        job,
                        task: (offset + p) as u64,
                    })?,
                    &self.bytes_sent,
                )
                .is_ok()
                {
                    pending.push((p, w));
                    sent[w] += 1;
                } else if first_err.is_none() {
                    first_err = Some(Error::Cluster("task write failed".into()));
                }
            }
        }
        let expected: Vec<usize> = sent;
        let (tx, rx) = std::sync::mpsc::sync_channel::<(usize, Result<Vec<u8>>)>(2);
        let mut reader_streams = Vec::with_capacity(streams.len());
        for s in streams.iter_mut() {
            reader_streams.push(s.try_clone().map_err(io_err)?);
        }
        thread::scope(|scope| {
            for (w, s) in reader_streams.into_iter().enumerate() {
                let n_expected = expected[w];
                if n_expected == 0 {
                    continue;
                }
                let tx = tx.clone();
                let recv = &self.bytes_recv;
                scope.spawn(move || {
                    let mut s = s;
                    for _ in 0..n_expected {
                        let got = read_frame(&mut s, recv);
                        let failed = got.is_err();
                        if tx.send((w, got)).is_err() || failed {
                            return;
                        }
                    }
                });
            }
            drop(tx);
            while let Ok((w, bytes)) = rx.recv() {
                if let Ok(bytes) = bytes {
                    let reply: Reply = match decode(&bytes) {
                        Ok(r) => r,
                        Err(e) => {
                            first_err.get_or_insert(e);
                            continue;
                        }
                    };
                    let p = reply.partition() as usize;
                    if !pending.contains(&(p, w)) {
                        first_err.get_or_insert_with(|| {
                            Error::Cluster(format!("unexpected partition {p} from rank {}", w + 1))
                        });
                        continue;
                    }
                    match reply {
                        Reply::Ok { payload, .. } => {
                            let got = decode::<T>(&payload)
                                .map_err(|e| Error::Cluster(format!("reply decode: {e}")));
                            match got.and_then(&mut on_ok) {
                                Ok(()) => {}
                                Err(e) => {
                                    if first_err.is_none() {
                                        first_err = Some(e)
                                    }
                                }
                            }
                            pending.retain(|(q, _)| *q != p);
                        }
                        Reply::Err { .. } => {
                            // worker compute failed: replay this partition locally
                            pending.retain(|(q, _)| *q != p);
                            match crate::metrics::task(job, offset + p, 1, || compute(p))
                                .and_then(&mut on_ok)
                            {
                                Ok(()) => {}
                                Err(e) => {
                                    if first_err.is_none() {
                                        first_err = Some(e)
                                    }
                                }
                            }
                        }
                    }
                } else {
                    crate::metrics::DISCONNECTS.fetch_add(1, Ordering::Relaxed);
                }
                // On disconnect, retain outstanding partitions for replay.
            }
        });
        for (p, _) in pending.clone() {
            match crate::metrics::task(job, offset + p, 1, || compute(p)).and_then(&mut on_ok) {
                Ok(()) => {}
                Err(e) => {
                    if first_err.is_none() {
                        first_err = Some(e)
                    }
                }
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

    fn connected_pair() -> (Cluster, Cluster) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        let make = |rank, stream: TcpStream| {
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            Cluster {
                rank,
                n: 2,
                streams: Mutex::new(vec![stream]),
                children: Mutex::new(Vec::new()),
                bytes_sent: AtomicU64::new(0),
                bytes_recv: AtomicU64::new(0),
            }
        };
        (make(0, server), make(1, client))
    }

    #[test]
    fn disconnect_replays_outstanding_partitions() {
        let (driver, worker) = connected_pair();
        let peer = thread::spawn(move || {
            let mut streams = worker.streams.lock().unwrap();
            for _ in 0..2 {
                let _: Task =
                    decode(&read_frame(&mut streams[0], &worker.bytes_recv).unwrap()).unwrap();
            }
            streams[0].shutdown(std::net::Shutdown::Both).unwrap();
        });
        let start = Instant::now();
        let mut values = Vec::new();
        driver
            .dispatch_each(
                4,
                |p| Ok(p as u64),
                |v| {
                    values.push(v);
                    Ok(())
                },
            )
            .unwrap();
        peer.join().unwrap();
        values.sort();
        assert_eq!(values, vec![0, 1, 2, 3]);
        assert!(start.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn duplicate_and_wrong_owner_replies_are_rejected() {
        for partitions in [[1, 1], [0, 3], [99, 3]] {
            let (driver, worker) = connected_pair();
            let peer = thread::spawn(move || {
                let mut streams = worker.streams.lock().unwrap();
                for p in partitions {
                    let _: Task =
                        decode(&read_frame(&mut streams[0], &worker.bytes_recv).unwrap()).unwrap();
                    let reply = Reply::Ok {
                        partition: p,
                        payload: encode(&(p as u64)).unwrap(),
                    };
                    write_frame(
                        &mut streams[0],
                        &encode(&reply).unwrap(),
                        &worker.bytes_sent,
                    )
                    .unwrap();
                }
            });
            let mut values = Vec::new();
            assert!(driver
                .dispatch_each(
                    4,
                    |p| Ok(p as u64),
                    |v| {
                        values.push(v);
                        Ok(())
                    }
                )
                .is_err());
            peer.join().unwrap();
            values.sort();
            assert_eq!(values, vec![0, 1, 2, 3]);
        }
    }

    #[test]
    fn bounded_windows_preserve_repeated_action_alignment() {
        let (driver, worker) = connected_pair();
        let peer = thread::spawn(move || {
            for _ in 0..3 {
                worker
                    .dispatch_each(19, |p| Ok(p as u64), |_| Ok(()))
                    .unwrap();
                worker.gather(vec![99u64]).unwrap();
            }
        });
        for _ in 0..3 {
            let mut values = Vec::new();
            driver
                .dispatch_each(
                    19,
                    |p| Ok(p as u64),
                    |v| {
                        values.push(v);
                        Ok(())
                    },
                )
                .unwrap();
            values.sort();
            assert_eq!(values, (0..19).collect::<Vec<u64>>());
            assert_eq!(driver.gather(vec![98u64]).unwrap(), vec![98, 99]);
        }
        peer.join().unwrap();
    }

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
