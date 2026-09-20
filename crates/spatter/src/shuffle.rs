use serde::de::DeserializeOwned;
use serde::Serialize;
use spatter_core::{Error, Result};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::thread;

pub fn hash_bucket<K: Hash>(key: &K, n: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() as usize) % n.max(1)
}

pub fn combine<K, V, F>(map: &mut HashMap<K, V>, key: K, val: V, f: &F)
where
    K: Eq + Hash,
    F: Fn(V, V) -> V,
{
    match map.remove(&key) {
        Some(old) => {
            map.insert(key, f(old, val));
        }
        None => {
            map.insert(key, val);
        }
    }
}

pub fn write_map_combine<K, V, C, Create, MergeV>(
    n_out: usize,
    pairs: impl Iterator<Item = (K, V)>,
    create: &Create,
    merge_value: &MergeV,
    cancel: &AtomicBool,
) -> Result<Vec<Vec<(K, C)>>>
where
    K: Eq + Hash,
    Create: Fn(V) -> C,
    MergeV: Fn(C, V) -> C,
{
    write_map_combine_limited(
        n_out,
        pairs,
        create,
        merge_value,
        cancel,
        spill_limit_bytes(),
    )
}

pub(crate) fn write_map_combine_limited<K, V, C, Create, MergeV>(
    n_out: usize,
    pairs: impl Iterator<Item = (K, V)>,
    create: &Create,
    merge_value: &MergeV,
    cancel: &AtomicBool,
    limit: usize,
) -> Result<Vec<Vec<(K, C)>>>
where
    K: Eq + Hash,
    Create: Fn(V) -> C,
    MergeV: Fn(C, V) -> C,
{
    use std::sync::atomic::Ordering;
    let mut combined: HashMap<K, C> = HashMap::new();
    let mut buckets = Vec::with_capacity(n_out);
    for _ in 0..n_out {
        buckets.push(Vec::new());
    }
    let mut bytes = 0usize;
    let flush = |combined: &mut HashMap<K, C>, buckets: &mut [Vec<(K, C)>], bytes: &mut usize| {
        for (k, c) in combined.drain() {
            buckets[hash_bucket(&k, n_out)].push((k, c));
        }
        *bytes = 0;
    };
    for (k, v) in pairs {
        if cancel.load(Ordering::Relaxed) {
            return Err(Error::Cancelled);
        }
        match combined.remove(&k) {
            Some(c) => {
                combined.insert(k, merge_value(c, v));
            }
            None => {
                combined.insert(k, create(v));
            }
        }
        bytes = bytes.saturating_add(64);
        if bytes >= limit {
            flush(&mut combined, &mut buckets, &mut bytes);
        }
    }
    flush(&mut combined, &mut buckets, &mut bytes);
    Ok(buckets)
}

pub fn reduce_bucket<K, V, F>(pairs: Vec<(K, V)>, f: &F) -> Vec<(K, V)>
where
    K: Eq + Hash,
    F: Fn(V, V) -> V,
{
    let mut map = HashMap::new();
    for (k, v) in pairs {
        combine(&mut map, k, v, f);
    }
    map.into_iter().collect()
}

pub fn parallel_reduce_buckets<K, V, F>(
    merged: Vec<Vec<(K, V)>>,
    f: &F,
    parallelism: usize,
) -> Vec<Vec<(K, V)>>
where
    K: Eq + Hash + Send,
    V: Send,
    F: Fn(V, V) -> V + Sync,
{
    let n = merged.len();
    if n <= 1 {
        return merged.into_iter().map(|b| reduce_bucket(b, f)).collect();
    }
    let threads = parallelism.max(1).min(n);
    type Slot<K, V> = Option<Vec<(K, V)>>;
    let slots = std::sync::Mutex::new(merged.into_iter().map(Some).collect::<Vec<Slot<K, V>>>());
    thread::scope(|scope| {
        let mut joins = Vec::with_capacity(threads);
        for t in 0..threads {
            let slots = &slots;
            joins.push(scope.spawn(move || {
                let mut out = Vec::new();
                let mut i = t;
                while i < n {
                    let bucket = slots.lock().expect("slots")[i].take().expect("bucket");
                    out.push((i, reduce_bucket(bucket, f)));
                    i += threads;
                }
                out
            }));
        }
        let mut result: Vec<Vec<(K, V)>> = (0..n).map(|_| Vec::new()).collect();
        for join in joins {
            for (i, bucket) in join.join().expect("reduce") {
                result[i] = bucket;
            }
        }
        result
    })
}

pub enum ShuffleBlocks<T> {
    Memory(Vec<Vec<T>>),
    Spilled(Vec<PathBuf>),
}

impl<T> Drop for ShuffleBlocks<T> {
    fn drop(&mut self) {
        if let ShuffleBlocks::Spilled(paths) = self {
            for p in paths {
                let _ = std::fs::remove_file(p);
            }
        }
    }
}

fn spill_limit_bytes() -> usize {
    std::env::var("SPATTER_SPILL_MB")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(64)
        .saturating_mul(1024 * 1024)
}

pub struct IncrementalBuckets<T: Serialize> {
    n_out: usize,
    id: u64,
    seq: u64,
    memory: Vec<Vec<T>>,
    paths: Vec<PathBuf>,
    files: Vec<Option<File>>,
    bytes: usize,
    spilled: bool,
}

impl<T: Serialize> IncrementalBuckets<T> {
    pub fn new(n_out: usize, id: u64) -> Self {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Self {
            n_out,
            id,
            seq,
            memory: (0..n_out).map(|_| Vec::new()).collect(),
            paths: Vec::new(),
            files: (0..n_out).map(|_| None).collect(),
            bytes: 0,
            spilled: false,
        }
    }

    pub fn push(&mut self, buckets: Vec<Vec<T>>) -> Result<()> {
        for (i, chunk) in buckets.into_iter().enumerate() {
            let add = bincode::serialized_size(&chunk)
                .map(|n| n as usize)
                .unwrap_or_else(|_| chunk.len().saturating_mul(64));
            self.bytes = self.bytes.saturating_add(add);
            if self.spilled {
                self.append(i, &chunk)?;
            } else {
                self.memory[i].extend(chunk);
            }
        }
        if !self.spilled && self.bytes >= spill_limit_bytes() {
            self.flush_to_disk()?;
        }
        Ok(())
    }

    fn flush_to_disk(&mut self) -> Result<()> {
        let pid = std::process::id();
        let mut paths = Vec::with_capacity(self.n_out);
        let mut files = Vec::with_capacity(self.n_out);
        for i in 0..self.n_out {
            let path = std::env::temp_dir().join(format!(
                "spatter-spill-{}-{}-{}-{}.bin",
                pid, self.id, self.seq, i
            ));
            let wrote = (|| {
                let mut file = File::create(&path).map_err(|e| Error::Io(e.to_string()))?;
                append_chunk(&mut file, &self.memory[i])?;
                Ok(file)
            })();
            match wrote {
                Ok(file) => {
                    paths.push(path);
                    files.push(Some(file));
                }
                Err(e) => {
                    let _ = std::fs::remove_file(&path);
                    for p in &paths {
                        let _ = std::fs::remove_file(p);
                    }
                    return Err(e);
                }
            }
        }
        self.memory = (0..self.n_out).map(|_| Vec::new()).collect();
        self.paths = paths;
        self.files = files;
        self.spilled = true;
        Ok(())
    }

    fn append(&mut self, i: usize, chunk: &[T]) -> Result<()> {
        if chunk.is_empty() {
            return Ok(());
        }
        let file = self.files[i]
            .as_mut()
            .ok_or_else(|| Error::Io("spill file missing".into()))?;
        append_chunk(file, chunk)
    }

    pub fn finish(mut self) -> Result<ShuffleBlocks<T>> {
        self.files.clear();
        if self.spilled {
            let paths = std::mem::take(&mut self.paths);
            Ok(ShuffleBlocks::Spilled(paths))
        } else {
            Ok(ShuffleBlocks::Memory(std::mem::take(&mut self.memory)))
        }
    }
}

impl<T: Serialize> Drop for IncrementalBuckets<T> {
    fn drop(&mut self) {
        self.files.clear();
        for p in &self.paths {
            let _ = std::fs::remove_file(p);
        }
    }
}

fn append_chunk<T: Serialize>(file: &mut File, chunk: &[T]) -> Result<()> {
    if chunk.is_empty() {
        return Ok(());
    }
    let bytes = bincode::serialize(chunk).map_err(|e| Error::Io(e.to_string()))?;
    const MAX: usize = 32 * 1024 * 1024;
    if bytes.len() > MAX && chunk.len() > 1 {
        let mid = chunk.len() / 2;
        append_chunk(file, &chunk[..mid])?;
        return append_chunk(file, &chunk[mid..]);
    }
    if bytes.len() > 64 * 1024 * 1024 {
        return Err(Error::Io("spill record too large".into()));
    }
    file.write_all(&(bytes.len() as u32).to_be_bytes())
        .map_err(|e| Error::Io(e.to_string()))?;
    file.write_all(&bytes).map_err(|e| Error::Io(e.to_string()))
}

pub fn reduce_spilled_path<K, V, F>(path: &PathBuf, f: &F) -> Result<Vec<(K, V)>>
where
    K: Eq + Hash + DeserializeOwned,
    V: DeserializeOwned,
    F: Fn(V, V) -> V,
{
    let mut file = File::open(path).map_err(|e| Error::Io(e.to_string()))?;
    let mut map = HashMap::new();
    loop {
        let mut lenb = [0u8; 4];
        let nread = file.read(&mut lenb).map_err(|e| Error::Io(e.to_string()))?;
        if nread == 0 {
            break;
        }
        if nread < 4 {
            return Err(Error::Io("truncated spill header".into()));
        }
        let len = u32::from_be_bytes(lenb) as usize;
        if len > 64 * 1024 * 1024 {
            return Err(Error::Io("spill chunk too large".into()));
        }
        let mut buf = vec![0u8; len];
        file.read_exact(&mut buf)
            .map_err(|e| Error::Io(e.to_string()))?;
        let chunk: Vec<(K, V)> =
            bincode::deserialize(&buf).map_err(|e| Error::Io(e.to_string()))?;
        for (k, v) in chunk {
            combine(&mut map, k, v, f);
        }
    }
    Ok(map.into_iter().collect())
}

pub fn read_chunks<T: DeserializeOwned>(path: &PathBuf) -> Result<Vec<T>> {
    let mut file = File::open(path).map_err(|e| Error::Io(e.to_string()))?;
    let mut out = Vec::new();
    loop {
        let mut lenb = [0u8; 4];
        let nread = file.read(&mut lenb).map_err(|e| Error::Io(e.to_string()))?;
        if nread == 0 {
            break;
        }
        if nread < 4 {
            return Err(Error::Io("truncated spill header".into()));
        }
        let len = u32::from_be_bytes(lenb) as usize;
        if len > 64 * 1024 * 1024 {
            return Err(Error::Io("spill chunk too large".into()));
        }
        let mut buf = vec![0u8; len];
        file.read_exact(&mut buf)
            .map_err(|e| Error::Io(e.to_string()))?;
        let chunk: Vec<T> = bincode::deserialize(&buf).map_err(|e| Error::Io(e.to_string()))?;
        out.extend(chunk);
    }
    Ok(out)
}

pub fn store_blocks<T: Serialize>(id: u64, buckets: Vec<Vec<T>>) -> Result<ShuffleBlocks<T>> {
    let pairs: usize = buckets.iter().map(|b| b.len()).sum();
    let est = pairs.saturating_mul(64);
    if est < spill_limit_bytes() {
        return Ok(ShuffleBlocks::Memory(buckets));
    }
    let n_buckets = buckets.len();
    let pid = std::process::id();
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut paths = Vec::with_capacity(n_buckets);
    for (i, bucket) in buckets.into_iter().enumerate() {
        let path = std::env::temp_dir().join(format!("spatter-spill-{pid}-{id}-{seq}-{i}.bin"));
        let wrote = (|| {
            let mut file = File::create(&path).map_err(|e| Error::Io(e.to_string()))?;
            append_chunk(&mut file, &bucket)
        })();
        if let Err(e) = wrote {
            let _ = std::fs::remove_file(&path);
            for p in &paths {
                let _ = std::fs::remove_file(p);
            }
            return Err(e);
        }
        paths.push(path);
    }
    Ok(ShuffleBlocks::Spilled(paths))
}

impl<T> ShuffleBlocks<T>
where
    T: Clone + DeserializeOwned,
{
    pub fn partition(&self, p: usize) -> Result<Vec<T>> {
        match self {
            ShuffleBlocks::Memory(buckets) => Ok(buckets[p].clone()),
            ShuffleBlocks::Spilled(paths) => read_chunks(&paths[p]),
        }
    }
}

#[cfg(test)]
mod spill_tests {
    use super::*;

    #[test]
    fn incremental_spill_roundtrip() {
        let mut acc = IncrementalBuckets::new(2, 1);
        acc.bytes = spill_limit_bytes().saturating_sub(1);
        acc.push(vec![vec![("a".to_string(), 1)], vec![]]).unwrap();
        acc.push(vec![vec![("a".to_string(), 2)], vec![("b".to_string(), 3)]])
            .unwrap();
        let blocks = acc.finish().unwrap();
        match &blocks {
            ShuffleBlocks::Spilled(_) => {
                let p0 = blocks.partition(0).unwrap();
                let p1 = blocks.partition(1).unwrap();
                assert!(p0
                    .iter()
                    .any(|(k, v)| k == "a" && *v == 1 || k == "a" && *v == 2));
                assert!(p1.iter().any(|(k, _)| k == "b"));
            }
            ShuffleBlocks::Memory(_) => panic!("expected spill"),
        }
    }

    #[test]
    fn map_combine_flushes_partial_combiners() {
        let cancel = AtomicBool::new(false);
        let pairs = vec![("a", 1), ("a", 2), ("b", 3)];
        let buckets =
            write_map_combine_limited(1, pairs.into_iter(), &|v: i32| v, &|a, b| a + b, &cancel, 0)
                .unwrap();
        assert_eq!(buckets.len(), 1);
        let reduced = reduce_bucket(buckets.into_iter().next().unwrap(), &|a, b| a + b);
        let mut reduced = reduced;
        reduced.sort();
        assert_eq!(reduced, vec![("a", 3), ("b", 3)]);
    }
}
