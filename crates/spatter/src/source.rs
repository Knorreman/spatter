use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

pub static FILE_PARTITION_OPENS: AtomicU64 = AtomicU64::new(0);

use spatter_core::{Error, Result};

pub fn byte_splits(len: u64, n_partitions: usize) -> Result<Vec<(u64, u64)>> {
    if n_partitions == 0 {
        return Err(Error::InvalidParallelism(0));
    }
    let mut splits = Vec::with_capacity(n_partitions);
    for i in 0..n_partitions {
        let start = i as u128 * len as u128 / n_partitions as u128;
        let end = (i as u128 + 1) * len as u128 / n_partitions as u128;
        splits.push((start as u64, end as u64));
    }
    Ok(splits)
}

pub struct FileLines {
    reader: BufReader<File>,
    end: u64,
    done: bool,
}

impl FileLines {
    pub fn open(path: &Path, start: u64, end: u64) -> Result<Self> {
        FILE_PARTITION_OPENS.fetch_add(1, Ordering::Relaxed);
        let mut file = File::open(path).map_err(|e| Error::Io(e.to_string()))?;
        if start > 0 {
            file.seek(SeekFrom::Start(start - 1))
                .map_err(|e| Error::Io(e.to_string()))?;
            let mut prev = [0u8; 1];
            file.read_exact(&mut prev)
                .map_err(|e| Error::Io(e.to_string()))?;
            let mut reader = BufReader::new(file);
            if prev[0] != b'\n' {
                let mut skip = String::new();
                reader
                    .read_line(&mut skip)
                    .map_err(|e| Error::Io(e.to_string()))?;
            }
            return Ok(Self {
                reader,
                end,
                done: start >= end,
            });
        }
        file.seek(SeekFrom::Start(0))
            .map_err(|e| Error::Io(e.to_string()))?;
        let reader = BufReader::new(file);
        Ok(Self {
            reader,
            end,
            done: start >= end,
        })
    }
}

impl Iterator for FileLines {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let pos = match self.reader.stream_position() {
            Ok(p) => p,
            Err(_) => {
                self.done = true;
                return None;
            }
        };
        if pos >= self.end {
            self.done = true;
            return None;
        }
        let mut line = String::new();
        match self.reader.read_line(&mut line) {
            Ok(0) => {
                self.done = true;
                None
            }
            Ok(_) => {
                if line.ends_with('\n') {
                    line.pop();
                    if line.ends_with('\r') {
                        line.pop();
                    }
                }
                Some(line)
            }
            Err(e) => panic!("read_text_file: {e}"),
        }
    }
}

pub fn text_file_meta(
    path: impl AsRef<Path>,
    n_partitions: usize,
) -> Result<(PathBuf, Vec<(u64, u64)>)> {
    let path = path.as_ref().to_path_buf();
    let len = std::fs::metadata(&path)
        .map_err(|e| Error::Io(e.to_string()))?
        .len();
    let splits = byte_splits(len, n_partitions)?;
    Ok((path, splits))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_splits_cover_file() {
        let splits = byte_splits(10, 3).unwrap();
        assert_eq!(splits, vec![(0, 3), (3, 6), (6, 10)]);
    }
}
