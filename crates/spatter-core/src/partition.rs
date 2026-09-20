use std::thread::available_parallelism;

pub fn default_parallelism() -> usize {
    available_parallelism().map(|n| n.get()).unwrap_or(1)
}

pub fn parse_local_threads(master: &str) -> crate::Result<usize> {
    let master = master.trim();
    if master == "local" {
        return Ok(1);
    }
    if master == "local[*]" {
        return Ok(default_parallelism());
    }
    if let Some(inner) = master
        .strip_prefix("local[")
        .and_then(|s| s.strip_suffix(']'))
    {
        let n: usize = inner
            .parse()
            .map_err(|_| crate::Error::InvalidMaster(master.to_string()))?;
        if n == 0 {
            return Err(crate::Error::InvalidParallelism(0));
        }
        return Ok(n);
    }
    Err(crate::Error::InvalidMaster(master.to_string()))
}

pub fn split_contiguous<T>(data: Vec<T>, n_partitions: usize) -> crate::Result<Vec<Vec<T>>> {
    if n_partitions == 0 {
        return Err(crate::Error::InvalidParallelism(0));
    }
    let len = data.len();
    let mut parts: Vec<Vec<T>> = (0..n_partitions).map(|_| Vec::new()).collect();
    if len == 0 {
        return Ok(parts);
    }
    let mut data = data.into_iter();
    for (i, part) in parts.iter_mut().enumerate() {
        let start = (i as u128 * len as u128 / n_partitions as u128) as usize;
        let end = ((i as u128 + 1) * len as u128 / n_partitions as u128) as usize;
        part.extend(data.by_ref().take(end.saturating_sub(start)));
    }
    Ok(parts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_local_meanings() {
        assert_eq!(parse_local_threads("local").unwrap(), 1);
        assert!(parse_local_threads("local[*]").unwrap() >= 1);
        assert_eq!(parse_local_threads("local[4]").unwrap(), 4);
    }

    #[test]
    fn parse_rejects_cluster() {
        assert!(parse_local_threads("spatter://localhost:7077").is_err());
        assert!(parse_local_threads("local[0]").is_err());
    }

    #[test]
    fn contiguous_splits_like_spark() {
        let parts = split_contiguous(vec![1, 2, 3, 4, 5], 2).unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], vec![1, 2]);
        assert_eq!(parts[1], vec![3, 4, 5]);
    }

    #[test]
    fn contiguous_rejects_zero() {
        assert!(split_contiguous(vec![1], 0).is_err());
    }

    #[test]
    fn contiguous_empty_and_more_parts_than_items() {
        let empty: Vec<i32> = split_contiguous(Vec::new(), 3)
            .unwrap()
            .into_iter()
            .flatten()
            .collect();
        assert!(empty.is_empty());
        let parts = split_contiguous(vec![1, 2], 5).unwrap();
        assert_eq!(parts.len(), 5);
        let flat: Vec<_> = parts.into_iter().flatten().collect();
        assert_eq!(flat, vec![1, 2]);
    }

    #[test]
    fn parse_malformed() {
        assert!(matches!(
            parse_local_threads("local[]"),
            Err(crate::Error::InvalidMaster(_))
        ));
        assert!(matches!(parse_local_threads(" local[4] "), Ok(4)));
    }
}
