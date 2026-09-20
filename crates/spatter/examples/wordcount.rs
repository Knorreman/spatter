use std::env;
use std::time::Instant;

use spatter::prelude::*;

fn input_path() -> String {
    let args: Vec<String> = env::args().collect();
    let mut i = 1;
    while i < args.len() {
        if args[i] == "--cluster" {
            i += 2;
            continue;
        }
        if !args[i].starts_with('-') {
            return args[i].clone();
        }
        i += 1;
    }
    "/tmp/gospark-wc-big.txt".to_string()
}

fn peak_rss_kb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmHWM:"))
                .and_then(|l| l.split_whitespace().nth(1)?.parse().ok())
        })
        .unwrap_or(0)
}

fn main() -> Result<()> {
    let path = input_path();
    let t0 = Instant::now();
    let sc = SpatterContext::builder()
        .master("local[*]")
        .get_or_create()?;
    let mut counts = sc
        .read_text_file(path)?
        .flat_map(|line| {
            line.split_whitespace()
                .map(|w| w.to_string())
                .collect::<Vec<_>>()
        })
        .map(|w| (w, 1usize))
        .reduce_by_key(|a, b| a + b)
        .collect_to_driver()?;
    let job_ms = t0.elapsed().as_millis();
    let (sent, recv) = sc.net_bytes();
    let opens = sc.gather_u64(sc.file_partition_opens())?;
    let sent = sc.gather_u64(sent)?;
    let recv = sc.gather_u64(recv)?;
    let peak = sc.gather_u64(peak_rss_kb())?;
    if sc.is_driver() {
        counts.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        let sum: usize = counts.iter().map(|(_, c)| c).sum();
        eprintln!(
            "METRICS ranks={} keys={} sum={} job_ms={} sent={} recv={} opens={} peak_kb={}",
            sc.world_size(),
            counts.len(),
            sum,
            job_ms,
            sent.iter().sum::<u64>(),
            recv.iter().sum::<u64>(),
            opens.iter().sum::<u64>(),
            peak.iter().copied().max().unwrap_or(0)
        );
        for (word, count) in counts.iter().take(10) {
            println!("{word}: {count}");
        }
    }
    Ok(())
}
