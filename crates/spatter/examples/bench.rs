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
    env::var("BENCH_INPUT").unwrap_or_else(|_| "/tmp/gospark-wc-big.txt".into())
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

fn report(sc: &SpatterContext, msg: String) {
    if sc.is_driver() {
        eprintln!("{msg}");
    }
}

fn wordcount(sc: &SpatterContext, path: &str) -> Result<()> {
    let t0 = Instant::now();
    let counts = sc
        .read_text_file(path)?
        .flat_map(|line| {
            line.split_whitespace()
                .map(|w| w.to_string())
                .collect::<Vec<_>>()
        })
        .map(|w| (w, 1usize))
        .reduce_by_key(|a, b| a + b)
        .collect_to_driver()?;
    let ms = t0.elapsed().as_millis();
    let keys = counts.len();
    let sum: usize = counts.iter().map(|(_, c)| c).sum();
    report(
        sc,
        format!("BENCH wordcount exec_ms={ms} keys={keys} sum={sum}"),
    );
    Ok(())
}

fn skewed_agg(sc: &SpatterContext, path: &str) -> Result<()> {
    let t0 = Instant::now();
    let counts = sc
        .read_text_file(path)?
        .flat_map(|line| {
            line.split_whitespace()
                .flat_map(|w| [("hot".to_string(), 9usize), (format!("key:{w}"), 1usize)])
                .collect::<Vec<_>>()
        })
        .reduce_by_key(|a, b| a + b)
        .collect_to_driver()?;
    let ms = t0.elapsed().as_millis();
    let hot = counts
        .iter()
        .find(|(k, _)| k == "hot")
        .map(|(_, v)| *v)
        .unwrap_or(0);
    let sum: usize = counts.iter().map(|(_, v)| v).sum();
    report(
        sc,
        format!(
            "BENCH skew exec_ms={ms} keys={} hot={hot} sum={sum}",
            counts.len()
        ),
    );
    Ok(())
}

fn iterative_reuse(sc: &SpatterContext, path: &str) -> Result<()> {
    let t0 = Instant::now();
    let rdd = sc
        .read_text_file(path)?
        .flat_map(|line| {
            line.split_whitespace()
                .map(|w| w.to_string())
                .collect::<Vec<_>>()
        })
        .map(|w| (w, 1usize))
        .reduce_by_key(|a, b| a + b);
    let mut runs = Vec::new();
    for _ in 0..3 {
        let n = rdd.count()?;
        runs.push(n);
    }
    let ms = t0.elapsed().as_millis();
    report(sc, format!("BENCH reuse exec_ms={ms} runs={runs:?}"));
    Ok(())
}

fn main() -> Result<()> {
    let path = input_path();
    let scenario = env::var("BENCH_SCENARIO").unwrap_or_else(|_| "wordcount".into());
    let t0 = Instant::now();
    let sc = SpatterContext::builder()
        .master(env::var("BENCH_MASTER").unwrap_or_else(|_| "local[16]".into()))
        .get_or_create()?;
    let startup_ms = t0.elapsed().as_millis();
    report(
        &sc,
        format!("BENCH startup_ms={startup_ms} ranks={}", sc.world_size()),
    );
    match scenario.as_str() {
        "wordcount" => wordcount(&sc, &path)?,
        "skew" => skewed_agg(&sc, &path)?,
        "reuse" => iterative_reuse(&sc, &path)?,
        other => {
            return Err(Error::Cluster(format!("unknown BENCH_SCENARIO: {other}")));
        }
    }
    let (sent, recv) = sc.net_bytes();
    let peaks = sc.gather_u64(peak_rss_kb())?;
    let sent = sc.gather_u64(sent)?;
    let recv = sc.gather_u64(recv)?;
    report(
        &sc,
        format!(
            "BENCH end sent={} recv={} peak_kb={} rank_peaks_kb={peaks:?} sum_rank_peaks_kb={} scenario={scenario}",
            sent.iter().sum::<u64>(), recv.iter().sum::<u64>(),
            peaks.iter().copied().max().unwrap_or(0), peaks.iter().sum::<u64>()
        ),
    );
    Ok(())
}
