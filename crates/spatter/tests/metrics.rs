use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::{Duration, Instant};

use spatter::metrics::{render, MetricsServer};
use spatter::prelude::*;

fn scrape(addr: std::net::SocketAddr, path: &str) -> String {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    write!(stream, "GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n").unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    response
}

fn counter(text: &str, name: &str) -> u64 {
    text.lines()
        .find_map(|line| line.strip_prefix(&format!("spatter_{name} ")))
        .unwrap()
        .parse()
        .unwrap()
}

#[test]
fn endpoint_observes_real_tasks_and_closes_on_drop() {
    let server = MetricsServer::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = server.local_addr();
    let before = counter(&render(), "task_attempts_total");
    let sc = SpatterContext::builder()
        .master("local[2]")
        .get_or_create()
        .unwrap();
    assert_eq!(sc.parallelize(vec![1, 2, 3]).count().unwrap(), 3);
    let response = scrape(addr, "/metrics");
    assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert!(counter(&response, "task_attempts_total") >= before + 2);
    assert!(scrape(addr, "/missing").starts_with("HTTP/1.1 404"));
    let _idle = TcpStream::connect(addr).unwrap();
    std::thread::sleep(Duration::from_millis(30));
    let started = Instant::now();
    drop(server);
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(TcpListener::bind(addr).is_ok());
}

#[test]
fn retry_counters_observe_local_panic_recovery() {
    let before = counter(&render(), "task_retries_total");
    let sc = SpatterContext::builder()
        .master("local")
        .get_or_create()
        .unwrap();
    let first = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let data = sc.parallelize(vec![7]).map(move |v| {
        if first.swap(false, std::sync::atomic::Ordering::Relaxed) {
            panic!("transient");
        }
        v
    });
    assert_eq!(data.collect().unwrap(), vec![7]);
    assert!(counter(&render(), "task_retries_total") > before);
    assert!(counter(&render(), "task_failures_total") > 0);
}

#[test]
fn shuffle_activity_is_visible_in_counters() {
    let before = render();
    let sc = SpatterContext::builder()
        .master("local[2]")
        .get_or_create()
        .unwrap();
    let counts = sc
        .parallelize(vec![
            ("hot".to_owned(), 9u64),
            ("hot".to_owned(), 9),
            ("cold".to_owned(), 1),
        ])
        .reduce_by_key(|a, b| a + b)
        .collect()
        .unwrap();
    assert_eq!(counts.iter().map(|(_, v)| v).sum::<u64>(), 19);
    let after = render();
    assert!(
        counter(&after, "shuffle_map_records_total")
            > counter(&before, "shuffle_map_records_total")
    );
    if std::env::var("SPATTER_SPILL_MB").as_deref() == Ok("0") {
        assert!(
            counter(&after, "spill_written_bytes_total")
                > counter(&before, "spill_written_bytes_total")
        );
        assert!(
            counter(&after, "spill_read_bytes_total") > counter(&before, "spill_read_bytes_total")
        );
    }
}
