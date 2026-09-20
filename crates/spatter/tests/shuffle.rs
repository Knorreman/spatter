use std::collections::HashMap;

use spatter::prelude::*;

fn sc() -> SpatterContext {
    SpatterContext::builder()
        .master("local[4]")
        .get_or_create()
        .unwrap()
}

#[test]
fn reduce_by_key_sums() {
    let pairs = vec![
        ("a".to_string(), 1),
        ("b".to_string(), 2),
        ("a".to_string(), 3),
        ("c".to_string(), 4),
        ("b".to_string(), 5),
        ("a".to_string(), 6),
    ];
    let mut got = sc()
        .parallelize(pairs)
        .reduce_by_key(|a, b| a + b)
        .collect()
        .unwrap();
    got.sort();
    assert_eq!(
        got,
        vec![
            ("a".to_string(), 10),
            ("b".to_string(), 7),
            ("c".to_string(), 4)
        ]
    );
}

#[test]
fn reduce_by_key_is_shuffle_dep() {
    let parent = sc().parallelize(vec![("x".to_string(), 1), ("y".to_string(), 2)]);
    let shuffled = parent.clone().reduce_by_key(|a, b| a + b);
    match shuffled.dependencies() {
        [Dependency::Shuffle { parent: p, .. }] => assert_eq!(*p, parent.id()),
        other => panic!("expected shuffle dep, got {other:?}"),
    }
}

#[test]
fn group_by_key_collects_values() {
    let mut got = sc()
        .parallelize(vec![
            ("a".to_string(), 1),
            ("a".to_string(), 2),
            ("b".to_string(), 3),
        ])
        .group_by_key()
        .collect()
        .unwrap();
    got.sort_by(|a, b| a.0.cmp(&b.0));
    for (_, vs) in &mut got {
        vs.sort();
    }
    assert_eq!(
        got,
        vec![("a".to_string(), vec![1, 2]), ("b".to_string(), vec![3])]
    );
}

#[test]
fn reduce_by_key_matches_hashmap() {
    let data: Vec<(i32, i32)> = (0..1000).map(|i| (i % 17, i)).collect();
    let mut expected = HashMap::new();
    for (k, v) in &data {
        *expected.entry(*k).or_insert(0) += v;
    }
    let got = sc()
        .parallelize(data)
        .reduce_by_key(|a, b| a + b)
        .collect()
        .unwrap();
    let got: HashMap<_, _> = got.into_iter().collect();
    assert_eq!(got, expected);
}

#[test]
fn shuffle_recomputes_per_action() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    let hits = Arc::new(AtomicUsize::new(0));
    let h = Arc::clone(&hits);
    let rdd = sc()
        .parallelize(vec![("a".to_string(), 1), ("a".to_string(), 2)])
        .map(move |x| {
            h.fetch_add(1, Ordering::SeqCst);
            x
        })
        .reduce_by_key(|a, b| a + b);
    assert_eq!(rdd.clone().collect().unwrap().len(), 1);
    assert_eq!(rdd.collect().unwrap().len(), 1);
    assert_eq!(hits.load(Ordering::SeqCst), 4);
}

#[test]
fn combine_by_key_counts() {
    let mut got = sc()
        .parallelize(vec![
            ("a".to_string(), 1),
            ("a".to_string(), 2),
            ("b".to_string(), 3),
        ])
        .combine_by_key(
            |v| (1, v),
            |(n, s), v| (n + 1, s + v),
            |a, b| (a.0 + b.0, a.1 + b.1),
        )
        .collect()
        .unwrap();
    got.sort();
    assert_eq!(
        got,
        vec![("a".to_string(), (2, 3)), ("b".to_string(), (1, 3))]
    );
}

#[test]
fn aggregate_by_key_sum() {
    let mut got = sc()
        .parallelize(vec![
            ("a".to_string(), 1),
            ("a".to_string(), 2),
            ("b".to_string(), 3),
        ])
        .aggregate_by_key(0, |acc, v| acc + v, |a, b| a + b)
        .collect()
        .unwrap();
    got.sort();
    assert_eq!(got, vec![("a".to_string(), 3), ("b".to_string(), 3)]);
}

#[test]
fn cancel_stops_unbounded_sibling() {
    let err = sc()
        .parallelize_partitions(vec![0, 1], 2)
        .unwrap()
        .flat_map(|x| -> Box<dyn Iterator<Item = i32> + Send> {
            if x == 0 {
                Box::new(std::iter::repeat(0))
            } else {
                Box::new(std::iter::from_fn(|| panic!("fail-fast")))
            }
        })
        .collect()
        .unwrap_err();
    match err {
        Error::PartitionPanic { message, .. } => assert!(message.contains("fail-fast")),
        other => panic!("expected PartitionPanic, got {other:?}"),
    }
}
