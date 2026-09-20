use spatter::prelude::*;

fn sc() -> SpatterContext {
    SpatterContext::builder()
        .master("local[2]")
        .get_or_create()
        .unwrap()
}

#[test]
fn map_is_single_result_stage() {
    let stages = sc().parallelize(vec![1, 2, 3]).map(|x| x + 1).stages();
    assert_eq!(stages.len(), 1);
    assert!(matches!(stages[0].kind, StageKind::Result));
}

#[test]
fn reduce_by_key_cuts_shuffle_then_result() {
    let stages = sc()
        .parallelize(vec![("a".to_string(), 1), ("a".to_string(), 2)])
        .reduce_by_key(|a, b| a + b)
        .stages();
    assert_eq!(stages.len(), 2);
    assert!(matches!(stages[0].kind, StageKind::ShuffleMap { .. }));
    assert!(matches!(stages[1].kind, StageKind::Result));
}

#[test]
fn shuffled_rdd_has_hash_partitioner() {
    let rdd = sc()
        .parallelize(vec![("a".to_string(), 1), ("b".to_string(), 2)])
        .reduce_by_key(|a, b| a + b);
    let p = rdd.partitioner().expect("partitioner");
    assert_eq!(p.num_partitions(), rdd.get_num_partitions());
}

#[test]
fn union_concatenates() {
    let sc = sc();
    let a = sc.parallelize(vec![1, 2]);
    let b = sc.parallelize(vec![3, 4]);
    let mut got = a.union(b).collect().unwrap();
    got.sort();
    assert_eq!(got, vec![1, 2, 3, 4]);
}

#[test]
fn distinct_drops_duplicates() {
    let mut got = sc()
        .parallelize(vec![1, 1, 2, 2, 3])
        .distinct()
        .collect()
        .unwrap();
    got.sort();
    assert_eq!(got, vec![1, 2, 3]);
}

#[test]
fn diamond_lineage_is_not_exponential() {
    let base = sc().parallelize((0..8).collect());
    let mut left = base.clone();
    let mut right = base;
    for _ in 0..12 {
        left = left.map(|x| x);
        right = right.map(|x| x);
    }
    let stages = left.union(right).stages();
    assert_eq!(stages.len(), 1);
}

#[test]
fn chained_shuffles_do_not_deadlock() {
    let mut got = sc()
        .parallelize(vec![
            ("a".to_string(), 1),
            ("a".to_string(), 2),
            ("b".to_string(), 3),
        ])
        .reduce_by_key(|a, b| a + b)
        .map(|(k, v)| (k, v * 10))
        .reduce_by_key(|a, b| a + b)
        .collect()
        .unwrap();
    got.sort();
    assert_eq!(got, vec![("a".to_string(), 30), ("b".to_string(), 30)]);
}

#[test]
fn executor_entry_requires_cluster_env() {
    assert!(matches!(run_executor(), Err(Error::Cluster(_))));
    assert_eq!(launch_mode(), LaunchMode::Driver);
}
