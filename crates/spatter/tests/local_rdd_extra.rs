use spatter::prelude::*;

fn context(master: &str) -> SpatterContext {
    SpatterContext::builder()
        .master(master)
        .get_or_create()
        .unwrap()
}

#[test]
fn local_one_uses_one_partition_and_preserves_order() {
    let sc = context("local[1]");
    let rdd = sc.parallelize(vec![5, 4, 3, 2, 1]);

    assert_eq!(sc.parallelism(), 1);
    assert_eq!(rdd.get_num_partitions(), 1);
    assert_eq!(rdd.collect().unwrap(), vec![5, 4, 3, 2, 1]);
}

#[test]
fn empty_rdd_collects_and_counts() {
    let rdd = context("local[2]").parallelize(Vec::<i32>::new());

    assert_eq!(rdd.collect().unwrap(), Vec::<i32>::new());
    assert_eq!(rdd.count().unwrap(), 0);
}

#[test]
fn take_zero_returns_empty() {
    let rdd = context("local[3]").parallelize((0..20).collect());

    assert_eq!(rdd.take(0).unwrap(), Vec::<i32>::new());
}

#[test]
fn large_map_reduce_matches_iterator() {
    let data = (0_i64..10_000).collect::<Vec<_>>();
    let expected = data.iter().copied().map(|x| x * 3 - 7).sum::<i64>();
    let actual = context("local[4]")
        .parallelize(data)
        .map(|x| x * 3 - 7)
        .reduce(|left, right| left + right)
        .unwrap();

    assert_eq!(actual, expected);
}

#[test]
fn nested_map_filter_pipeline_matches_iterator() {
    let data = (0..100).collect::<Vec<_>>();
    let expected = data
        .iter()
        .copied()
        .map(|x| x + 1)
        .filter(|x| x % 3 == 0)
        .map(|x| x * x)
        .filter(|x| x % 2 == 0)
        .collect::<Vec<_>>();
    let actual = context("local[4]")
        .parallelize(data)
        .map(|x| x + 1)
        .filter(|x| x % 3 == 0)
        .map(|x| x * x)
        .filter(|x| x % 2 == 0)
        .collect()
        .unwrap();

    assert_eq!(actual, expected);
}

#[test]
fn string_rdd_supports_transformations_and_actions() {
    let rdd = context("local[1]")
        .parallelize(vec![
            "rust".to_string(),
            "spark".to_string(),
            "rdd".to_string(),
        ])
        .filter(|value| value.len() > 3)
        .map(|value| value.to_uppercase());

    assert_eq!(rdd.count().unwrap(), 2);
    assert_eq!(
        rdd.collect().unwrap(),
        vec!["RUST".to_string(), "SPARK".to_string()]
    );
}
