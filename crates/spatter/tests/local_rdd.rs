use spatter::prelude::*;

fn sc() -> SpatterContext {
    SpatterContext::builder()
        .master("local[4]")
        .get_or_create()
        .unwrap()
}

#[test]
fn map_reduce_matches_iterator() {
    let data = vec![1, 2, 3, 4, 56];
    let expected: i32 = data.iter().map(|x| x * 2).sum();
    let got = sc()
        .parallelize(data)
        .map(|x| x * 2)
        .reduce(|a, b| a + b)
        .unwrap();
    assert_eq!(got, expected);
}

#[test]
fn collect_preserves_values() {
    let data = (0..20).collect::<Vec<_>>();
    let got = sc().parallelize(data.clone()).collect().unwrap();
    assert_eq!(got, data);
}

#[test]
fn filter_even() {
    let got = sc()
        .parallelize((0..10).collect())
        .filter(|x| x % 2 == 0)
        .collect()
        .unwrap();
    assert_eq!(got, vec![0, 2, 4, 6, 8]);
}

#[test]
fn flat_map_chars() {
    let got = sc()
        .parallelize(vec!["ab".to_string(), "c".to_string()])
        .flat_map(|s| s.chars().collect::<Vec<_>>())
        .collect()
        .unwrap();
    assert_eq!(got.len(), 3);
}

#[test]
fn count_and_take() {
    let rdd = sc().parallelize((0..100).collect());
    assert_eq!(rdd.count().unwrap(), 100);
    assert_eq!(rdd.take(3).unwrap(), vec![0, 1, 2]);
}

#[test]
fn reduce_empty_errors() {
    let err = sc()
        .parallelize(Vec::<i32>::new())
        .reduce(|a, b| a + b)
        .unwrap_err();
    assert!(matches!(err, Error::EmptyRdd));
}

#[test]
fn map_pipelines_with_capture() {
    let k = 3;
    let got = sc()
        .parallelize(vec![1, 2, 3])
        .map(move |x| x * k)
        .collect()
        .unwrap();
    assert_eq!(got, vec![3, 6, 9]);
}

#[test]
fn zero_partitions_rejected() {
    match sc().parallelize_partitions(vec![1, 2, 3], 0) {
        Err(Error::InvalidParallelism(0)) => {}
        Ok(_) => panic!("expected InvalidParallelism"),
        Err(e) => panic!("expected InvalidParallelism, got {e:?}"),
    }
}

#[test]
fn rdd_clone_allows_branching() {
    let base = sc().parallelize(vec![1, 2, 3, 4]);
    let a = base.clone().map(|x| x + 1).collect().unwrap();
    let b = base.map(|x| x * 10).collect().unwrap();
    assert_eq!(a, vec![2, 3, 4, 5]);
    assert_eq!(b, vec![10, 20, 30, 40]);
}

#[test]
fn invalid_master() {
    match SpatterContext::builder().master("k8s://").get_or_create() {
        Err(Error::InvalidMaster(_)) => {}
        Ok(_) => panic!("expected InvalidMaster"),
        Err(e) => panic!("expected InvalidMaster, got {e:?}"),
    }
}
