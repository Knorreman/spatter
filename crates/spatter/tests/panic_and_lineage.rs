use spatter::prelude::*;

fn sc() -> SpatterContext {
    SpatterContext::builder()
        .master("local[4]")
        .get_or_create()
        .unwrap()
}

#[test]
fn map_panic_becomes_error() {
    let err = sc()
        .parallelize(vec![1, 2, 3])
        .map(|x| {
            if x == 2 {
                panic!("boom-map");
            }
            x
        })
        .collect()
        .unwrap_err();
    match err {
        Error::PartitionPanic { message, .. } => assert!(message.contains("boom-map")),
        other => panic!("expected PartitionPanic, got {other:?}"),
    }
}

#[test]
fn take_panic_becomes_error() {
    let err = sc()
        .parallelize(vec![1, 2, 3])
        .map(|x| {
            if x == 1 {
                panic!("boom-take");
            }
            x
        })
        .take(1)
        .unwrap_err();
    match err {
        Error::PartitionPanic { message, .. } => assert!(message.contains("boom-take")),
        other => panic!("expected PartitionPanic, got {other:?}"),
    }
}

#[test]
fn reduce_merge_panic_becomes_error() {
    let err = sc()
        .parallelize_partitions(vec![1, 2], 2)
        .unwrap()
        .reduce(|_, _| panic!("boom-merge"))
        .unwrap_err();
    match err {
        Error::Panic { message } => assert!(message.contains("boom-merge")),
        other => panic!("expected Panic, got {other:?}"),
    }
}

#[test]
fn take_skips_empty_partitions() {
    let got = sc()
        .parallelize_partitions(vec![1, 2, 3], 8)
        .unwrap()
        .take(2)
        .unwrap();
    assert_eq!(got, vec![1, 2]);
}

#[test]
fn take_beyond_len_returns_all() {
    let got = sc().parallelize(vec![1, 2]).take(100).unwrap();
    assert_eq!(got, vec![1, 2]);
}

#[test]
fn take_drop_panic_becomes_error() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    #[derive(Clone)]
    struct Boom {
        id: i32,
        live: Arc<AtomicBool>,
    }
    impl Drop for Boom {
        fn drop(&mut self) {
            if self.id == 99 && self.live.swap(false, Ordering::SeqCst) {
                panic!("boom-drop");
            }
        }
    }
    let live = Arc::new(AtomicBool::new(true));
    match sc()
        .parallelize_partitions(
            vec![
                Boom {
                    id: 1,
                    live: Arc::clone(&live),
                },
                Boom {
                    id: 99,
                    live: Arc::clone(&live),
                },
            ],
            1,
        )
        .unwrap()
        .take(1)
    {
        Err(Error::PartitionPanic { message, .. }) => assert!(message.contains("boom-drop")),
        Ok(_) => panic!("expected PartitionPanic"),
        Err(e) => panic!("expected PartitionPanic, got {e:?}"),
    }
}

#[test]
fn clone_without_t_clone() {
    struct Nc;
    let rdd = sc().parallelize(vec![1, 2, 3]).map(|_| Nc);
    assert_eq!(rdd.clone().count().unwrap(), 3);
    assert_eq!(rdd.count().unwrap(), 3);
}

#[test]
fn lineage_ids_and_narrow_deps() {
    let base = sc().parallelize(vec![1, 2, 3, 4]);
    let mapped = base.clone().map(|x| x + 1);
    assert_ne!(base.id(), mapped.id());
    assert_eq!(
        mapped.dependencies(),
        &[Dependency::Narrow { parent: base.id() }]
    );
    assert!(base.dependencies().is_empty());
    assert_eq!(mapped.get_num_partitions(), base.get_num_partitions());
}

#[test]
fn repeated_action_recomputes() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    let hits = Arc::new(AtomicUsize::new(0));
    let h = Arc::clone(&hits);
    let rdd = sc().parallelize(vec![1, 2, 3]).map(move |x| {
        h.fetch_add(1, Ordering::SeqCst);
        x
    });
    assert_eq!(rdd.count().unwrap(), 3);
    assert_eq!(rdd.count().unwrap(), 3);
    assert_eq!(hits.load(Ordering::SeqCst), 6);
}

#[test]
fn flat_map_order() {
    let got = sc()
        .parallelize(vec!["ab".to_string(), "c".to_string()])
        .flat_map(|s| s.chars().collect::<Vec<_>>())
        .collect()
        .unwrap();
    assert_eq!(got, vec!['a', 'b', 'c']);
}
