use std::io::Write;

use spatter::prelude::*;

#[test]
fn read_text_file_splits_lines() {
    let dir = std::env::temp_dir();
    let path = dir.join("spatter-textfile-test.txt");
    {
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(f, "alpha").unwrap();
        writeln!(f, "beta gamma").unwrap();
        writeln!(f, "delta").unwrap();
    }
    let sc = SpatterContext::builder()
        .master("local[2]")
        .get_or_create()
        .unwrap();
    let mut lines = sc.read_text_file(&path).unwrap().collect().unwrap();
    lines.sort();
    assert_eq!(
        lines,
        vec![
            "alpha".to_string(),
            "beta gamma".to_string(),
            "delta".to_string()
        ]
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn read_text_file_many_partitions() {
    let path = std::env::temp_dir().join("spatter-textfile-many.txt");
    std::fs::write(&path, "the cat sat\nthe cat ran\n").unwrap();
    let sc = SpatterContext::builder()
        .master("local[8]")
        .get_or_create()
        .unwrap();
    let mut lines = sc
        .read_text_file_partitions(&path, 16)
        .unwrap()
        .collect()
        .unwrap();
    lines.sort();
    assert_eq!(
        lines,
        vec!["the cat ran".to_string(), "the cat sat".to_string()]
    );
    let _ = std::fs::remove_file(path);
}
