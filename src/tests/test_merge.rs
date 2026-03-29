use bytes::Bytes;
use crate::merge::MergeIter;

fn b(s: &str) -> Bytes { Bytes::from(s.to_owned()) }

fn src(pairs: &[(&str, &str)]) -> Box<dyn Iterator<Item = (Bytes, Bytes)>> {
    let v: Vec<_> = pairs.iter().map(|(k, v)| (b(k), b(v))).collect();
    Box::new(v.into_iter())
}

#[test]
fn test_single_source() {
    let iter = MergeIter::new(vec![src(&[("a","1"),("b","2")])]);
    let res: Vec<_> = iter.collect();
    assert_eq!(res, vec![(b("a"), b("1")), (b("b"), b("2"))]);
}

#[test]
fn test_merges_sorted_order() {
    let iter = MergeIter::new(vec![
        src(&[("a","1"), ("c","3")]),
        src(&[("b","2"), ("d","4")]),
    ]);
    let keys: Vec<_> = iter.map(|(k, _)| k).collect();
    assert_eq!(keys, vec![b("a"), b("b"), b("c"), b("d")]);
}

#[test]
fn test_newer_source_wins_on_duplicate() {
    let iter = MergeIter::new(vec![
        src(&[("key", "old")]),
        src(&[("key", "new")]),
    ]);
    let res: Vec<_> = iter.collect();
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].1, b("new"));
}

#[test]
fn test_tombstone_is_skipped() {
    let iter = MergeIter::new(vec![
        src(&[("a","1"), ("b","")]),
    ]);
    let res: Vec<_> = iter.collect();
    assert_eq!(res, vec![(b("a"), b("1"))]);
}

#[test]
fn test_tombstone_hides_older_version() {
    let iter = MergeIter::new(vec![
        src(&[("key", "old")]),
        src(&[("key", "")]),
    ]);
    assert_eq!(iter.count(), 0);
}

#[test]
fn test_empty_sources() {
    let iter = MergeIter::new(vec![src(&[]), src(&[])]);
    assert_eq!(iter.count(), 0);
}

#[test]
fn test_three_sources_priority() {
    let iter = MergeIter::new(vec![
        src(&[("key", "oldest")]),
        src(&[("key", "middle")]),
        src(&[("key", "newest")]),
    ]);
    let res: Vec<_> = iter.collect();
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].1, b("newest"));
}