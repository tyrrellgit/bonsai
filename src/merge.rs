use bytes::Bytes;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

#[derive(Eq)]
pub(crate) struct HeapItem {
    key:      Bytes,
    value:    Bytes,
    priority: usize, // higher = newer source
    iter_idx: usize,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool { self.cmp(other) == Ordering::Equal }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap; reverse key order makes it a min-heap.
        // On key ties, higher priority (newer source) wins.
        other.key.cmp(&self.key)
            .then(self.priority.cmp(&other.priority))
    }
}
impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) }
}

pub(crate) enum MergeIter {
    Empty,
    Single {
        iter: Box<dyn Iterator<Item = (Bytes, Bytes)>>,
    },
    Multi {
        iters: Vec<Box<dyn Iterator<Item = (Bytes, Bytes)>>>,
        heap:  BinaryHeap<HeapItem>,
    },
}

impl MergeIter {
    pub fn new(mut iters: Vec<Box<dyn Iterator<Item = (Bytes, Bytes)>>>) -> Self {
        match iters.len() {
            0 => MergeIter::Empty,
            1 => MergeIter::Single{ iter: iters.remove(0) },
            _ => {
                let mut heap = BinaryHeap::new();
                for (idx, iter) in iters.iter_mut().enumerate() {
                    if let Some((key, value)) = iter.next() {
                        heap.push(HeapItem { key, value, priority: idx, iter_idx: idx });
                    }
                }
                MergeIter::Multi { iters, heap }
            }
        }
    }
}

impl Iterator for MergeIter {
    type Item = (Bytes, Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            MergeIter::Empty => None,
            MergeIter::Single { iter } => {
                // skip tombstones
                iter.find(|(_, v)| !v.is_empty())
            }
            MergeIter::Multi { iters, heap } => {
                loop {
                    let item = heap.pop()?;

                    if let Some((key, value)) = iters[item.iter_idx].next() {
                        heap.push(HeapItem {
                            key, value,
                            priority: item.priority,
                            iter_idx: item.iter_idx,
                        });
                    }

                    while heap.peek().is_some_and(|t| t.key == item.key) {
                        let dup = heap.pop().unwrap();
                        if let Some((key, value)) = iters[dup.iter_idx].next() {
                            heap.push(HeapItem {
                                key, value,
                                priority: dup.priority,
                                iter_idx: dup.iter_idx,
                            });
                        }
                    }

                    if !item.value.is_empty() {
                        return Some((item.key, item.value));
                    }
                }
            }
        }
    }
}