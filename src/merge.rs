use bytes::Bytes;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

struct HeapItem {
    key:      Bytes,
    value:    Bytes,
    priority: usize, // higher = newer source
    iter_idx: usize,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool { self.cmp(other) == Ordering::Equal }
}
impl Eq for HeapItem {}

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

pub struct MergeIter {
    iters: Vec<Box<dyn Iterator<Item = (Bytes, Bytes)>>>,
    heap:  BinaryHeap<HeapItem>,
}

impl MergeIter {
    /// `iters` ordered oldest → newest; index position becomes the priority.
    pub fn new(mut iters: Vec<Box<dyn Iterator<Item = (Bytes, Bytes)>>>) -> Self {
        let mut heap = BinaryHeap::new();
        for (idx, iter) in iters.iter_mut().enumerate() {
            if let Some((key, value)) = iter.next() {
                heap.push(HeapItem { key, value, priority: idx, iter_idx: idx });
            }
        }
        MergeIter { iters, heap }
    }
}

impl Iterator for MergeIter {
    type Item = (Bytes, Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let item = self.heap.pop()?;

            // Advance this source's iterator
            if let Some((key, value)) = self.iters[item.iter_idx].next() {
                self.heap.push(HeapItem {
                    key, value,
                    priority: item.priority,
                    iter_idx: item.iter_idx,
                });
            }

            // Drain lower-priority duplicates of the same key
            while self.heap.peek().is_some_and(|t| t.key == item.key) {
                let dup = self.heap.pop().unwrap();
                if let Some((key, value)) = self.iters[dup.iter_idx].next() {
                    self.heap.push(HeapItem {
                        key, value,
                        priority: dup.priority,
                        iter_idx: dup.iter_idx,
                    });
                }
            }

            // Skip tombstones
            if !item.value.is_empty() {
                return Some((item.key, item.value));
            }
        }
    }
}