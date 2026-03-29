use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use std::ops::Bound;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub struct MemTable {
    pub map:  Arc<SkipMap<Bytes, Bytes>>,
    pub id:   usize,
    size:     Arc<AtomicUsize>,
}

impl MemTable {
    pub fn new(id: usize) -> Self {
        MemTable {
            map:  Arc::new(SkipMap::new()),
            id,
            size: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn put(&self, key: Bytes, value: Bytes) {
        self.size.fetch_add(key.len() + value.len(), Ordering::Relaxed);
        self.map.insert(key, value);
    }

    pub fn delete(&self, key: Bytes) {
        self.size.fetch_add(key.len(), Ordering::Relaxed);
        self.map.insert(key, Bytes::new());
    }

    pub fn get(&self, key: &Bytes) -> Option<Bytes> {
        self.map.get(key).map(|e| e.value().clone())
    }

    pub fn scan(&self, lower: Bound<Bytes>, upper: Bound<Bytes>) -> Vec<(Bytes, Bytes)> {
        self.map
            .range((lower, upper))
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect()
    }

    pub fn approximate_size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    pub fn entry_count(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (Bytes, Bytes)> + '_ {
        self.map.iter().map(|e| (e.key().clone(), e.value().clone()))
    }
}