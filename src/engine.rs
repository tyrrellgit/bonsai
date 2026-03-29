use anyhow::Result;
use bytes::Bytes;
use std::path::{ Path, PathBuf };
use std::sync::Arc;
use std::ops::Bound;

use crate::memtable::MemTable;
use crate::sstable::SSTable;
use crate::merge::MergeIter;
use crate::wal::Wal;

const DEFAULT_MEMTABLE_SIZE_LIMIT: usize = 4 * 1024 * 1024;

pub struct Engine {
    memtable:            Arc<MemTable>,
    wal:                 Wal,
    /// (frozen memtable, path of its WAL) — oldest at index 0
    imm_memtables:       Vec<(Arc<MemTable>, PathBuf)>,
    sstables:            Vec<SSTable>,
    data_dir:            PathBuf,
    memtable_size_limit: usize,
    next_id:             usize,
}

impl Engine {
    /// Create a brand-new engine in an empty directory.
    pub fn new(data_dir: impl Into<PathBuf>) -> Result<Self> {
        let data_dir = data_dir.into();
        std::fs::create_dir_all(&data_dir)?;

        let id  = 0;
        let wal = Wal::new(&data_dir.join(format!("{:08}.wal", id)))?;

        Ok(Engine {
            memtable:            Arc::new(MemTable::new(id)),
            wal,
            imm_memtables:       Vec::new(),
            sstables:            Vec::new(),
            data_dir,
            memtable_size_limit: DEFAULT_MEMTABLE_SIZE_LIMIT,
            next_id:             1,
        })
    }

    /// Open an existing engine directory, replaying any WALs that survived.
    pub fn open(data_dir: impl Into<PathBuf>) -> Result<Self> {
        let data_dir = data_dir.into();
        std::fs::create_dir_all(&data_dir)?;

        // ── Restore SSTables ────────────────────────────────────────────────
        let mut sstables: Vec<SSTable> = std::fs::read_dir(&data_dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|x| x == "sst"))
            .map(|e| {
                let path = e.path();
                let id   = parse_id(&path);
                SSTable::open(id, path)
            })
            .collect::<Result<Vec<_>>>()?;
        sstables.sort_by_key(|s| s.id);

        // ── Replay WALs ─────────────────────────────────────────────────────
        let mut wal_paths: Vec<PathBuf> = std::fs::read_dir(&data_dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|x| x == "wal"))
            .map(|e| e.path())
            .collect();
        wal_paths.sort(); // lexicographic == ID order given zero-padded names

        let mut imm_memtables: Vec<(Arc<MemTable>, PathBuf)> = Vec::new();

        for path in &wal_paths {
            let id  = parse_id(path);
            let mem = Arc::new(MemTable::new(id));

            for entry in Wal::replay(path)? {
                match entry {
                    crate::wal::WalEntry::Put    { key, value } => mem.put(key, value),
                    crate::wal::WalEntry::Delete { key }        => mem.delete(key),
                }
            }
            imm_memtables.push((mem, path.clone()));
        }

        // The highest-ID WAL becomes the new active memtable + WAL
        let (memtable, wal) = if let Some((mem, path)) = imm_memtables.pop() {
            let wal = Wal::new(&path)?; // re-open in append mode
            (mem, wal)
        } else {
            // No WALs found — start fresh
            let id  = sstables.last().map_or(0, |s| s.id + 1);
            let wal = Wal::new(&data_dir.join(format!("{:08}.wal", id)))?;
            (Arc::new(MemTable::new(id)), wal)
        };

        let next_id = memtable.id + 1;

        Ok(Engine {
            memtable,
            wal,
            imm_memtables,
            sstables,
            data_dir,
            memtable_size_limit: DEFAULT_MEMTABLE_SIZE_LIMIT,
            next_id,
        })
    }

    // ── Writes ───────────────────────────────────────────────────────────────

    pub fn put(&mut self, key: Bytes, value: Bytes) -> Result<()> {
        // WAL first — if we crash after this but before memtable.put(), the
        // WAL replay on restart will re-apply the write. Safe.
        self.wal.put(&key, &value)?;
        self.memtable.put(key, value);
        self.maybe_freeze()
    }

    pub fn delete(&mut self, key: Bytes) -> Result<()> {
        self.wal.delete(&key)?;
        self.memtable.delete(key);
        self.maybe_freeze()
    }

    // ── Reads ────────────────────────────────────────────────────────────────

    pub fn get(&self, key: &Bytes) -> Result<Option<Bytes>> {
        if let Some(v) = self.memtable.get(key) {
            return Ok(if v.is_empty() { None } else { Some(v) });
        }
        for (imm, _) in self.imm_memtables.iter().rev() {
            if let Some(v) = imm.get(key) {
                return Ok(if v.is_empty() { None } else { Some(v) });
            }
        }
        for sst in self.sstables.iter().rev() {
            if let Some(v) = sst.get(key)? {
                return Ok(if v.is_empty() { None } else { Some(v) });
            }
        }
        Ok(None)
    }
    pub fn scan(&self, lower: Bound<Bytes>, upper: Bound<Bytes>) -> Result<MergeIter> {
        let mut iters: Vec<Box<dyn Iterator<Item = (Bytes, Bytes)>>> = Vec::new();

        // Oldest → newest so index == priority in MergeIter
        for sst in self.sstables.iter() {
            iters.push(Box::new(sst.scan(lower.clone(), upper.clone())?));
        }
        for (imm, _) in self.imm_memtables.iter() {
            iters.push(Box::new(imm.scan(lower.clone(), upper.clone()).into_iter()));
        }
        iters.push(Box::new(
            self.memtable.scan(lower, upper).into_iter()
        ));

        Ok(MergeIter::new(iters))
    }

    // ── Freeze & Flush ────────────────────────────────────────────────────────

    fn maybe_freeze(&mut self) -> Result<()> {
        if self.memtable.approximate_size() >= self.memtable_size_limit {
            self.freeze_memtable()?;
        }
        Ok(())
    }

    fn freeze_memtable(&mut self) -> Result<()> {
        // Freeze: current memtable + its WAL path go onto the immutable list
        let old_wal_path = self.wal.path.clone();
        let frozen       = Arc::clone(&self.memtable);
        self.imm_memtables.push((frozen, old_wal_path));

        // New active memtable + fresh WAL
        let new_id = self.next_id;
        self.next_id  += 1;
        self.memtable  = Arc::new(MemTable::new(new_id));
        self.wal       = Wal::new(&self.wal_path(new_id))?;

        self.flush_oldest_imm()
    }

    pub fn flush_oldest_imm(&mut self) -> Result<()> {
        if self.imm_memtables.is_empty() { return Ok(()); }

        let (imm, wal_path) = self.imm_memtables.remove(0);
        let sst_path        = self.sst_path(imm.id);

        let sst = SSTable::from_memtable(&imm, &sst_path)?;
        self.sstables.push(sst);

        // SSTable is safely on disk — WAL is no longer needed
        std::fs::remove_file(&wal_path)?;
        Ok(())
    }

    fn sst_path(&self, id: usize) -> PathBuf {
        self.data_dir.join(format!("{:08}.sst", id))
    }

    fn wal_path(&self, id: usize) -> PathBuf {
        self.data_dir.join(format!("{:08}.wal", id))
    }
}

fn parse_id(path: &Path) -> usize {
    path.file_stem()
        .and_then(|s| s.to_str())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}