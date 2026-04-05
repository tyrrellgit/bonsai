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

/// Configuration for leveled compaction.
#[derive(Clone)]
pub struct CompactionConfig {
    pub max_l0_files: usize,
    pub level_multiplier: usize,
    pub num_levels: usize,
}

impl CompactionConfig {
    pub fn max_level_files(&self, level: usize) -> usize {
        self.max_l0_files * self.level_multiplier.pow(level as u32)
    }
}

/// Default compaction config
impl Default for CompactionConfig {
    fn default() -> Self {
        CompactionConfig {
            max_l0_files: 4,
            level_multiplier: 10,
            num_levels: 7,
        }
    }
}

pub struct Engine {
    memtable:            Arc<MemTable>,
    wal:                 Wal,
    imm_memtables:       Vec<(Arc<MemTable>, PathBuf)>,
    sstables_by_level:   Vec<Vec<SSTable>>,
    data_dir:            PathBuf,
    memtable_size_limit: usize,
    next_id:             usize,
    compaction_config:   CompactionConfig,
}

impl Engine {
    /// Create a brand-new engine in an empty directory with default compaction config.
    pub fn new(data_dir: impl Into<PathBuf>) -> Result<Self> {
        Self::new_with_config(data_dir, CompactionConfig::default())
    }

    /// Create a brand-new engine with custom compaction configuration.
    pub fn new_with_config(data_dir: impl Into<PathBuf>, config: CompactionConfig) -> Result<Self> {
        let data_dir = data_dir.into();
        std::fs::create_dir_all(&data_dir)?;

        let id  = 0;
        let wal = Wal::new(&data_dir.join(format!("{:08}.wal", id)))?;

        Ok(Engine {
            memtable:            Arc::new(MemTable::new(id)),
            wal,
            imm_memtables:       Vec::new(),
            sstables_by_level:   (0..config.num_levels).map(|_| Vec::new()).collect(),
            data_dir,
            memtable_size_limit: DEFAULT_MEMTABLE_SIZE_LIMIT,
            next_id:             1,
            compaction_config:   config,
        })
    }

    /// Open an existing engine directory, replaying any WALs that survived.
    pub fn open(data_dir: impl Into<PathBuf>) -> Result<Self> {
        Self::open_with_config(data_dir, CompactionConfig::default())
    }

    /// Open an existing engine with custom compaction configuration.
    pub fn open_with_config(data_dir: impl Into<PathBuf>, config: CompactionConfig) -> Result<Self> {
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

        // All recovered SSTables start in level 0
        let mut sstables_by_level: Vec<Vec<SSTable>> = (0..config.num_levels).map(|_| Vec::new()).collect();
        sstables_by_level[0] = sstables;

        Ok(Engine {
            memtable,
            wal,
            imm_memtables,
            sstables_by_level,
            data_dir,
            memtable_size_limit: DEFAULT_MEMTABLE_SIZE_LIMIT,
            next_id,
            compaction_config:   config,
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
        // Search SSTables from newest to oldest, across all levels
        for level in (0..self.sstables_by_level.len()).rev() {
            for sst in self.sstables_by_level[level].iter().rev() {
                if let Some(v) = sst.get(key)? {
                    return Ok(if v.is_empty() { None } else { Some(v) });
                }
            }
        }
        Ok(None)
    }
    pub fn scan(&self, lower: Bound<Bytes>, upper: Bound<Bytes>) -> Result<MergeIter> {
        let mut iters: Vec<Box<dyn Iterator<Item = (Bytes, Bytes)>>> = Vec::new();

        // Add SSTables from all levels, oldest → newest so index == priority in MergeIter
        for level in 0..self.sstables_by_level.len() {
            for sst in self.sstables_by_level[level].iter() {
                iters.push(Box::new(sst.scan(lower.clone(), upper.clone())?));
            }
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

    pub fn freeze_memtable(&mut self) -> Result<()> {
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
        self.sstables_by_level[0].push(sst);

        // Ensure level 0 stays sorted by ID for consistency
        self.sstables_by_level[0].sort_by_key(|s| s.id);

        // SSTable is safely on disk — WAL is no longer needed
        std::fs::remove_file(&wal_path)?;

        // Check for compaction
        self.trigger_compaction()?;
        Ok(())
    }

    fn sst_path(&self, id: usize) -> PathBuf {
        self.data_dir.join(format!("{:08}.sst", id))
    }

    fn wal_path(&self, id: usize) -> PathBuf {
        self.data_dir.join(format!("{:08}.wal", id))
    }

    // ── Compaction ────────────────────────────────────────────────────────────

    /// Check if compaction is needed and perform it with cascading through levels.
    fn trigger_compaction(&mut self) -> Result<()> {
        // Compaction loop: compact L0 if needed, then cascade through levels
        loop {
            let should_compact = self.sstables_by_level[0].len() >= self.compaction_config.max_l0_files;
            
            if should_compact {
                // Compact L0 with L1
                self.compact_level(0)?;
            } else {
                // Check if any other level needs compaction
                let mut found_level = None;
                for level in 1..self.compaction_config.num_levels - 1 {
                    let level_size = self.sstables_by_level[level].len();
                    let max_files_at_level = self.compaction_config.max_level_files(level);
                    if level_size > max_files_at_level {
                        found_level = Some(level);
                        break;
                    }
                }
                
                if let Some(level) = found_level {
                    self.compact_level(level)?;
                } else {
                    break;
                }
            }
        }
        Ok(())
    }

    /// Compact a specific level with the next level.
    /// Merges all SSTables from `level` and `level+1` into `level+1`.
    fn compact_level(&mut self, level: usize) -> Result<()> {
        // Can't compact beyond the last level
        if level >= self.compaction_config.num_levels - 1 {
            return Ok(());
        }

        let next_level = level + 1;
        
        // Nothing to do if both levels are empty
        if self.sstables_by_level[level].is_empty() && self.sstables_by_level[next_level].is_empty() {
            return Ok(());
        }

        // Collect iterators: level SSTables first (oldest), then next_level (newer)
        let mut iters: Vec<Box<dyn Iterator<Item = (Bytes, Bytes)>>> = Vec::new();
        let mut old_paths: Vec<PathBuf> = Vec::new();

        // Add all iterators from current level
        for sst in self.sstables_by_level[level].iter() {
            old_paths.push(sst.path.clone());
            iters.push(Box::new(sst.scan(Bound::Unbounded, Bound::Unbounded)?));
        }

        // Add all iterators from next level
        for sst in self.sstables_by_level[next_level].iter() {
            old_paths.push(sst.path.clone());
            iters.push(Box::new(sst.scan(Bound::Unbounded, Bound::Unbounded)?));
        }

        // Merge all input SSTables
        let merge_iter = MergeIter::new(iters);

        // Group merged data into output SSTables for next_level
        // For now, write everything to a single SSTable (can be improved with splitting)
        let temp_mem = MemTable::new(self.next_id);
        self.next_id += 1;

        for (key, value) in merge_iter {
            if value.is_empty() {
                temp_mem.delete(key);
            } else {
                temp_mem.put(key, value);
            }
        }

        // Write merged result as a new SSTable
        let output_sst_path = self.sst_path(temp_mem.id);
        let output_sst = SSTable::from_memtable(&temp_mem, &output_sst_path)?;

        // Clear the current level and replace next level with the merged output
        self.sstables_by_level[level].clear();
        self.sstables_by_level[next_level] = vec![output_sst];

        // Clean up old files
        for path in old_paths {
            let _ = std::fs::remove_file(path);
        }

        Ok(())
    }

    /// Get the compaction configuration.
    pub fn compaction_config(&self) -> &CompactionConfig {
        &self.compaction_config
    }

    /// Get count of L0 SSTables (for testing/metrics).
    pub fn l0_count(&self) -> usize {
        self.sstables_by_level.get(0).map_or(0, |v| v.len())
    }

    /// Get count of all SSTables at a given level (for testing/metrics).
    pub fn level_count(&self, level: usize) -> usize {
        self.sstables_by_level.get(level).map_or(0, |v| v.len())
    }

    /// Get total number of levels.
    pub fn num_levels(&self) -> usize {
        self.sstables_by_level.len()
    }

    /// Get total size of all SSTables across all levels (for testing/metrics).
    pub fn total_sst_count(&self) -> usize {
        self.sstables_by_level.iter().map(|level| level.len()).sum()
    }
}

fn parse_id(path: &Path) -> usize {
    path.file_stem()
        .and_then(|s| s.to_str())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}