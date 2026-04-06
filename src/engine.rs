use anyhow::Result;
use bytes::Bytes;
use std::collections::HashSet;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};

use crate::compact::{Compactor, CompactionResult};
use crate::compact::leveled::LeveledCompactionStrategy;
use crate::memtable::MemTable;
use crate::merge::MergeIter;
use crate::sstable::SSTable;
use crate::wal::Wal;

/// Defaults for Flushing and Compaction
pub(crate) const DEFAULT_MEMTABLE_SIZE_LIMIT: usize = 4 * 1024 * 1024;
pub(crate) const DEFAULT_MAX_L0_SIZE: usize = 200 * 1024 * 1024;
pub(crate) const DEFAULT_MAX_L0_FILES: usize = 4;
pub(crate) const DEFAULT_LEVEL_MULTIPLIER: usize = 10;
pub(crate) const DEFAULT_NUM_LEVELS: usize = 7;

/// Configuration for compaction.
#[derive(Clone, Debug)]
pub struct CompactionConfig {
    pub memtable_size_limit: usize,
    pub max_l0_files:        usize,
    pub max_l0_size:         usize,
    pub level_multiplier:    usize,
    pub num_levels:          usize,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        CompactionConfig {
            memtable_size_limit: DEFAULT_MEMTABLE_SIZE_LIMIT,
            max_l0_files:        DEFAULT_MAX_L0_FILES,
            max_l0_size:         DEFAULT_MAX_L0_SIZE,
            level_multiplier:    DEFAULT_LEVEL_MULTIPLIER,
            num_levels:          DEFAULT_NUM_LEVELS,
        }
    }
}

impl CompactionConfig {
    pub fn max_level_files(&self, level: usize) -> usize {
        self.max_l0_files * self.level_multiplier.pow(level as u32)
    }
}


// ── Flush worker ──────────────────────────────────────────────────────────────

struct FlushJob {
    mem:      Arc<MemTable>,
    wal_path: PathBuf,
    sst_path: PathBuf,
}


// ── Engine ────────────────────────────────────────────────────────────────────

pub struct Engine {
    memtable:            Arc<MemTable>,
    wal:                 Wal,
    imm_memtables:       Vec<(Arc<MemTable>, PathBuf)>,
    sstables_by_level:   Vec<Vec<SSTable>>,
    data_dir:            PathBuf,
    memtable_size_limit: usize,
    next_id:             usize,
    compaction_config:   CompactionConfig,
    flush_tx:            Option<Sender<FlushJob>>,
    flush_rx:            Receiver<SSTable>,
    flush_thread:        Option<JoinHandle<()>>,
    compactor:           Compactor,
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

        let (flush_tx, flush_rx, flush_thread) = Self::spawn_flush_worker();
        let compactor = Compactor::new(LeveledCompactionStrategy::new(config.clone()));

        Ok(Engine {
            memtable:            Arc::new(MemTable::new(id)),
            wal,
            imm_memtables:       Vec::new(),
            sstables_by_level:   (0..config.num_levels).map(|_| Vec::new()).collect(),
            data_dir,
            memtable_size_limit: config.memtable_size_limit,
            next_id:             1,
            compaction_config:   config,
            flush_tx:            Some(flush_tx),
            flush_rx,
            flush_thread:        Some(flush_thread),
            compactor,
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

        // ── Restore SSTables ─────────────────────────────────────────────────
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

        // ── Replay WALs ──────────────────────────────────────────────────────
        let mut wal_paths: Vec<PathBuf> = std::fs::read_dir(&data_dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|x| x == "wal"))
            .map(|e| e.path())
            .collect();
        wal_paths.sort();

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
            let wal = Wal::new(&path)?;
            (mem, wal)
        } else {
            let id  = sstables.last().map_or(0, |s| s.id + 1);
            let wal = Wal::new(&data_dir.join(format!("{:08}.wal", id)))?;
            (Arc::new(MemTable::new(id)), wal)
        };

        let next_id = memtable.id + 1;

        let mut sstables_by_level: Vec<Vec<SSTable>> =
            (0..config.num_levels).map(|_| Vec::new()).collect();
        sstables_by_level[0] = sstables;

        let (flush_tx, flush_rx, flush_thread) = Self::spawn_flush_worker();
        let compactor = Compactor::new(LeveledCompactionStrategy::new(config.clone()));

        let engine = Engine {
            memtable,
            wal,
            imm_memtables,
            sstables_by_level,
            data_dir,
            memtable_size_limit: config.memtable_size_limit,
            next_id,
            compaction_config:   config,
            flush_tx:            Some(flush_tx),
            flush_rx,
            flush_thread:        Some(flush_thread),
            compactor,
        };

        // Re-dispatch flush jobs for imm memtables recovered from WALs
        for (mem, wal_path) in &engine.imm_memtables {
            let sst_path = engine.sst_path(mem.id);
            engine.send_flush_job(Arc::clone(mem), wal_path.clone(), sst_path);
        }

        Ok(engine)
    }


    // ── Writes ───────────────────────────────────────────────────────────────

    pub fn put(&mut self, key: Bytes, value: Bytes) -> Result<()> {
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
        for level in (0..self.sstables_by_level.len()).rev() {
            for sst in self.sstables_by_level[level].iter().rev() {
                if let Some(v) = sst.get(key)? {
                    return Ok(if v.is_empty() { None } else { Some(v) });
                }
            }
        }
        Ok(None)
    }

    pub fn scan(&self, lower: Bound<Bytes>, upper: Bound<Bytes>)
        -> Result<impl Iterator<Item = (Bytes, Bytes)>>
    {
        let mut iters: Vec<Box<dyn Iterator<Item = (Bytes, Bytes)>>> = Vec::new();

        for level in (0..self.sstables_by_level.len()).rev() {
            for sst in self.sstables_by_level[level].iter() {
                if self.sst_overlaps_range(sst, &lower, &upper) {
                    iters.push(Box::new(sst.scan(lower.clone(), upper.clone())?));
                }
            }
        }
        for (imm, _) in self.imm_memtables.iter() {
            iters.push(Box::new(imm.scan(lower.clone(), upper.clone()).into_iter()));
        }
        iters.push(Box::new(self.memtable.scan(lower, upper).into_iter()));

        Ok(MergeIter::new(iters))
    }

    fn sst_overlaps_range(&self, sst: &SSTable, lower: &Bound<Bytes>, upper: &Bound<Bytes>) -> bool {
        range_overlaps(&sst.first_key, &sst.last_key, lower, upper)
    }


    // ── Freeze & Flush ────────────────────────────────────────────────────────

    fn maybe_freeze(&mut self) -> Result<()> {
        self.drain_completed_flushes()?;
        self.drain_completed_compactions()?;
        if self.memtable.approximate_size() >= self.memtable_size_limit {
            self.freeze_memtable()?;
        }
        Ok(())
    }

    pub fn freeze_memtable(&mut self) -> Result<()> {
        let old_wal_path = self.wal.path.clone();
        let frozen       = Arc::clone(&self.memtable);
        self.imm_memtables.push((Arc::clone(&frozen), old_wal_path.clone()));

        let new_id    = self.next_id;
        self.next_id += 1;
        self.memtable = Arc::new(MemTable::new(new_id));
        self.wal      = Wal::new(&self.wal_path(new_id))?;

        let sst_path = self.sst_path(frozen.id);
        self.send_flush_job(frozen, old_wal_path, sst_path);

        Ok(())
    }

    pub fn flush_oldest_imm(&mut self) -> Result<()> {
        self.drain_completed_flushes()
    }


    // ── Background workers ────────────────────────────────────────────────────

    fn spawn_flush_worker() -> (Sender<FlushJob>, Receiver<SSTable>, JoinHandle<()>) {
        let (job_tx, job_rx)   = mpsc::channel::<FlushJob>();
        let (done_tx, done_rx) = mpsc::channel::<SSTable>();
        let handle = thread::spawn(move || {
            for job in job_rx {
                match SSTable::from_memtable(&job.mem, &job.sst_path) {
                    Ok(sst) => {
                        let _ = std::fs::remove_file(&job.wal_path);
                        if done_tx.send(sst).is_err() { break; }
                    }
                    Err(e) => eprintln!("[flush worker] {e}"),
                }
            }
        });
        (job_tx, done_rx, handle)
    }

    fn send_flush_job(&self, mem: Arc<MemTable>, wal_path: PathBuf, sst_path: PathBuf) {
        if let Some(tx) = &self.flush_tx {
            let _ = tx.send(FlushJob { mem, wal_path, sst_path });
        }
    }

    pub(crate) fn drain_completed_flushes(&mut self) -> Result<()> {
        while let Ok(sst) = self.flush_rx.try_recv() {
            self.imm_memtables.remove(0);
            let pos = self.sstables_by_level[0].partition_point(|s| s.id < sst.id);
            self.sstables_by_level[0].insert(pos, sst);
            self.trigger_compaction()?;
        }
        Ok(())
    }

    pub(crate) fn drain_completed_compactions(&mut self) -> Result<()> {
        while let Some(result) = self.compactor.try_recv() {
            self.apply_compaction_result(result);
            self.trigger_compaction()?;
        }
        Ok(())
    }

    // ── Compaction ────────────────────────────────────────────────────────────

    fn trigger_compaction(&mut self) -> Result<()> {
        self.compactor.maybe_schedule(
            &self.sstables_by_level,
            &mut self.next_id,
            &self.data_dir,
        );
        Ok(())
    }

    fn apply_compaction_result(&mut self, result: CompactionResult) {
        let upper_idx = result.task.upper_level.unwrap_or(0);
        let lower_idx = result.task.lower_level;

        let upper_ids: HashSet<usize> = result.task.upper_sst_ids.iter().copied().collect();
        let lower_ids: HashSet<usize> = result.task.lower_sst_ids.iter().copied().collect();

        let mut to_delete: Vec<PathBuf> = self.sstables_by_level[upper_idx]
            .iter()
            .filter(|s| upper_ids.contains(&s.id))
            .map(|s| s.path.clone())
            .collect();
        to_delete.extend(
            self.sstables_by_level[lower_idx]
                .iter()
                .filter(|s| lower_ids.contains(&s.id))
                .map(|s| s.path.clone()),
        );

        self.sstables_by_level[upper_idx].retain(|s| !upper_ids.contains(&s.id));
        self.sstables_by_level[lower_idx].retain(|s| !lower_ids.contains(&s.id));

        for sst in result.outputs {
            let pos = self.sstables_by_level[lower_idx]
                .partition_point(|s| s.first_key < sst.first_key);
            self.sstables_by_level[lower_idx].insert(pos, sst);
        }

        for path in to_delete { let _ = std::fs::remove_file(path); }
    }


    // ── Helpers ───────────────────────────────────────────────────────────────

    fn sst_path(&self, id: usize) -> PathBuf { self.data_dir.join(format!("{:08}.sst", id)) }
    fn wal_path(&self, id: usize) -> PathBuf { self.data_dir.join(format!("{:08}.wal", id)) }

    pub fn compaction_config(&self) -> &CompactionConfig { &self.compaction_config }
    pub fn l0_count(&self)          -> usize { self.sstables_by_level.first().map_or(0, |v| v.len()) }
    pub fn level_count(&self, level: usize) -> usize { self.sstables_by_level.get(level).map_or(0, |v| v.len()) }
    pub fn num_levels(&self)        -> usize { self.sstables_by_level.len() }
    pub fn total_sst_count(&self)   -> usize { self.sstables_by_level.iter().map(|l| l.len()).sum() }
}


// ── Shutdown ──────────────────────────────────────────────────────────────────

impl Drop for Engine {
    fn drop(&mut self) {
        drop(self.flush_tx.take());
        if let Some(t) = self.flush_thread.take() { let _ = t.join(); }
        // Compactor's own Drop handles its thread
    }
}


// ── Free functions ────────────────────────────────────────────────────────────

fn parse_id(path: &Path) -> usize {
    path.file_stem()
        .and_then(|s| s.to_str())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

fn range_overlaps(
    first_key: &Bytes,
    last_key:  &Bytes,
    lower:     &Bound<Bytes>,
    upper:     &Bound<Bytes>,
) -> bool {
    let past_upper = match upper {
        Bound::Included(u) => first_key > u,
        Bound::Excluded(u) => first_key >= u,
        Bound::Unbounded   => false,
    };
    let before_lower = match lower {
        Bound::Included(l) => last_key < l,
        Bound::Excluded(l) => last_key <= l,
        Bound::Unbounded   => false,
    };
    !past_upper && !before_lower
}