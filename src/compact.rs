pub mod leveled;

use anyhow::Result;
use bytes::Bytes;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};

use crate::merge::MergeIter;
use crate::sstable::{ SSTable, SSTFooter };

/// Snapshot of all levels passed to the strategy for decision-making.
/// levels[0] = L0, levels[1] = L1, etc.
pub(crate) struct LSMState {
    pub levels: Vec<Vec<SSTFooter>>,
}

impl LSMState {
    pub fn from_levels(sstables_by_level: &[Vec<SSTable>]) -> Self {
        LSMState {
            levels: sstables_by_level
                .iter()
                .map(|level| level.iter().map(SSTFooter::from_table).collect())
                .collect(),
        }
    }
}

// ── Task & result ─────────────────────────────────────────────────────────────

pub struct CompactionTask {
    /// None = L0, Some(n) = Ln
    pub upper_level:   Option<usize>,
    pub upper_sst_ids: Vec<usize>,
    pub lower_level:   usize,
    pub lower_sst_ids: Vec<usize>,
}

pub struct CompactionResult {
    pub task:    CompactionTask,
    pub outputs: Vec<SSTable>,
}

// ── Strategy trait ────────────────────────────────────────────────────────────

/// Pure decision logic — no I/O, no thread management.
/// Implement this to swap out compaction behaviour.
pub(crate) trait CompactionStrategy: Send + 'static {
    fn pick_compaction(&self, state: &LSMState) -> Option<CompactionTask>;
}

// ── Internal worker types ─────────────────────────────────────────────────────

struct CompactionJob {
    task:        CompactionTask,
    inputs:      Vec<(usize, PathBuf)>, // (sst_id, path)
    output_id:   usize,
    output_path: PathBuf,
    size_hint:   usize,
}

// ── Compactor ─────────────────────────────────────────────────────────────────
// Owns the background thread. Engine calls maybe_schedule() and try_recv().

pub struct Compactor {
    strategy:   Box<dyn CompactionStrategy>,
    job_tx:     Option<Sender<CompactionJob>>,
    result_rx:  Receiver<CompactionResult>,
    thread:     Option<JoinHandle<()>>,
    in_flight:  bool,
}

impl Compactor {
    pub fn new(strategy: impl CompactionStrategy) -> Self {
        let (job_tx, job_rx)   = mpsc::channel::<CompactionJob>();
        let (done_tx, done_rx) = mpsc::channel::<CompactionResult>();

        let thread = thread::spawn(move || {
            for job in job_rx {
                match do_compaction(job) {
                    Ok(result) => { if done_tx.send(result).is_err() { break; } }
                    Err(e)     => eprintln!("[compaction worker] {e}"),
                }
            }
        });

        Compactor {
            strategy:  Box::new(strategy),
            job_tx:    Some(job_tx),
            result_rx: done_rx,
            thread:    Some(thread),
            in_flight: false,
        }
    }

    /// Non-blocking: pick a compaction task from the current state and dispatch
    /// it to the background worker if none is already in flight.
    pub fn maybe_schedule(
        &mut self,
        sstables: &[Vec<SSTable>],
        next_id:  &mut usize,
        data_dir: &Path,
    ) {
        if self.in_flight { return; }

        let state = LSMState::from_levels(sstables);
        let Some(task) = self.strategy.pick_compaction(&state) else { return };

        self.dispatch(task, sstables, next_id, data_dir);
    }

    fn dispatch(
        &mut self,
        task:     CompactionTask,
        sstables: &[Vec<SSTable>],
        next_id:  &mut usize,
        data_dir: &Path,
    ) {
        let upper_idx = task.upper_level.unwrap_or(0);
        let mut inputs: Vec<(usize, PathBuf)> = Vec::new();
        let mut size_hint = 0usize;

        for id in &task.upper_sst_ids {
            if let Some(s) = sstables[upper_idx].iter().find(|s| s.id == *id) {
                inputs.push((s.id, s.path.clone()));
                size_hint += (s.data_len as usize).saturating_div(32);
            }
        }
        for id in &task.lower_sst_ids {
            if let Some(s) = sstables[task.lower_level].iter().find(|s| s.id == *id) {
                inputs.push((s.id, s.path.clone()));
                size_hint += (s.data_len as usize).saturating_div(32);
            }
        }

        if inputs.is_empty() { return; }

        let output_id   = *next_id;
        *next_id       += 1;
        let output_path = data_dir.join(format!("{:08}.sst", output_id));

        if let Some(tx) = &self.job_tx
            && tx.send(CompactionJob { task, inputs, output_id, output_path, size_hint }).is_ok() {
                self.in_flight = true;
        }
    }

    /// Non-blocking: returns a completed result if one is ready.
    pub fn try_recv(&mut self) -> Option<CompactionResult> {
        match self.result_rx.try_recv() {
            Ok(r)  => { self.in_flight = false; Some(r) }
            Err(_) => None,
        }
    }
}

impl Drop for Compactor {
    fn drop(&mut self) {
        drop(self.job_tx.take());
        if let Some(t) = self.thread.take() { let _ = t.join(); }
    }
}

// ── Worker function ───────────────────────────────────────────────────────────

fn do_compaction(job: CompactionJob) -> Result<CompactionResult> {
    let mut iters: Vec<Box<dyn Iterator<Item = (Bytes, Bytes)>>> = Vec::new();

    for (id, path) in &job.inputs {
        let sst = SSTable::open(*id, path.clone())?;
        iters.push(Box::new(sst.scan(Bound::Unbounded, Bound::Unbounded)?));
    }

    // Stream directly from merge iterator to disk — no intermediate MemTable.
    let output = SSTable::from_iter(
        job.output_id,
        &job.output_path,
        MergeIter::new(iters),
        job.size_hint,
    )?;

    Ok(CompactionResult { task: job.task, outputs: vec![output] })
}