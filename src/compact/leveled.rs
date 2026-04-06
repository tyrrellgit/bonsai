use crate::engine::CompactionConfig;
use super::{CompactionStrategy, CompactionTask, LSMState, SSTFooter};


pub struct LeveledCompactionStrategy {
    pub config: CompactionConfig,
}

impl LeveledCompactionStrategy {
    pub fn new(config: CompactionConfig) -> Self {
        LeveledCompactionStrategy { config }
    }

    /// Compute target sizes for all levels.
    fn compute_target_sizes(&self, state: &LSMState) -> Vec<u64> {
        let n = state.levels.len();
        let mut targets = vec![0u64; n];
        if n < 2 { return targets; }

        let base_bytes = self.config.max_l0_size as u64;
        let last = n - 1;

        let last_actual: u64 = state.levels[last].iter().map(|s| s.data_len).sum();
        targets[last] = last_actual.max(base_bytes);

        for i in (1..last).rev() {
            let t = targets[i + 1] / self.config.level_multiplier as u64;
            if t == 0 { break; }
            targets[i] = t;
        }

        targets
    }

    /// The base level is the first non-L0 level with a positive target size.
    /// L0 compacts directly here, skipping empty intermediate levels — avoids
    /// unnecessary write amplification when the engine is small/empty.
    fn base_level(targets: &[u64]) -> usize {
        targets.iter()
            .enumerate()
            .skip(1) // skip L0
            .find(|&(_, t)| *t > 0)
            .map(|(i, _)| i)
            .unwrap_or(targets.len() - 1)
    }

    fn level_actual_size(state: &LSMState, level: usize) -> u64 {
        state.levels.get(level)
            .map(|l| l.iter().map(|s| s.data_len).sum())
            .unwrap_or(0)
    }

    /// Oldest SST = lowest id. Compacting oldest-first gives the most uniform
    /// key distribution across output SSTs over time.
    fn oldest_sst(level: &[SSTFooter]) -> Option<&SSTFooter> {
        level.iter().min_by_key(|s| s.id)
    }

    /// All SSTs in `candidates` whose key range overlaps [first_key, last_key].
    fn overlapping_ids(candidates: &[SSTFooter], first_key: &Vec<u8>, last_key: &Vec<u8>) -> Vec<usize> {
        candidates.iter()
            .filter(|s| !(s.last_key < *first_key || s.first_key > *last_key))
            .map(|s| s.id)
            .collect()
    }
}

impl CompactionStrategy for LeveledCompactionStrategy {
    fn pick_compaction(&self, state: &LSMState) -> Option<CompactionTask> {
        let targets = self.compute_target_sizes(state);
        let base    = Self::base_level(&targets);

        // ── L0 compaction — highest priority ─────────────────────────────────
        if state.levels[0].len() >= self.config.max_l0_files {
            let upper_sst_ids: Vec<usize> = state.levels[0].iter().map(|s| s.id).collect();

            // Key range spans the union of all L0 SST ranges
            let first_key = state.levels[0].iter().map(|s| &s.first_key).min()?.clone();
            let last_key  = state.levels[0].iter().map(|s| &s.last_key).max()?.clone();

            let lower_sst_ids = Self::overlapping_ids(&state.levels[base], &first_key, &last_key);

            return Some(CompactionTask {
                upper_level:   None, // L0
                upper_sst_ids,
                lower_level:   base,
                lower_sst_ids,
            });
        }

        // ── Non-L0 compaction — pick level with highest actual/target ratio > 1 ──
        let mut best_level: Option<usize> = None;
        let mut best_ratio = 1.0f64;

        // Exclude the last level — there's no level below it to compact into
        for (level, &target) in targets.iter().enumerate()
        .take(state.levels.len().saturating_sub(1)).skip(1) {
            if target == 0 { continue; }
            let actual = Self::level_actual_size(state, level);
            let ratio  = actual as f64 / target as f64;
            if ratio > best_ratio {
                best_ratio = ratio;
                best_level = Some(level);
            }
        }

        let level     = best_level?;
        let upper_sst = Self::oldest_sst(&state.levels[level])?;

        let lower_sst_ids = Self::overlapping_ids(
            &state.levels[level + 1],
            &upper_sst.first_key,
            &upper_sst.last_key,
        );

        Some(CompactionTask {
            upper_level:   Some(level),
            upper_sst_ids: vec![upper_sst.id],
            lower_level:   level + 1,
            lower_sst_ids,
        })
    }
}