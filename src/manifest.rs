//! Manifest — crash-safe record of which SST files are live at each level.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

pub(crate) const MANIFEST_FILENAME: &str = "MANIFEST";

// ── Record types ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ManifestRecord {
    /// Flush completed: SST `id` is now live at `level`.
    /// Written (and fsynced) before the corresponding WAL is deleted.
    NewFile { level: usize, id: usize },

    /// Compaction completed.
    /// `added`   = (level, id) pairs for the new output SSTs.
    /// `removed` = (level, id) pairs for the input SSTs being retired.
    /// Written (and fsynced) before any old SST file is deleted.
    CompactionEdit {
        added:   Vec<(usize, usize)>,
        removed: Vec<(usize, usize)>,
    },
}

// ── Manifest ──────────────────────────────────────────────────────────────────

pub(crate) struct Manifest {
    writer: BufWriter<File>,
}

impl Manifest {
    /// Create a brand-new manifest, truncating any existing file.
    pub fn create(dir: &Path) -> Result<Self> {
        let path = dir.join(MANIFEST_FILENAME);
        let file = OpenOptions::new()
            .create(true).write(true).truncate(true)
            .open(&path)?;
        Ok(Manifest { writer: BufWriter::new(file) })
    }

    /// Open (or create) the manifest for append and replay its records.
    pub fn open(dir: &Path, num_levels: usize) -> Result<(Self, Vec<Vec<usize>>)> {
        let path = dir.join(MANIFEST_FILENAME);
        let mut levels: Vec<Vec<usize>> = vec![Vec::new(); num_levels];

        if path.exists() {
            let f = File::open(&path)?;
            let mut reader = BufReader::new(f);
            loop {
                match read_record(&mut reader) {
                    Ok(Some(record)) => apply_record(&mut levels, record),
                    Ok(None)         => break,          // clean EOF
                    Err(_)           => break,          // truncated tail — same policy as WAL
                }
            }
        }

        let file = OpenOptions::new()
            .create(true).append(true)
            .open(&path)?;

        Ok((Manifest { writer: BufWriter::new(file) }, levels))
    }

    /// Append a record and fsync before returning.
    pub fn append(&mut self, record: &ManifestRecord) -> Result<()> {
        write_record(&mut self.writer, record)?;
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        Ok(())
    }
}

// ── Replay ────────────────────────────────────────────────────────────────────

fn apply_record(levels: &mut [Vec<usize>], record: ManifestRecord) {
    match record {
        ManifestRecord::NewFile { level, id } => {
            if level < levels.len() {
                levels[level].push(id);
            }
        }
        ManifestRecord::CompactionEdit { added, removed } => {
            let removed_set: std::collections::HashSet<(usize, usize)> =
                removed.into_iter().collect();
            for (lvl, ids) in levels.iter_mut().enumerate() {
                ids.retain(|&id| !removed_set.contains(&(lvl, id)));
            }
            for (level, id) in added {
                if level < levels.len() {
                    levels[level].push(id);
                }
            }
        }
    }
}

// ── Wire format: 4-byte-length-prefixed postcard + CRC32 ─────────────────────

fn write_record<W: Write>(w: &mut W, record: &ManifestRecord) -> Result<()> {
    let encoded = postcard::to_allocvec(record)?;
    let crc     = crc32fast::hash(&encoded);
    w.write_all(&(encoded.len() as u32).to_le_bytes())?;
    w.write_all(&encoded)?;
    w.write_all(&crc.to_le_bytes())?;
    Ok(())
}

fn read_record<R: Read>(r: &mut R) -> Result<Option<ManifestRecord>> {
    let mut len_buf = [0u8; 4];
    match r.read_exact(&mut len_buf) {
        Ok(())                                                       => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e)                                                  => return Err(e.into()),
    }
    let len = u32::from_le_bytes(len_buf) as usize;

    let mut encoded = vec![0u8; len];
    r.read_exact(&mut encoded)?;

    let mut crc_buf = [0u8; 4];
    r.read_exact(&mut crc_buf)?;
    let stored_crc = u32::from_le_bytes(crc_buf);

    if crc32fast::hash(&encoded) != stored_crc {
        anyhow::bail!("manifest CRC mismatch — truncated tail");
    }

    Ok(Some(postcard::from_bytes(&encoded)?))
}