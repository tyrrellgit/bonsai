use anyhow::Result;
use bytes::Bytes;
use fastbloom::BloomFilter;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Bound;
use std::path::{Path, PathBuf};

use crate::memtable::MemTable;

const BLOOM_FP_RATE: f64 = 0.01;
const INDEX_STRIDE: usize = 16; // one index entry per 16 data entries
const FOOTER_LEN_BYTES: u64 = std::mem::size_of::<u32>() as u64;

// ── Footer ────────────────────────────────────────────────────────────────────
// Written after the data section. Last 4 bytes of the file are the footer length,
// so open() can seek straight to an index without scanning.

#[derive(Serialize, Deserialize)]
struct SSTFooter {
    data_len:  u64,
    index:     Vec<(Vec<u8>, u64)>, // sparse: (key, byte_offset_of_entry)
    bloom:     BloomFilter,
    first_key: Vec<u8>,
    last_key:  Vec<u8>,
}

// ── Public types ──────────────────────────────────────────────────────────────

pub(crate) struct SSTableEntry {
    pub key:   Bytes,
    pub value: Bytes,
}

pub(crate) struct SSTable {
    pub id:        usize,
    pub path:      PathBuf,
    pub first_key: Bytes,
    pub last_key:  Bytes,
    data_len:      u64,
    index:         Vec<(Bytes, u64)>, // in-memory sparse index
    bloom:         BloomFilter,
}

pub(crate) struct SSTableIter {
    reader:   BufReader<File>,
    data_end: u64, // byte offset where data section ends
    upper:    Bound<Bytes>,
    pending:  Option<(Bytes, Bytes)>,
}

// ── Iterator ──────────────────────────────────────────────────────────────────

impl Iterator for SSTableIter {
    type Item = (Bytes, Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        let (key, value) = if let Some(p) = self.pending.take() {
            p
        } else {
            // Guard: stop before reading into the footer
            let pos = self.reader.stream_position().ok()?;
            if pos >= self.data_end { return None; }
            let entry = read_entry(&mut self.reader).ok()??;
            (entry.key, entry.value)
        };

        let past_upper = match &self.upper {
            Bound::Included(u) => key > *u,
            Bound::Excluded(u) => key >= *u,
            Bound::Unbounded   => false,
        };
        if past_upper { return None; }

        Some((key, value))
    }
}

// ── SSTable ───────────────────────────────────────────────────────────────────

impl SSTable {
    /// Write a new SSTable from a memtable
    pub fn from_memtable(mem: &MemTable, path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .write(true).create(true).truncate(true).open(path)?;
        let mut writer = BufWriter::new(file);

        let mut bloom = BloomFilter::with_false_pos(BLOOM_FP_RATE)
            .expected_items(mem.entry_count());

        let mut first_key  = Vec::new();
        let mut last_key   = Vec::new();
        let mut data_len   = 0u64;
        let mut raw_index: Vec<(Vec<u8>, u64)> = Vec::new();

        for (count, (key, value)) in mem.iter().enumerate() {
            if count == 0 { first_key = key.to_vec(); }
            last_key = key.to_vec();

            // Sparse index: record offset before every INDEX_STRIDE-th entry
            if count % INDEX_STRIDE == 0 {
                raw_index.push((key.to_vec(), data_len));
            }

            bloom.insert(key.as_ref());
            data_len += write_entry(&mut writer, &key, &value)?;
        }

        let footer = SSTFooter {
            data_len,
            index: raw_index,
            bloom,
            first_key: first_key.clone(),
            last_key:  last_key.clone(),
        };
        let footer_bytes = postcard::to_allocvec(&footer)?;
        writer.write_all(&footer_bytes)?;
        writer.write_all(&(footer_bytes.len() as u32).to_le_bytes())?;
        writer.flush()?;

        let index = footer.index.iter()
            .map(|(k, o)| (Bytes::copy_from_slice(k), *o))
            .collect();

        Ok(SSTable {
            id: mem.id,
            path: path.to_owned(),
            first_key: Bytes::from(first_key),
            last_key:  Bytes::from(last_key),
            data_len,
            index,
            bloom: footer.bloom,
        })
    }

    /// Open an existing SSTable
    pub fn open(id: usize, path: PathBuf) -> Result<Self> {
        let mut file = File::open(&path)?;

        // Last 4 bytes = footer length
        file.seek(SeekFrom::End(-(FOOTER_LEN_BYTES as i64)))?;
        let mut len_buf = [0u8; FOOTER_LEN_BYTES as usize];
        file.read_exact(&mut len_buf)?;
        let footer_len = u32::from_le_bytes(len_buf) as i64;

        // Seek back and read footer
        file.seek(SeekFrom::End(-((FOOTER_LEN_BYTES as i64) + footer_len)))?;
        let mut footer_bytes = vec![0u8; footer_len as usize];
        file.read_exact(&mut footer_bytes)?;
        let footer: SSTFooter = postcard::from_bytes(&footer_bytes)?;

        let total_len = file.metadata()?.len();
        let data_len  = total_len - FOOTER_LEN_BYTES - footer_len as u64;

        let index = footer.index.iter()
            .map(|(k, o)| (Bytes::copy_from_slice(k), *o))
            .collect();

        Ok(SSTable {
            id,
            path,
            first_key: Bytes::from(footer.first_key),
            last_key:  Bytes::from(footer.last_key),
            data_len,
            index,
            bloom: footer.bloom,
        })
    }

    /// Point lookup
    pub fn get(&self, key: &Bytes) -> Result<Option<Bytes>> {
        if !self.bloom.contains(key.as_ref()) {
            return Ok(None);
        }

        let offset = self.index_seek(key);
        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(offset))?;

        // Limit reads to the data section, not the footer
        let mut reader = BufReader::new(file.take(self.data_len - offset));

        while let Some(entry) = read_entry(&mut reader)? {
            match entry.key.cmp(key) {
                std::cmp::Ordering::Equal   => return Ok(Some(entry.value)),
                std::cmp::Ordering::Greater => return Ok(None),
                std::cmp::Ordering::Less    => continue,
            }
        }
        Ok(None)
    }

    /// Range scan streaming iterator.
    pub fn scan(&self, lower: Bound<Bytes>, upper: Bound<Bytes>) -> Result<SSTableIter> {
        let offset = match &lower {
            Bound::Included(l) | Bound::Excluded(l) => self.index_seek(l),
            Bound::Unbounded                         => 0,
        };

        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(offset))?;
        let mut reader = BufReader::new(file);

        // Short linear scan from the index position to the exact lower bound
        let mut pending = None;
        loop {
            let pos = reader.stream_position()?;
            if pos >= self.data_len { break; }

            let Some(entry) = read_entry(&mut reader)? else { break };

            let meets_lower = match &lower {
                Bound::Included(l) => entry.key >= *l,
                Bound::Excluded(l) => entry.key >  *l,
                Bound::Unbounded   => true,
            };
            if meets_lower {
                pending = Some((entry.key, entry.value));
                break;
            }
        }

        Ok(SSTableIter { reader, data_end: self.data_len, upper, pending })
    }

    /// Binary search the sparse index for the largest key ≤ target.
    /// Returns the byte offset to seek to before linear scanning.
    fn index_seek(&self, key: &Bytes) -> u64 {
        if self.index.is_empty() { return 0; }

        match self.index.binary_search_by(|(k, _)| k.as_ref().cmp(key.as_ref())) {
            Ok(i)  => self.index[i].1,     // exact match in index
            Err(0) => 0,                          // key is before all index entries
            Err(i) => self.index[i - 1].1, // largest index entry before key
        }
    }
}

// ── Wire format ───────────────────────────────────────────────────────────────

fn write_entry<W: Write>(w: &mut W, key: &[u8], val: &[u8]) -> Result<u64> {
    w.write_all(&(key.len() as u16).to_le_bytes())?;
    w.write_all(key)?;
    w.write_all(&(val.len() as u16).to_le_bytes())?;
    w.write_all(val)?;
    Ok(2 + key.len() as u64 + 2 + val.len() as u64)
}

fn read_entry<R: Read>(r: &mut R) -> Result<Option<SSTableEntry>> {
    let mut len = [0u8; 2];
    match r.read_exact(&mut len) {
        Ok(())                                                           => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof   => return Ok(None),
        Err(e)                                                    => return Err(e.into()),
    }

    let key_len = u16::from_le_bytes(len) as usize;
    let mut key = vec![0u8; key_len];
    r.read_exact(&mut key)?;

    r.read_exact(&mut len)?;
    let val_len = u16::from_le_bytes(len) as usize;
    let mut val = vec![0u8; val_len];
    r.read_exact(&mut val)?;

    Ok(Some(SSTableEntry { key: Bytes::from(key), value: Bytes::from(val) }))
}