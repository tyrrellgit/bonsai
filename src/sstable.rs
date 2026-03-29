use anyhow::Result;
use bytes::Bytes;
use fastbloom::BloomFilter;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::io::Take;
use std::ops::Bound;
use std::path::{Path, PathBuf};

use crate::memtable::MemTable;

const BLOOM_FP_RATE: f64 = 0.01;

pub struct SSTableEntry {
    pub key:   Bytes,
    pub value: Bytes,
}

pub struct SSTable {
    pub id:        usize,
    pub path:      PathBuf,
    pub first_key: Bytes,
    pub last_key:  Bytes,
    data_len:      u64,   // byte length of data section, excluding bloom footer
    bloom:         BloomFilter,
}

pub struct SSTableIter {
    reader:  Take<BufReader<File>>,
    upper:   Bound<Bytes>,
    pending: Option<(Bytes, Bytes)>, // first qualifying entry from lower bound scan
}

impl Iterator for SSTableIter {
    type Item = (Bytes, Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        let (key, value) = if let Some(p) = self.pending.take() {
            p
        } else {
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

impl SSTable {
    pub fn from_memtable(mem: &MemTable, path: &Path) -> Result<Self> {
        let file = OpenOptions::new().write(true).create(true).truncate(false).open(path)?;
        let mut writer = BufWriter::new(file);

        let mut bloom = BloomFilter::with_false_pos(BLOOM_FP_RATE)
            .expected_items(mem.entry_count());

        let mut first_key = Bytes::new();
        let mut last_key  = Bytes::new();
        let mut data_len  = 0u64;

        for (count, (key, value)) in mem.iter().enumerate() {
            if count == 0 { first_key = key.clone(); }
            last_key = key.clone();

            bloom.insert(key.as_ref());
            data_len += write_entry(&mut writer, &key, &value)?;
        }

        let bloom_bytes = bincode::serialize(&bloom)?;
        writer.write_all(&bloom_bytes)?;
        writer.write_all(&(bloom_bytes.len() as u32).to_le_bytes())?;
        writer.flush()?;

        Ok(SSTable { id: mem.id, path: path.to_owned(), first_key, last_key, data_len, bloom })
    }

    pub fn open(id: usize, path: PathBuf) -> Result<Self> {
        let mut file = File::open(&path)?;

        file.seek(SeekFrom::End(-4))?;
        let mut len_buf = [0u8; 4];
        file.read_exact(&mut len_buf)?;
        let bloom_len = u32::from_le_bytes(len_buf) as usize;

        file.seek(SeekFrom::End(-(4 + bloom_len as i64)))?;
        let mut bloom_bytes = vec![0u8; bloom_len];
        file.read_exact(&mut bloom_bytes)?;
        let bloom: BloomFilter = bincode::deserialize(&bloom_bytes)?;

        let total_len = file.metadata()?.len();
        let data_len  = total_len - 4 - bloom_len as u64;

        file.seek(SeekFrom::Start(0))?;
        let mut reader = BufReader::new(file.take(data_len));

        let mut first_key = Bytes::new();
        let mut last_key  = Bytes::new();
        let mut count     = 0usize;

        while let Some(entry) = read_entry(&mut reader)? {
            if count == 0 { first_key = entry.key.clone(); }
            last_key = entry.key;
            count += 1;
        }

        Ok(SSTable { id, path, first_key, last_key, data_len, bloom })
    }

    pub fn get(&self, key: &Bytes) -> Result<Option<Bytes>> {
        if !self.bloom.contains(key.as_ref()) {
            return Ok(None);
        }

        let file = File::open(&self.path)?;
        let mut reader = BufReader::new(file).take(self.data_len);

        while let Some(entry) = read_entry(&mut reader)? {
            match entry.key.cmp(key) {
                std::cmp::Ordering::Equal   => return Ok(Some(entry.value)),
                std::cmp::Ordering::Greater => return Ok(None),
                std::cmp::Ordering::Less    => continue,
            }
        }
        Ok(None)
    }

    pub fn scan(&self, lower: Bound<Bytes>, upper: Bound<Bytes>) -> Result<SSTableIter> {
        let file   = File::open(&self.path)?;
        let mut reader = BufReader::new(file).take(self.data_len);

        // Advance past entries below the lower bound, holding onto the first match
        let mut pending = None;
        while let Some(entry) = read_entry(&mut reader)? {
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

        Ok(SSTableIter { reader, upper, pending })
    }
}

fn write_entry<W: Write>(w: &mut W, key: &[u8], val: &[u8]) -> Result<u64> {
    w.write_all(&(key.len() as u16).to_le_bytes())?;
    w.write_all(key)?;
    w.write_all(&(val.len() as u16).to_le_bytes())?;
    w.write_all(val)?;
    Ok(2 + key.len() as u64 + 2 + val.len() as u64)
}

fn read_entry<R: Read>(r: &mut R) -> Result<Option<SSTableEntry>> {
    let mut len = [0u8; 2];
    if r.read(&mut len)? == 0 { return Ok(None); }

    let key_len = u16::from_le_bytes(len) as usize;
    let mut key = vec![0u8; key_len];
    r.read_exact(&mut key)?;

    r.read_exact(&mut len)?;
    let val_len = u16::from_le_bytes(len) as usize;
    let mut val = vec![0u8; val_len];
    r.read_exact(&mut val)?;

    Ok(Some(SSTableEntry { key: Bytes::from(key), value: Bytes::from(val) }))
}