use anyhow::Result;
use bytes::Bytes;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

const OP_PUT:    u8 = 1;
const OP_DELETE: u8 = 0;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum WalEntry {
    Put    { key: Bytes, value: Bytes },
    Delete { key: Bytes },
}

pub struct Wal {
    writer: BufWriter<File>,
    pub path: PathBuf,
}

impl Wal {
    pub fn new(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        Ok(Wal { writer: BufWriter::new(file), path: path.to_owned() })
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write_record(OP_PUT, key, value)
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.write_record(OP_DELETE, key, &[])
    }

    fn write_record(&mut self, op: u8, key: &[u8], val: &[u8]) -> Result<()> {
        let mut record = Vec::new();
        record.push(op);
        record.extend_from_slice(&(key.len() as u16).to_le_bytes());
        record.extend_from_slice(key);
        record.extend_from_slice(&(val.len() as u16).to_le_bytes());
        record.extend_from_slice(val);

        let crc = crc32fast::hash(&record);
        record.extend_from_slice(&crc.to_le_bytes());

        self.writer.write_all(&record)?;
        // Flush on every write — guarantees durability before memtable is updated
        self.writer.flush()?;
        Ok(())
    }

    /// Read back all valid records from a WAL file.
    /// Stops at the first record with a bad CRC — this is a partial write
    /// from a crash mid-record and is safe to discard.
    pub fn replay(path: &Path) -> Result<Vec<WalEntry>> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();

        loop {
            let mut op_buf = [0u8; 1];
            if reader.read(&mut op_buf)? == 0 { break; } // clean EOF
            let op = op_buf[0];

            let mut len_buf = [0u8; 2];

            if reader.read_exact(&mut len_buf).is_err() { break; }
            let key_len = u16::from_le_bytes(len_buf) as usize;
            let mut key = vec![0u8; key_len];
            if reader.read_exact(&mut key).is_err() { break; }

            if reader.read_exact(&mut len_buf).is_err() { break; }
            let val_len = u16::from_le_bytes(len_buf) as usize;
            let mut val = vec![0u8; val_len];
            if reader.read_exact(&mut val).is_err() { break; }

            let mut crc_buf = [0u8; 4];
            if reader.read_exact(&mut crc_buf).is_err() { break; }
            let stored_crc = u32::from_le_bytes(crc_buf);

            let mut payload = Vec::new();
            payload.push(op);
            payload.extend_from_slice(&(key_len as u16).to_le_bytes());
            payload.extend_from_slice(&key);
            payload.extend_from_slice(&(val_len as u16).to_le_bytes());
            payload.extend_from_slice(&val);

            if crc32fast::hash(&payload) != stored_crc { break; }

            entries.push(match op {
                OP_PUT    => WalEntry::Put    { key: Bytes::from(key), value: Bytes::from(val) },
                OP_DELETE => WalEntry::Delete { key: Bytes::from(key) },
                _         => anyhow::bail!("unknown WAL op: {op}"),
            });
        }

        Ok(entries)
    }
}