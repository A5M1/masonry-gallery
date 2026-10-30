use std::io;

use super::codec::{invalid_data, put_varint, Reader};
use super::format::{INDEX_MAGIC_HI, INDEX_MAGIC_LO};

#[derive(Clone, Copy, Debug)]
pub struct IndexEntry {
    pub file_offset: u64,
    pub filename_hash: u64,
}

pub fn write_index_block(buf: &mut Vec<u8>, entries: &mut [IndexEntry]) {
    if entries.is_empty() {
        return;
    }
    entries.sort_by_key(|entry| entry.filename_hash);
    buf.push(INDEX_MAGIC_HI);
    buf.push(INDEX_MAGIC_LO);
    put_varint(buf, entries.len() as u64);
    for entry in entries.iter() {
        put_varint(buf, entry.file_offset);
        put_varint(buf, entry.filename_hash);
    }
}

pub fn read_index_block(reader: &mut Reader<'_>) -> io::Result<Vec<IndexEntry>> {
    let magic_hi = reader.read_u8()?;
    let magic_lo = reader.read_u8()?;
    if magic_hi != INDEX_MAGIC_HI || magic_lo != INDEX_MAGIC_LO {
        return Err(invalid_data("invalid index magic"));
    }
    let count = reader.read_varint()?;
    if count > 10_000_000 {
        return Err(invalid_data("index entry count exceeds limit"));
    }
    let mut entries = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let file_offset = reader.read_varint()?;
        let filename_hash = reader.read_varint()?;
        entries.push(IndexEntry {
            file_offset,
            filename_hash,
        });
    }
    Ok(entries)
}
