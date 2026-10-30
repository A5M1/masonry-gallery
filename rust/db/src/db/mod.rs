use std::collections::{HashMap, VecDeque};
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub mod codec;
pub mod ffi;
pub mod format;
pub mod index;

use codec::{
    crc32, fnv1a64, invalid_data, join_components, md5_file, normalize_path,
    parse_numeric_filename, path_component_suffixes, path_components, put_varint, Reader,
};
use format::{
    decode_record, encode_record, read_header, serialize_record_to_value, write_header, FileHeader,
    HashMode, MediaType, MetaByte, Record, RecordDeltaState, FLAG_HAS_INDEX, INDEX_MAGIC_HI,
    OP_BEGIN, OP_CHUNK_MARKER, OP_DELETE, OP_END, OP_TX_BEGIN, OP_TX_END,
};
use index::{read_index_block, write_index_block, IndexEntry};

const WAL_DIR_NAME: &str = "wal";
const WAL_CHUNK_PREFIX: &str = "chunk-";

static WAL_CHUNK_SEQ: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
struct AsyncOp {
    key: String,
    value: Option<String>,
    is_delete: bool,
}

struct DbState {
    db_path: PathBuf,
    map: HashMap<String, String>,
    records: HashMap<String, Record>,
    dir_table: Vec<String>,
    index_table: Vec<IndexEntry>,
    header: FileHeader,
    current_record_seq: u64,
    delta: RecordDeltaState,
    records_start: usize,
    tx_active: bool,
    tx_snapshot: Option<HashMap<String, String>>,
    tx_snapshot_records: Option<HashMap<String, Record>>,
    db_inited: bool,
}

impl DbState {
    fn new(path: PathBuf) -> Self {
        Self {
            db_path: path,
            map: HashMap::new(),
            records: HashMap::new(),
            dir_table: Vec::new(),
            index_table: Vec::new(),
            header: FileHeader::new(now_seconds(), 0),
            current_record_seq: 0,
            delta: RecordDeltaState::default(),
            records_start: 0,
            tx_active: false,
            tx_snapshot: None,
            tx_snapshot_records: None,
            db_inited: false,
        }
    }
}

pub struct ThumbDb {
    state: Mutex<DbState>,
    compaction_requested: AtomicBool,
    async_running: AtomicBool,
    async_queue: Mutex<VecDeque<AsyncOp>>,
    refs: AtomicUsize,
}

impl Drop for ThumbDb {
    fn drop(&mut self) {
        self.async_running.store(false, Ordering::SeqCst);
    }
}

fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn path_with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

fn wal_dir_for(db_path: &Path) -> Option<PathBuf> {
    db_path.parent().map(|parent| parent.join(WAL_DIR_NAME))
}

fn write_via_temp(path: &Path, data: &[u8]) -> io::Result<()> {
    let temp_path = path_with_suffix(path, ".tmp");
    {
        let mut file = fs::File::create(&temp_path)?;
        file.write_all(data)?;
        file.sync_all()?;
    }
    let backup_path = path_with_suffix(path, ".bak");
    let had_original = path.exists();
    if backup_path.exists() {
        let _ = fs::remove_file(&backup_path);
    }
    if had_original {
        fs::rename(path, &backup_path)?;
    }
    if let Err(error) = fs::rename(&temp_path, path) {
        if had_original {
            let _ = fs::rename(&backup_path, path);
        }
        return Err(error);
    }
    if had_original {
        let _ = fs::remove_file(&backup_path);
    }
    Ok(())
}

fn create_empty_file(path: &Path) -> io::Result<(FileHeader, usize)> {
    let header = FileHeader::new(now_seconds(), 0);
    let mut data = Vec::new();
    write_header(&mut data, &header, &[]);
    let records_start = data.len();
    let mut file = fs::File::create(path)?;
    file.write_all(&data)?;
    file.sync_all()?;
    Ok((header, records_start))
}

fn append_bytes(path: &Path, data: &[u8]) -> io::Result<()> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    file.write_all(data)?;
    file.sync_all()?;
    Ok(())
}

fn add_dir_to_table(state: &mut DbState, component: &str) -> u32 {
    if let Some(index) = state
        .dir_table
        .iter()
        .position(|existing| existing == component)
    {
        return index as u32;
    }
    let index = state.dir_table.len() as u32;
    state.dir_table.push(component.to_string());
    index
}

fn add_dirs_for_path(state: &mut DbState, value: &str) -> Vec<u32> {
    path_component_suffixes(value)
        .into_iter()
        .map(|component| add_dir_to_table(state, &component))
        .collect()
}

fn lookup_dirs_for_path(state: &DbState, value: &str) -> Option<Vec<u32>> {
    path_component_suffixes(value)
        .into_iter()
        .map(|component| {
            state
                .dir_table
                .iter()
                .position(|existing| existing == &component)
                .map(|index| index as u32)
        })
        .collect()
}

fn hash_for_value(value: &str, mode: HashMode) -> Vec<u8> {
    let path = Path::new(value);
    let digest = if path.is_file() {
        md5_file(path).unwrap_or([0u8; 16])
    } else {
        [0u8; 16]
    };
    let length = mode.length();
    if length == 0 {
        Vec::new()
    } else {
        let mut out = vec![0u8; length];
        let copy_len = length.min(digest.len());
        out[..copy_len].copy_from_slice(&digest[..copy_len]);
        out
    }
}

fn build_record(state: &mut DbState, key: &str, value: &str, sequence: u64) -> Record {
    let media_type = MediaType::from_path(value);
    let meta = MetaByte {
        media_type,
        animated: false,
        thumb_mode: 0,
        hash_override: false,
        has_extensions: false,
        hash_mode: state.header.default_hash_mode,
    };
    let mut record = Record::new(sequence, key.to_string(), now_seconds(), meta);
    record.dir_indexes = add_dirs_for_path(state, value);
    record.dir_count = record.dir_indexes.len();
    record.hash = hash_for_value(value, meta.hash_mode);
    record.hash_len = meta.hash_mode.length();
    record
}

fn build_record_with_dirs(
    state: &DbState,
    key: &str,
    value: &str,
    sequence: u64,
) -> Option<Record> {
    let media_type = MediaType::from_path(value);
    let meta = MetaByte {
        media_type,
        animated: false,
        thumb_mode: 0,
        hash_override: false,
        has_extensions: false,
        hash_mode: state.header.default_hash_mode,
    };
    let mut record = Record::new(sequence, key.to_string(), now_seconds(), meta);
    record.dir_indexes = lookup_dirs_for_path(state, value)?;
    record.dir_count = record.dir_indexes.len();
    record.hash = hash_for_value(value, meta.hash_mode);
    record.hash_len = meta.hash_mode.length();
    Some(record)
}

fn find_next_begin(data: &[u8], start: usize) -> usize {
    let mut pos = start.min(data.len());
    while pos < data.len() {
        if data[pos] == OP_BEGIN {
            return pos;
        }
        pos += 1;
    }
    data.len()
}

fn load_database(state: &mut DbState) -> io::Result<()> {
    let data = fs::read(&state.db_path)?;
    let mut reader = Reader::new(&data);
    let (header, dir_table) = read_header(&mut reader)?;
    let records_start = reader.position();

    state.header = header;
    state.dir_table = dir_table;
    state.map.clear();
    state.records.clear();
    state.index_table.clear();
    state.delta = RecordDeltaState {
        last_filename_value: 0,
        last_timestamp_value: state.header.base_timestamp,
    };
    state.records_start = records_start;
    state.current_record_seq = 0;

    let mut is_first = true;
    while reader.position() < data.len() {
        let peek = reader.peek_u8()?;
        if state.header.has_index && peek == INDEX_MAGIC_HI {
            let mut index_reader = Reader::with_pos(&data, reader.position());
            if let Ok(entries) = read_index_block(&mut index_reader) {
                state.index_table = entries;
                reader.set_position(index_reader.position())?;
                state.delta = RecordDeltaState {
                    last_filename_value: 0,
                    last_timestamp_value: state.header.base_timestamp,
                };
                state.current_record_seq = 0;
                is_first = true;
                continue;
            }
            break;
        }
        match peek {
            OP_TX_BEGIN | OP_TX_END => {
                reader.read_u8()?;
            }
            OP_DELETE => {
                reader.read_u8()?;
                let filename_value = reader.read_varint()?;
                let _timestamp = reader.read_varint()?;
                let end = reader.read_u8()?;
                if end != OP_END {
                    return Err(invalid_data("invalid delete marker"));
                }
                let key = filename_value.to_string();
                state.map.remove(&key);
                state.records.remove(&key);
            }
            OP_CHUNK_MARKER => {
                reader.read_u8()?;
                let _count = reader.read_varint()?;
            }
            _ => {
                let start = reader.position();
                state.current_record_seq = state.current_record_seq.wrapping_add(1);
                match decode_record(
                    &data,
                    start,
                    &state.dir_table,
                    &mut state.delta,
                    is_first,
                    state.header.default_hash_mode,
                ) {
                    Ok((record, end)) => {
                        reader.set_position(end)?;
                        is_first = false;
                        if let Some(value) = serialize_record_to_value(&record, &state.dir_table) {
                            state.map.insert(record.filename.clone(), value);
                            state.records.insert(record.filename.clone(), record);
                        }
                    }
                    Err(error) => {
                        if error.crc_mismatch || error.end_pos > start {
                            reader.set_position(error.end_pos.min(data.len()))?;
                        } else {
                            let next = find_next_begin(&data, start.saturating_add(1));
                            reader.set_position(next)?;
                        }
                        if reader.position() <= start {
                            reader.set_position(find_next_begin(&data, start.saturating_add(1)))?;
                        }
                        if reader.position() >= data.len() {
                            break;
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

fn validate_file(path: &Path) -> i32 {
    let data = match fs::read(path) {
        Ok(data) => data,
        Err(_) => return -1,
    };
    let mut reader = Reader::new(&data);
    let (header, dir_table) = match read_header(&mut reader) {
        Ok(value) => value,
        Err(_) => return -1,
    };
    let mut delta = RecordDeltaState {
        last_filename_value: 0,
        last_timestamp_value: header.base_timestamp,
    };
    let mut is_first = true;
    let mut crc_errors = 0i32;
    while reader.position() < data.len() {
        let peek = match reader.peek_u8() {
            Ok(value) => value,
            Err(_) => break,
        };
        if header.has_index && peek == INDEX_MAGIC_HI {
            break;
        }
        match peek {
            OP_TX_BEGIN | OP_TX_END => {
                if reader.read_u8().is_err() {
                    break;
                }
            }
            OP_DELETE => {
                if reader.read_u8().is_err()
                    || reader.read_varint().is_err()
                    || reader.read_varint().is_err()
                    || reader.read_u8().ok() != Some(OP_END)
                {
                    return -1;
                }
            }
            OP_CHUNK_MARKER => {
                let _ = reader.read_u8();
                let _ = reader.read_varint();
            }
            _ => {
                let start = reader.position();
                match decode_record(
                    &data,
                    start,
                    &dir_table,
                    &mut delta,
                    is_first,
                    header.default_hash_mode,
                ) {
                    Ok((_record, end)) => {
                        if reader.set_position(end).is_err() {
                            break;
                        }
                        is_first = false;
                    }
                    Err(error) => {
                        if error.crc_mismatch {
                            crc_errors += 1;
                        }
                        let next = if error.end_pos > start {
                            error.end_pos
                        } else {
                            find_next_begin(&data, start.saturating_add(1))
                        };
                        if next <= start || reader.set_position(next.min(data.len())).is_err() {
                            return -1;
                        }
                    }
                }
            }
        }
    }
    if crc_errors > 0 {
        -1
    } else {
        0
    }
}

fn recover_from_corruption(state: &mut DbState) -> io::Result<()> {
    let backup_path = path_with_suffix(&state.db_path, ".corrupt");
    if backup_path.exists() {
        let _ = fs::remove_file(&backup_path);
    }
    if state.db_path.exists() {
        fs::rename(&state.db_path, &backup_path)?;
    }

    let data = fs::read(&backup_path).unwrap_or_default();
    let mut recovered_header = FileHeader::new(now_seconds(), 0);
    let mut recovered_dirs = Vec::new();
    if !data.is_empty() {
        let mut reader = Reader::new(&data);
        if let Ok((header, dirs)) = read_header(&mut reader) {
            recovered_header = header;
            recovered_dirs = dirs;
        }
    }

    state.header = recovered_header;
    state.dir_table = recovered_dirs;
    state.map.clear();
    state.records.clear();
    state.index_table.clear();
    state.delta = RecordDeltaState {
        last_filename_value: 0,
        last_timestamp_value: state.header.base_timestamp,
    };
    state.current_record_seq = 0;
    state.records_start = 0;

    let mut salvaged = 0usize;
    let mut pos = 0usize;
    while pos < data.len() {
        if data[pos] != OP_BEGIN {
            pos += 1;
            continue;
        }
        match decode_record(
            &data,
            pos,
            &state.dir_table,
            &mut state.delta,
            salvaged == 0,
            state.header.default_hash_mode,
        ) {
            Ok((record, end)) => {
                if let Some(value) = serialize_record_to_value(&record, &state.dir_table) {
                    state.map.insert(record.filename.clone(), value);
                    state.records.insert(record.filename.clone(), record);
                    salvaged += 1;
                }
                pos = end.max(pos + 1);
            }
            Err(error) => {
                pos = if error.end_pos > pos {
                    error.end_pos
                } else {
                    find_next_begin(&data, pos + 1)
                };
                if pos >= data.len() {
                    break;
                }
            }
        }
    }

    if salvaged > 0 {
        write_map_to_disk(state, true, true)?;
    } else {
        let (header, records_start) = create_empty_file(&state.db_path)?;
        state.header = header;
        state.dir_table.clear();
        state.records_start = records_start;
        state.delta = RecordDeltaState {
            last_filename_value: 0,
            last_timestamp_value: state.header.base_timestamp,
        };
        state.current_record_seq = 0;
    }
    Ok(())
}

fn write_map_to_disk(state: &mut DbState, rebuild_all: bool, with_index: bool) -> io::Result<()> {
    let map_entries: Vec<(String, String)> = state
        .map
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    for (_key, value) in &map_entries {
        let _ = add_dirs_for_path(state, value);
    }

    let mut header = state.header.clone();
    if with_index {
        header.flags |= FLAG_HAS_INDEX;
    } else {
        header.flags &= !FLAG_HAS_INDEX;
    }
    header.has_index = with_index;
    header.record_count = map_entries.len() as u64;
    header.dir_table_size = state.dir_table.len() as u64;

    let mut data = Vec::new();
    write_header(&mut data, &header, &state.dir_table);
    let records_start = data.len();
    let mut delta = RecordDeltaState {
        last_filename_value: 0,
        last_timestamp_value: header.base_timestamp,
    };
    let mut index_entries = Vec::new();
    let mut new_records = HashMap::new();

    for (sequence, (key, value)) in map_entries.iter().enumerate() {
        let sequence = sequence as u64;
        let existing = state.records.get(key).cloned();
        let mut record = if !rebuild_all {
            match existing {
                Some(existing)
                    if serialize_record_to_value(&existing, &state.dir_table).as_deref()
                        == Some(value.as_str()) =>
                {
                    existing
                }
                _ => build_record(state, key, value, sequence),
            }
        } else {
            build_record(state, key, value, sequence)
        };
        record.sequence = sequence;
        let offset = data.len() as u64;
        let encoded = encode_record(&record, &state.dir_table, &mut delta)?;
        data.extend_from_slice(&encoded);
        index_entries.push(IndexEntry {
            file_offset: offset,
            filename_hash: fnv1a64(key.as_bytes()),
        });
        new_records.insert(key.clone(), record);
    }

    if with_index {
        write_index_block(&mut data, &mut index_entries);
    }
    write_via_temp(&state.db_path, &data)?;

    state.header = header;
    state.records = new_records;
    state.records_start = records_start;
    state.current_record_seq = 0;
    state.delta = RecordDeltaState {
        last_filename_value: 0,
        last_timestamp_value: state.header.base_timestamp,
    };
    state.index_table = if with_index {
        index_entries
    } else {
        Vec::new()
    };
    Ok(())
}

fn prepare_for_append(state: &mut DbState) -> io::Result<()> {
    if state.header.has_index {
        write_map_to_disk(state, false, false)?;
    }
    Ok(())
}

fn append_record(state: &mut DbState, record: &Record) -> io::Result<()> {
    if !state.db_path.exists() {
        let (header, records_start) = create_empty_file(&state.db_path)?;
        state.header = header;
        state.records_start = records_start;
    }
    let mut delta = state.delta;
    let encoded = encode_record(record, &state.dir_table, &mut delta)?;
    append_bytes(&state.db_path, &encoded)?;
    state.delta = delta;
    Ok(())
}

impl ThumbDb {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut state = DbState::new(path.clone());
        if !path.exists() {
            let (header, records_start) = create_empty_file(&path)?;
            state.header = header;
            state.records_start = records_start;
            state.delta = RecordDeltaState {
                last_filename_value: 0,
                last_timestamp_value: state.header.base_timestamp,
            };
        } else if load_database(&mut state).is_err() || validate_file(&path) != 0 {
            recover_from_corruption(&mut state)?;
        }
        state.db_inited = true;
        Ok(Self {
            state: Mutex::new(state),
            compaction_requested: AtomicBool::new(false),
            async_running: AtomicBool::new(false),
            async_queue: Mutex::new(VecDeque::new()),
            refs: AtomicUsize::new(1),
        })
    }

    pub fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let (header, records_start) = create_empty_file(&path)?;
        let mut state = DbState::new(path);
        state.header = header;
        state.records_start = records_start;
        state.delta = RecordDeltaState {
            last_filename_value: 0,
            last_timestamp_value: state.header.base_timestamp,
        };
        state.db_inited = true;
        Ok(Self {
            state: Mutex::new(state),
            compaction_requested: AtomicBool::new(false),
            async_running: AtomicBool::new(false),
            async_queue: Mutex::new(VecDeque::new()),
            refs: AtomicUsize::new(1),
        })
    }

    pub fn db_path(&self) -> PathBuf {
        self.state
            .lock()
            .map(|state| state.db_path.clone())
            .unwrap_or_default()
    }

    pub fn set(&self, key: &str, value: &str) -> io::Result<()> {
        let mut state = self.state.lock().unwrap();
        if state.tx_active {
            state.map.insert(key.to_string(), value.to_string());
            return Ok(());
        }
        prepare_for_append(&mut state)?;
        let persisted_dirs = state.header.dir_table_size as usize;
        let sequence = state.current_record_seq;
        state.current_record_seq = state.current_record_seq.wrapping_add(1);
        let record = build_record(&mut state, key, value, sequence);
        state.map.insert(key.to_string(), value.to_string());
        state.records.insert(key.to_string(), record.clone());
        if state.dir_table.len() > persisted_dirs {
            write_map_to_disk(&mut state, false, false)
        } else {
            append_record(&mut state, &record)
        }
    }

    pub fn insert(
        &mut self,
        filename: &str,
        meta_byte: u8,
        dimensions: Option<(u32, u32)>,
        duration: Option<u64>,
    ) -> io::Result<()> {
        self.insert_raw(filename, meta_byte, dimensions, duration, None, &[])
    }

    pub fn insert_raw(
        &self,
        filename: &str,
        meta_byte: u8,
        dimensions: Option<(u32, u32)>,
        duration: Option<u64>,
        hash_mode: Option<HashMode>,
        hash: &[u8],
    ) -> io::Result<()> {
        let mut state = self.state.lock().unwrap();
        prepare_for_append(&mut state)?;
        let persisted_dirs = state.header.dir_table_size as usize;
        let sequence = state.current_record_seq;
        state.current_record_seq = state.current_record_seq.wrapping_add(1);
        let mut meta = MetaByte::decode(meta_byte, state.header.default_hash_mode);
        if let Some(mode) = hash_mode {
            meta.hash_mode = mode;
            meta.hash_override = mode != state.header.default_hash_mode;
        }
        if let Some((width, height)) = dimensions {
            meta.has_extensions = true;
            let mut record = Record::new(sequence, filename.to_string(), now_seconds(), meta);
            record.width = width;
            record.height = height;
            record.duration = duration.unwrap_or(0) as u32;
            record.hash_len = meta.hash_mode.length();
            record.hash = vec![0u8; record.hash_len];
            let copy_len = hash.len().min(record.hash_len);
            record.hash[..copy_len].copy_from_slice(&hash[..copy_len]);
            let map_value = serialize_record_to_value(&record, &state.dir_table)
                .unwrap_or_else(|| filename.to_string());
            state.map.insert(filename.to_string(), map_value);
            state.records.insert(filename.to_string(), record.clone());
            if state.dir_table.len() > persisted_dirs {
                return write_map_to_disk(&mut state, false, false);
            }
            return append_record(&mut state, &record);
        }

        let mut record = Record::new(sequence, filename.to_string(), now_seconds(), meta);
        record.hash_len = meta.hash_mode.length();
        record.hash = vec![0u8; record.hash_len];
        let copy_len = hash.len().min(record.hash_len);
        record.hash[..copy_len].copy_from_slice(&hash[..copy_len]);
        record.duration = duration.unwrap_or(0) as u32;
        if record.duration > 0 {
            meta.has_extensions = true;
            record.meta = meta;
        }
        let map_value = serialize_record_to_value(&record, &state.dir_table)
            .unwrap_or_else(|| filename.to_string());
        state.map.insert(filename.to_string(), map_value);
        state.records.insert(filename.to_string(), record.clone());
        if state.dir_table.len() > persisted_dirs {
            write_map_to_disk(&mut state, false, false)
        } else {
            append_record(&mut state, &record)
        }
    }

    pub fn get(&self, key: &str) -> Option<String> {
        self.state.lock().ok()?.map.get(key).cloned()
    }

    pub fn get_record(&self, key: &str) -> Option<Record> {
        let state = self.state.lock().ok()?;
        if let Some(record) = state.records.get(key) {
            return Some(record.clone());
        }
        let value = state.map.get(key)?;
        build_record_with_dirs(&state, key, value, 0)
    }

    pub fn delete(&self, key: &str) -> io::Result<bool> {
        let mut state = self.state.lock().unwrap();
        let existed = state.map.remove(key).is_some();
        state.records.remove(key);
        if state.tx_active {
            return Ok(existed);
        }
        let Some(filename_value) = parse_numeric_filename(key) else {
            write_map_to_disk(&mut state, false, false)?;
            return Ok(existed);
        };
        prepare_for_append(&mut state)?;
        let mut marker = Vec::new();
        marker.push(OP_DELETE);
        put_varint(&mut marker, filename_value);
        put_varint(&mut marker, now_seconds());
        marker.push(OP_END);
        append_bytes(&state.db_path, &marker)?;
        Ok(existed)
    }

    pub fn count(&self) -> usize {
        self.state.lock().map(|state| state.map.len()).unwrap_or(0)
    }

    pub fn iterate(&self) -> Vec<(String, String)> {
        self.state
            .lock()
            .map(|state| {
                state
                    .map
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn find_for_media(&self, media_path: &str) -> Option<String> {
        let state = self.state.lock().ok()?;
        let target = normalize_path(media_path);
        for (key, value) in state.map.iter() {
            if normalize_path(value) == target {
                return Some(key.clone());
            }
        }
        None
    }

    pub fn get_record_detail(&self, key: &str) -> Option<String> {
        let state = self.state.lock().ok()?;
        let record = state.records.get(key).cloned().or_else(|| {
            let value = state.map.get(key)?;
            build_record_with_dirs(&state, key, value, 0)
        })?;
        let hash = record
            .hash
            .iter()
            .take(record.hash_len.min(16))
            .map(|byte| format!("{:02x}", byte))
            .collect::<String>();
        Some(format!(
            "{{\"record_seq\":{},\"timestamp\":{},\"media_type\":\"{}\",\"animated\":{},\"thumb_mode\":{},\"hash_mode\":\"{}\",\"hash\":\"{}\",\"dir_count\":{},\"width\":{},\"height\":{},\"duration\":{},\"crc32\":{},\"orientation\":{},\"gps_lat\":{:.6},\"gps_lon\":{:.6}}}",
            record.sequence,
            record.timestamp,
            record.meta.media_type.name(),
            if record.meta.animated { "true" } else { "false" },
            record.meta.thumb_mode,
            record.meta.hash_mode.name(),
            hash,
            record.dir_count,
            record.width,
            record.height,
            record.duration,
            record.crc32,
            record.orientation,
            record.gps_lat,
            record.gps_lon
        ))
    }

    pub fn tx_begin(&self) -> io::Result<()> {
        let mut state = self.state.lock().unwrap();
        if state.tx_active {
            return Err(invalid_data("transaction already active"));
        }
        state.tx_snapshot = Some(state.map.clone());
        state.tx_snapshot_records = Some(state.records.clone());
        state.tx_active = true;
        Ok(())
    }

    pub fn tx_abort(&self) -> io::Result<()> {
        let mut state = self.state.lock().unwrap();
        if !state.tx_active {
            return Err(invalid_data("no active transaction"));
        }
        if let Some(snapshot) = state.tx_snapshot.take() {
            state.map = snapshot;
        }
        if let Some(snapshot) = state.tx_snapshot_records.take() {
            state.records = snapshot;
        }
        state.tx_active = false;
        Ok(())
    }

    pub fn tx_commit(&self) -> io::Result<()> {
        let mut state = self.state.lock().unwrap();
        if !state.tx_active {
            return Err(invalid_data("no active transaction"));
        }
        let snapshot = state.tx_snapshot.take().unwrap_or_default();
        let _snapshot_records = state.tx_snapshot_records.take();
        state.tx_active = false;

        let changed: Vec<(String, String)> = state
            .map
            .iter()
            .filter(|(key, value)| snapshot.get(*key) != Some(*value))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        let deleted: Vec<String> = snapshot
            .keys()
            .filter(|key| !state.map.contains_key(*key))
            .cloned()
            .collect();
        for key in &deleted {
            state.records.remove(key);
        }

        if !deleted.is_empty() {
            return write_map_to_disk(&mut state, false, false);
        }

        if changed.is_empty() {
            let marker = vec![OP_TX_BEGIN, OP_TX_END];
            append_bytes(&state.db_path, &marker)?;
            return Ok(());
        }

        let persisted_dirs = state.header.dir_table_size as usize;
        for (_key, value) in &changed {
            let _ = add_dirs_for_path(&mut state, value);
        }

        if state.dir_table.len() > persisted_dirs {
            for (key, value) in &changed {
                let sequence = state.current_record_seq;
                state.current_record_seq = state.current_record_seq.wrapping_add(1);
                let record = build_record(&mut state, key, value, sequence);
                state.records.insert(key.clone(), record);
            }
            return write_map_to_disk(&mut state, false, false);
        }

        if state.header.has_index {
            return write_map_to_disk(&mut state, false, false);
        }

        let mut data = Vec::new();
        data.push(OP_TX_BEGIN);
        let dir_table = state.dir_table.clone();
        for (key, value) in &changed {
            let sequence = state.current_record_seq;
            state.current_record_seq = state.current_record_seq.wrapping_add(1);
            let record = build_record(&mut state, key, value, sequence);
            let encoded = encode_record(&record, &dir_table, &mut state.delta)?;
            data.extend_from_slice(&encoded);
            state.records.insert(key.clone(), record);
        }
        data.push(OP_TX_END);
        append_bytes(&state.db_path, &data)?;
        Ok(())
    }

    pub fn compact(&self) -> io::Result<()> {
        let mut state = self.state.lock().unwrap();
        write_map_to_disk(&mut state, true, true)
    }

    pub fn sync(&self) -> io::Result<()> {
        let mut state = self.state.lock().unwrap();
        write_map_to_disk(&mut state, false, false)
    }

    pub fn validate(&self) -> i32 {
        let path = match self.state.lock() {
            Ok(state) => state.db_path.clone(),
            Err(_) => return -1,
        };
        validate_file(&path)
    }

    pub fn seek_to_record(&self, filename: &str) -> Option<String> {
        let state = self.state.lock().ok()?;
        if state.index_table.is_empty() || !state.header.has_index {
            return state.map.get(filename).cloned();
        }
        let target_hash = fnv1a64(filename.as_bytes());
        let found = state
            .index_table
            .binary_search_by_key(&target_hash, |entry| entry.filename_hash)
            .ok()
            .map(|index| state.index_table[index].file_offset)?;
        let data = fs::read(&state.db_path).ok()?;
        let mut reader = Reader::with_pos(&data, state.records_start);
        let mut delta = RecordDeltaState {
            last_filename_value: 0,
            last_timestamp_value: state.header.base_timestamp,
        };
        let mut is_first = true;
        while reader.position() < found as usize && reader.position() < data.len() {
            let peek = reader.peek_u8().ok()?;
            match peek {
                OP_TX_BEGIN | OP_TX_END => {
                    let _ = reader.read_u8();
                }
                OP_DELETE => {
                    let _ = reader.read_u8();
                    let _ = reader.read_varint();
                    let _ = reader.read_varint();
                    let _ = reader.read_u8();
                }
                OP_CHUNK_MARKER => {
                    let _ = reader.read_u8();
                    let _ = reader.read_varint();
                }
                _ => {
                    let start = reader.position();
                    let (_record, end) = decode_record(
                        &data,
                        start,
                        &state.dir_table,
                        &mut delta,
                        is_first,
                        state.header.default_hash_mode,
                    )
                    .ok()?;
                    reader.set_position(end).ok()?;
                    is_first = false;
                }
            }
        }
        let (record, _) = decode_record(
            &data,
            found as usize,
            &state.dir_table,
            &mut delta,
            is_first,
            state.header.default_hash_mode,
        )
        .ok()?;
        if record.filename != filename {
            return None;
        }
        serialize_record_to_value(&record, &state.dir_table)
    }

    pub fn sweep_orphans(&self) -> io::Result<usize> {
        let state = self.state.lock().unwrap();
        let Some(root) = state.db_path.parent() else {
            return Err(invalid_data("database has no parent directory"));
        };
        let mut deleted = 0usize;
        for entry in fs::read_dir(root)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            if is_reserved_thumbnail_dir_entry(&name) {
                continue;
            }
            let Some(base_key) = thumbnail_base_key(&name) else {
                continue;
            };
            if state.map.contains_key(&base_key) {
                continue;
            }
            if fs::remove_file(entry.path()).is_ok() {
                deleted += 1;
            }
        }
        Ok(deleted)
    }

    pub fn verify_thumbnails(&self) -> io::Result<(usize, usize, usize)> {
        let state = self.state.lock().unwrap();
        let Some(root) = state.db_path.parent() else {
            return Err(invalid_data("database has no parent directory"));
        };
        let mut total = 0usize;
        let mut valid = 0usize;
        let mut orphaned = 0usize;
        for entry in fs::read_dir(root)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            if is_reserved_thumbnail_dir_entry(&name) {
                continue;
            }
            let Some(base_key) = thumbnail_base_key(&name) else {
                continue;
            };
            total += 1;
            if state.map.contains_key(&base_key) {
                valid += 1;
            } else {
                orphaned += 1;
            }
        }
        Ok((total, valid, orphaned))
    }

    pub fn verify_records(&self) -> io::Result<(usize, usize, usize)> {
        let state = self.state.lock().unwrap();
        let Some(root) = state.db_path.parent() else {
            return Err(invalid_data("database has no parent directory"));
        };
        let mut total = 0usize;
        let mut valid = 0usize;
        let mut missing_thumbs = 0usize;
        for key in state.map.keys() {
            total += 1;
            let small = root.join(format!("{}-small.jpg", key));
            let large = root.join(format!("{}-large.jpg", key));
            if !small.is_file() && !large.is_file() {
                missing_thumbs += 1;
            } else {
                valid += 1;
            }
        }
        Ok((total, valid, missing_thumbs))
    }

    pub fn request_compaction(&self) {
        self.compaction_requested.store(true, Ordering::SeqCst);
    }

    pub fn perform_requested_compaction(&self) -> i32 {
        if !self.compaction_requested.swap(false, Ordering::SeqCst) {
            return 0;
        }
        match self.compact() {
            Ok(()) => 1,
            Err(_) => -1,
        }
    }

    pub fn write_wal_entry(&self, key: &str, value: Option<&str>) -> io::Result<()> {
        let db_path = self.db_path();
        let Some(wal_dir) = wal_dir_for(&db_path) else {
            return Err(invalid_data("database has no parent directory"));
        };
        fs::create_dir_all(&wal_dir)?;
        let sequence = WAL_CHUNK_SEQ.fetch_add(1, Ordering::SeqCst);
        let timestamp = now_seconds();
        let pid = std::process::id();
        let chunk_path = wal_dir.join(format!(
            "{}{}-{}-{}.wal",
            WAL_CHUNK_PREFIX, timestamp, sequence, pid
        ));
        let value_text = value.unwrap_or("__DELETE__");
        let payload = format!("{}\n{}\n", key, value_text);
        let checksum = crc32(payload.as_bytes());
        let contents = format!("{}CRC32:{:08X}\n", payload, checksum);
        let mut file = fs::File::create(&chunk_path)?;
        file.write_all(contents.as_bytes())?;
        file.sync_all()?;
        Ok(())
    }

    fn enqueue_async(&self, key: &str, value: Option<&str>, is_delete: bool) {
        if let Ok(mut queue) = self.async_queue.lock() {
            queue.push_back(AsyncOp {
                key: key.to_string(),
                value: value.map(|value| value.to_string()),
                is_delete,
            });
        }
    }

    pub fn set_async(&self, key: &str, value: &str) -> io::Result<()> {
        self.write_wal_entry(key, Some(value))?;
        self.enqueue_async(key, Some(value), false);
        Ok(())
    }

    pub fn delete_async(&self, key: &str) -> io::Result<()> {
        self.write_wal_entry(key, None)?;
        self.enqueue_async(key, None, true);
        Ok(())
    }

    pub fn process_wal_chunks(&self) -> io::Result<()> {
        let db_path = self.db_path();
        let Some(wal_dir) = wal_dir_for(&db_path) else {
            return Ok(());
        };
        if !wal_dir.is_dir() {
            return Ok(());
        }
        let mut state = self.state.lock().unwrap();
        for entry in fs::read_dir(&wal_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            if !name.contains(".wal") {
                continue;
            }
            let path = entry.path();
            let contents = match fs::read_to_string(&path) {
                Ok(contents) => contents,
                Err(_) => continue,
            };
            let mut lines = contents.lines();
            let key = match lines.next() {
                Some(line) => line.trim_end_matches('\r').to_string(),
                None => {
                    let _ = fs::remove_file(&path);
                    continue;
                }
            };
            let value = match lines.next() {
                Some(line) => line.trim_end_matches('\r').to_string(),
                None => {
                    let _ = fs::remove_file(&path);
                    continue;
                }
            };
            let checksum_line = lines.next().unwrap_or("").trim_end_matches('\r');
            let mut checksum_ok = false;
            if let Some(hex) = checksum_line.strip_prefix("CRC32:") {
                if let Ok(stored) = u32::from_str_radix(hex.trim(), 16) {
                    let payload = format!("{}\n{}\n", key, value);
                    checksum_ok = crc32(payload.as_bytes()) == stored;
                }
            }
            if checksum_ok {
                if value == "__DELETE__" {
                    state.map.remove(&key);
                    state.records.remove(&key);
                } else {
                    state.map.insert(key, value);
                }
            }
            let _ = fs::remove_file(&path);
        }
        Ok(())
    }

    fn apply_async_ops(&self) {
        loop {
            let op = match self.async_queue.lock() {
                Ok(mut queue) => queue.pop_front(),
                Err(_) => None,
            };
            let Some(op) = op else {
                break;
            };
            if let Ok(mut state) = self.state.lock() {
                if op.is_delete {
                    state.map.remove(&op.key);
                    state.records.remove(&op.key);
                } else if let Some(value) = op.value {
                    state.map.insert(op.key, value);
                }
            }
        }
    }

    pub fn start_async_worker(self: &Arc<Self>) {
        if self.async_running.swap(true, Ordering::SeqCst) {
            return;
        }
        let weak = Arc::downgrade(self);
        thread::spawn(move || loop {
            let Some(db) = weak.upgrade() else {
                break;
            };
            if !db.async_running.load(Ordering::SeqCst) {
                break;
            }
            db.apply_async_ops();
            drop(db);
            thread::sleep(Duration::from_millis(50));
        });
    }

    pub fn stop_async_worker(&self) {
        self.async_running.store(false, Ordering::SeqCst);
    }

    pub fn start_repair_task(self: &Arc<Self>, interval_seconds: u64) {
        let weak = Arc::downgrade(self);
        let interval = if interval_seconds > 0 {
            interval_seconds
        } else {
            3600
        };
        thread::spawn(move || loop {
            let mut slept = 0u64;
            while slept < interval {
                let Some(db) = weak.upgrade() else {
                    return;
                };
                let running = db
                    .state
                    .lock()
                    .map(|state| state.db_inited)
                    .unwrap_or(false);
                drop(db);
                if !running {
                    return;
                }
                thread::sleep(Duration::from_secs(1));
                slept += 1;
            }
            let Some(db) = weak.upgrade() else {
                return;
            };
            let _ = db.process_wal_chunks();
            let _ = db.perform_requested_compaction();
            let _ = db.sweep_orphans();
        });
    }
}

fn is_reserved_thumbnail_dir_entry(name: &str) -> bool {
    matches!(
        name,
        "wal"
            | "thumbs.tdb"
            | "thumbs.tdb.tmp"
            | "thumbs.tdb.bak"
            | "thumbs.db"
            | "thumbs.db.tmp"
            | "thumbs.db.bak"
    )
}

fn thumbnail_base_key(name: &str) -> Option<String> {
    let dot = name.rfind('.')?;
    let extension = &name[dot..];
    if !(extension.eq_ignore_ascii_case(".jpg")
        || extension.eq_ignore_ascii_case(".jpeg")
        || extension.eq_ignore_ascii_case(".webp"))
    {
        return None;
    }
    let marker = name.find("-small.").or_else(|| name.find("-large."))?;
    if marker == 0 {
        return None;
    }
    Some(name[..marker].to_string())
}

static REGISTRY: OnceLock<Mutex<HashMap<String, Arc<ThumbDb>>>> = OnceLock::new();

fn registry() -> &'static Mutex<HashMap<String, Arc<ThumbDb>>> {
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn open_for_dir_registered(path: &str) -> io::Result<Arc<ThumbDb>> {
    let mut registry = registry().lock().unwrap();
    if let Some(existing) = registry.get(path) {
        existing.refs.fetch_add(1, Ordering::SeqCst);
        return Ok(existing.clone());
    }
    let db = Arc::new(ThumbDb::open(path)?);
    db.refs.store(1, Ordering::SeqCst);
    registry.insert(path.to_string(), db.clone());
    Ok(db)
}

pub fn release_registered(db: &Arc<ThumbDb>) -> bool {
    let remaining = db.refs.fetch_sub(1, Ordering::SeqCst).saturating_sub(1);
    if remaining > 0 {
        return false;
    }
    let path = db.db_path().to_string_lossy().into_owned();
    let mut registry = registry().lock().unwrap();
    registry.remove(&path);
    true
}

pub fn find_registered(path: &str) -> Option<Arc<ThumbDb>> {
    registry().lock().ok()?.get(path).cloned()
}

pub fn normalized_media_path(path: &str) -> String {
    join_components(&path_components(path))
}

pub type MediaVaultDB = ThumbDb;

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_db_path(name: &str) -> PathBuf {
        let unique = format!(
            "galleria-rustdb-{}-{}-{}",
            name,
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let directory = std::env::temp_dir().join(unique);
        let _ = fs::create_dir_all(&directory);
        directory.join("thumbs.tdb")
    }

    fn cleanup_db(path: &Path) {
        if let Some(parent) = path.parent() {
            let _ = fs::remove_dir_all(parent);
        }
    }

    #[test]
    fn set_get_reopen_and_compact() {
        let path = temp_db_path("basic");
        {
            let db = ThumbDb::open(&path).unwrap();
            db.set("alpha", "C:\\photos\\alpha.jpg").unwrap();
            db.set("beta", "C:\\photos\\beta.jpg").unwrap();
            assert_eq!(db.get("alpha").as_deref(), Some("C:\\photos\\alpha.jpg"));
            db.compact().unwrap();
        }
        {
            let db = ThumbDb::open(&path).unwrap();
            assert_eq!(db.get("alpha").as_deref(), Some("C:\\photos\\alpha.jpg"));
            assert_eq!(db.get("beta").as_deref(), Some("C:\\photos\\beta.jpg"));
            assert!(db.validate() == 0);
        }
        cleanup_db(&path);
    }

    #[test]
    fn delete_tombstone_is_loaded() {
        let path = temp_db_path("delete");
        {
            let db = ThumbDb::open(&path).unwrap();
            db.set("123", "C:\\photos\\123.jpg").unwrap();
            db.delete("123").unwrap();
        }
        {
            let db = ThumbDb::open(&path).unwrap();
            assert_eq!(db.get("123"), None);
        }
        cleanup_db(&path);
    }

    #[test]
    fn transaction_rollback_and_commit() {
        let path = temp_db_path("tx");
        let db = ThumbDb::open(&path).unwrap();
        db.set("one", "C:\\photos\\one.jpg").unwrap();
        db.tx_begin().unwrap();
        db.set("two", "C:\\photos\\two.jpg").unwrap();
        assert_eq!(db.get("two").as_deref(), Some("C:\\photos\\two.jpg"));
        db.tx_abort().unwrap();
        assert_eq!(db.get("two"), None);
        db.tx_begin().unwrap();
        db.set("two", "C:\\photos\\two.jpg").unwrap();
        db.tx_commit().unwrap();
        db.compact().unwrap();
        drop(db);
        let db = ThumbDb::open(&path).unwrap();
        assert_eq!(db.get("two").as_deref(), Some("C:\\photos\\two.jpg"));
        drop(db);
        cleanup_db(&path);
    }

    #[test]
    fn compact_index_and_seek() {
        let path = temp_db_path("index");
        {
            let db = ThumbDb::open(&path).unwrap();
            db.set("111", "C:\\photos\\111.jpg").unwrap();
            db.set("abc", "C:\\photos\\abc.jpg").unwrap();
            db.set("222", "C:\\photos\\222.jpg").unwrap();
            db.compact().unwrap();
            assert_eq!(
                db.seek_to_record("abc").as_deref(),
                Some("C:\\photos\\abc.jpg")
            );
        }
        {
            let db = ThumbDb::open(&path).unwrap();
            assert_eq!(
                db.seek_to_record("abc").as_deref(),
                Some("C:\\photos\\abc.jpg")
            );
            assert_eq!(
                db.seek_to_record("222").as_deref(),
                Some("C:\\photos\\222.jpg")
            );
            assert_eq!(db.validate(), 0);
        }
        cleanup_db(&path);
    }

    #[test]
    fn insert_raw_extensions_round_trip() {
        let path = temp_db_path("extensions");
        {
            let db = ThumbDb::open(&path).unwrap();
            db.insert_raw(
                "asset",
                0,
                Some((1920, 1080)),
                Some(15320),
                Some(HashMode::HalfMd5),
                &[1, 2, 3, 4, 5, 6, 7, 8],
            )
            .unwrap();
        }
        {
            let db = ThumbDb::open(&path).unwrap();
            let record = db.get_record("asset").unwrap();
            assert_eq!(record.width, 1920);
            assert_eq!(record.height, 1080);
            assert_eq!(record.duration, 15320);
            assert_eq!(record.meta.hash_mode, HashMode::HalfMd5);
            assert_eq!(record.hash, vec![1, 2, 3, 4, 5, 6, 7, 8]);
        }
        cleanup_db(&path);
    }

    #[test]
    fn wal_chunks_apply_and_compact() {
        let path = temp_db_path("wal");
        {
            let db = ThumbDb::open(&path).unwrap();
            db.set_async("wal_key", "C:\\photos\\wal.jpg").unwrap();
            assert_eq!(db.get("wal_key"), None);
            db.process_wal_chunks().unwrap();
            assert_eq!(db.get("wal_key").as_deref(), Some("C:\\photos\\wal.jpg"));
            db.compact().unwrap();
        }
        {
            let db = ThumbDb::open(&path).unwrap();
            assert_eq!(db.get("wal_key").as_deref(), Some("C:\\photos\\wal.jpg"));
        }
        cleanup_db(&path);
    }

    #[test]
    fn delete_non_numeric_key_is_persistent() {
        let path = temp_db_path("delete-string");
        {
            let db = ThumbDb::open(&path).unwrap();
            db.set("abc", "C:\\photos\\abc.jpg").unwrap();
            assert!(db.delete("abc").unwrap());
        }
        {
            let db = ThumbDb::open(&path).unwrap();
            assert_eq!(db.get("abc"), None);
        }
        cleanup_db(&path);
    }
}
