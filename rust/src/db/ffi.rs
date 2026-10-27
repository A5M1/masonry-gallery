#![allow(clippy::missing_safety_doc, clippy::collapsible_match)]

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_uchar, c_uint, c_ulonglong, c_void};
use std::sync::Arc;

use super::format::HashMode;
use super::{find_registered, open_for_dir_registered, release_registered, ThumbDb};

const DB_OK: c_int = 0;
const DB_ERROR: c_int = -1;
const DB_NOT_FOUND: c_int = -2;
const DB_INVALID: c_int = -4;
const DB_MAX_PATH: usize = 4096;

#[repr(C)]
pub struct DbRecord {
    pub filename: [c_char; DB_MAX_PATH],
    pub meta_byte: u8,
    pub timestamp_delta: i64,
    pub width: u32,
    pub height: u32,
    pub duration: u64,
    pub hash_mode: u8,
    pub hash_len: usize,
    pub hash: [u8; 32],
}

unsafe extern "C" {
    fn malloc(size: usize) -> *mut c_void;
}

unsafe fn string_from_ptr(ptr: *const c_char) -> Option<String> {
    if ptr.is_null() {
        None
    } else {
        Some(CStr::from_ptr(ptr).to_string_lossy().into_owned())
    }
}

unsafe fn db_ref<'a>(ptr: *mut ThumbDb) -> Option<&'a ThumbDb> {
    if ptr.is_null() {
        None
    } else {
        Some(&*ptr)
    }
}

unsafe fn borrowed_arc(ptr: *mut ThumbDb) -> Option<Arc<ThumbDb>> {
    if ptr.is_null() {
        None
    } else {
        Arc::increment_strong_count(ptr);
        Some(Arc::from_raw(ptr))
    }
}

unsafe fn write_c_string(value: &str, out: *mut c_char, out_len: usize) -> bool {
    if out.is_null() || out_len == 0 {
        return false;
    }
    let bytes = value.as_bytes();
    let copy_len = bytes.len().min(out_len.saturating_sub(1));
    std::ptr::copy_nonoverlapping(bytes.as_ptr(), out as *mut u8, copy_len);
    *out.add(copy_len) = 0;
    true
}

#[no_mangle]
pub unsafe extern "C" fn db_open(path: *const c_char) -> *mut c_void {
    let Some(path) = string_from_ptr(path) else {
        return std::ptr::null_mut();
    };
    match open_for_dir_registered(&path) {
        Ok(db) => Arc::into_raw(db) as *mut c_void,
        Err(_) => std::ptr::null_mut(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn db_close(db: *mut c_void) {
    if db.is_null() {
        return;
    }
    let db = Arc::from_raw(db as *const ThumbDb);
    release_registered(&db);
}

#[no_mangle]
pub unsafe extern "C" fn db_insert(
    db: *mut c_void,
    filename: *const c_char,
    meta_byte: c_uchar,
    width: c_uint,
    height: c_uint,
    duration: c_ulonglong,
    hash_mode: u8,
    hash: *const c_uchar,
    hash_len: usize,
) -> c_int {
    let Some(db) = db_ref(db as *mut ThumbDb) else {
        return DB_ERROR;
    };
    let Some(filename) = string_from_ptr(filename) else {
        return DB_ERROR;
    };
    let mut hash_data = Vec::new();
    if hash_len > 0 {
        if hash.is_null() {
            return DB_INVALID;
        }
        hash_data.extend_from_slice(std::slice::from_raw_parts(hash, hash_len));
    }
    let dimensions = if width > 0 && height > 0 {
        Some((width, height))
    } else {
        None
    };
    let mode = Some(HashMode::from_bits(hash_mode));
    match db.insert_raw(
        &filename,
        meta_byte,
        dimensions,
        Some(duration),
        mode,
        &hash_data,
    ) {
        Ok(()) => DB_OK,
        Err(_) => DB_ERROR,
    }
}

#[no_mangle]
pub unsafe extern "C" fn db_insert_path(
    db: *mut c_void,
    key: *const c_char,
    media_path: *const c_char,
) -> c_int {
    let Some(db) = db_ref(db as *mut ThumbDb) else {
        return DB_ERROR;
    };
    let Some(key) = string_from_ptr(key) else {
        return DB_ERROR;
    };
    let Some(media_path) = string_from_ptr(media_path) else {
        return DB_ERROR;
    };
    match db.set(&key, &media_path) {
        Ok(()) => DB_OK,
        Err(_) => DB_ERROR,
    }
}

#[no_mangle]
pub unsafe extern "C" fn db_set(
    db: *mut c_void,
    key: *const c_char,
    media_path: *const c_char,
) -> c_int {
    db_insert_path(db, key, media_path)
}

#[no_mangle]
pub unsafe extern "C" fn db_get(
    db: *mut c_void,
    filename: *const c_char,
    out_record: *mut DbRecord,
) -> c_int {
    let Some(db) = db_ref(db as *mut ThumbDb) else {
        return DB_ERROR;
    };
    let Some(filename) = string_from_ptr(filename) else {
        return DB_ERROR;
    };
    if out_record.is_null() {
        return DB_ERROR;
    }
    let Some(record) = db.get_record(&filename) else {
        return DB_NOT_FOUND;
    };
    let out = &mut *out_record;
    out.filename = [0; DB_MAX_PATH];
    let name_bytes = record.filename.as_bytes();
    let name_len = name_bytes.len().min(DB_MAX_PATH - 1);
    for (index, byte) in name_bytes[..name_len].iter().enumerate() {
        out.filename[index] = *byte as c_char;
    }
    out.meta_byte = record.meta.encode();
    out.timestamp_delta = record.timestamp as i64;
    out.width = record.width;
    out.height = record.height;
    out.duration = u64::from(record.duration);
    out.hash_mode = record.meta.hash_mode as u8;
    out.hash_len = record.hash_len.min(32);
    out.hash = [0u8; 32];
    let copy_len = record.hash.len().min(out.hash_len);
    out.hash[..copy_len].copy_from_slice(&record.hash[..copy_len]);
    DB_OK
}

#[no_mangle]
pub unsafe extern "C" fn db_delete(db: *mut c_void, filename: *const c_char) -> c_int {
    let Some(db) = db_ref(db as *mut ThumbDb) else {
        return DB_ERROR;
    };
    let Some(filename) = string_from_ptr(filename) else {
        return DB_ERROR;
    };
    match db.delete(&filename) {
        Ok(true) => DB_OK,
        Ok(false) => DB_NOT_FOUND,
        Err(_) => DB_ERROR,
    }
}

#[no_mangle]
pub unsafe extern "C" fn db_begin_tx(db: *mut c_void) -> c_int {
    let Some(db) = db_ref(db as *mut ThumbDb) else {
        return DB_ERROR;
    };
    match db.tx_begin() {
        Ok(()) => DB_OK,
        Err(_) => DB_ERROR,
    }
}

#[no_mangle]
pub unsafe extern "C" fn db_commit(db: *mut c_void) -> c_int {
    let Some(db) = db_ref(db as *mut ThumbDb) else {
        return DB_ERROR;
    };
    match db.tx_commit() {
        Ok(()) => DB_OK,
        Err(_) => DB_ERROR,
    }
}

#[no_mangle]
pub unsafe extern "C" fn db_rollback(db: *mut c_void) -> c_int {
    let Some(db) = db_ref(db as *mut ThumbDb) else {
        return DB_ERROR;
    };
    match db.tx_abort() {
        Ok(()) => DB_OK,
        Err(_) => DB_ERROR,
    }
}

#[no_mangle]
pub unsafe extern "C" fn db_count(db: *mut c_void) -> c_int {
    let Some(db) = db_ref(db as *mut ThumbDb) else {
        return DB_ERROR;
    };
    db.count() as c_int
}

#[no_mangle]
pub unsafe extern "C" fn db_sync(db: *mut c_void) -> c_int {
    let Some(db) = db_ref(db as *mut ThumbDb) else {
        return DB_ERROR;
    };
    match db.sync() {
        Ok(()) => DB_OK,
        Err(_) => DB_ERROR,
    }
}

#[no_mangle]
pub extern "C" fn thumbdb_open() -> c_int {
    let path = std::env::var("GALLERIA_THUMBDB")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "thumbs.tdb".to_string());
    match open_for_dir_registered(&path) {
        Ok(_) => 0,
        Err(_) => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn thumbdb_open_for_dir(gallery_dir: *const c_char) -> *mut ThumbDb {
    let Some(path) = string_from_ptr(gallery_dir) else {
        return std::ptr::null_mut();
    };
    match open_for_dir_registered(&path) {
        Ok(db) => Arc::into_raw(db) as *mut ThumbDb,
        Err(_) => std::ptr::null_mut(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn thumbdb_close(inst: *mut ThumbDb) {
    if inst.is_null() {
        return;
    }
    let db = Arc::from_raw(inst as *const ThumbDb);
    release_registered(&db);
}

#[no_mangle]
pub unsafe extern "C" fn thumbdb_set(
    inst: *mut ThumbDb,
    key: *const c_char,
    value: *const c_char,
) -> c_int {
    let Some(db) = db_ref(inst) else {
        return -1;
    };
    let Some(key) = string_from_ptr(key) else {
        return -1;
    };
    let Some(value) = string_from_ptr(value) else {
        return -1;
    };
    match db.set(&key, &value) {
        Ok(()) => 0,
        Err(_) => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn thumbdb_get(
    inst: *mut ThumbDb,
    key: *const c_char,
    buf: *mut c_char,
    buflen: usize,
) -> c_int {
    let Some(db) = db_ref(inst) else {
        return -1;
    };
    let Some(key) = string_from_ptr(key) else {
        return -1;
    };
    if buf.is_null() || buflen == 0 {
        return -1;
    }
    match db.get(&key) {
        Some(value) => {
            if write_c_string(&value, buf, buflen) {
                0
            } else {
                -1
            }
        }
        None => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn thumbdb_find_for_media(
    inst: *mut ThumbDb,
    media_path: *const c_char,
    out_key: *mut c_char,
    out_key_len: usize,
) -> c_int {
    let Some(db) = db_ref(inst) else {
        return -1;
    };
    let Some(media_path) = string_from_ptr(media_path) else {
        return -1;
    };
    if out_key.is_null() || out_key_len == 0 {
        return -1;
    }
    match db.find_for_media(&media_path) {
        Some(key) => {
            if write_c_string(&key, out_key, out_key_len) {
                0
            } else {
                -1
            }
        }
        None => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn thumbdb_delete(inst: *mut ThumbDb, key: *const c_char) -> c_int {
    let Some(db) = db_ref(inst) else {
        return -1;
    };
    let Some(key) = string_from_ptr(key) else {
        return -1;
    };
    match db.delete(&key) {
        Ok(_) => 0,
        Err(_) => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn thumbdb_iterate(
    inst: *mut ThumbDb,
    callback: Option<extern "C" fn(*const c_char, *const c_char, *mut c_void)>,
    context: *mut c_void,
) {
    let Some(db) = db_ref(inst) else {
        return;
    };
    let Some(callback) = callback else {
        return;
    };
    for (key, value) in db.iterate() {
        let key_c = match CString::new(key) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let value_c = match CString::new(value) {
            Ok(value) => value,
            Err(_) => continue,
        };
        callback(key_c.as_ptr(), value_c.as_ptr(), context);
    }
}

#[no_mangle]
pub unsafe extern "C" fn thumbdb_compact(inst: *mut ThumbDb) -> c_int {
    let Some(db) = db_ref(inst) else {
        return -1;
    };
    match db.compact() {
        Ok(()) => 0,
        Err(_) => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn thumbdb_sweep_orphans(inst: *mut ThumbDb) -> c_int {
    let Some(db) = db_ref(inst) else {
        return -1;
    };
    match db.sweep_orphans() {
        Ok(_) => 0,
        Err(_) => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn thumbdb_tx_begin(inst: *mut ThumbDb) -> c_int {
    let Some(db) = db_ref(inst) else {
        return -1;
    };
    match db.tx_begin() {
        Ok(()) => 0,
        Err(_) => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn thumbdb_tx_commit(inst: *mut ThumbDb) -> c_int {
    let Some(db) = db_ref(inst) else {
        return -1;
    };
    match db.tx_commit() {
        Ok(()) => 0,
        Err(_) => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn thumbdb_tx_abort(inst: *mut ThumbDb) -> c_int {
    let Some(db) = db_ref(inst) else {
        return -1;
    };
    match db.tx_abort() {
        Ok(()) => 0,
        Err(_) => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn thumbdb_request_compaction(inst: *mut ThumbDb) {
    if let Some(db) = db_ref(inst) {
        db.request_compaction();
    }
}

#[no_mangle]
pub unsafe extern "C" fn thumbdb_perform_requested_compaction(inst: *mut ThumbDb) -> c_int {
    let Some(db) = db_ref(inst) else {
        return -1;
    };
    db.perform_requested_compaction()
}

#[no_mangle]
pub unsafe extern "C" fn thumbdb_start_repair_task(
    inst: *mut ThumbDb,
    interval_seconds: c_int,
) -> c_int {
    let Some(db) = borrowed_arc(inst) else {
        return -1;
    };
    db.start_repair_task(if interval_seconds > 0 {
        interval_seconds as u64
    } else {
        3600
    });
    0
}

#[no_mangle]
pub unsafe extern "C" fn thumbdb_start_async_worker(inst: *mut ThumbDb) -> c_int {
    let Some(db) = borrowed_arc(inst) else {
        return -1;
    };
    db.start_async_worker();
    0
}

#[no_mangle]
pub unsafe extern "C" fn thumbdb_stop_async_worker(inst: *mut ThumbDb) {
    if let Some(db) = db_ref(inst) {
        db.stop_async_worker();
    }
}

#[no_mangle]
pub unsafe extern "C" fn thumbdb_set_async(
    inst: *mut ThumbDb,
    key: *const c_char,
    value: *const c_char,
) -> c_int {
    let Some(db) = db_ref(inst) else {
        return -1;
    };
    let Some(key) = string_from_ptr(key) else {
        return -1;
    };
    let Some(value) = string_from_ptr(value) else {
        return -1;
    };
    match db.set_async(&key, &value) {
        Ok(()) => 0,
        Err(_) => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn thumbdb_delete_async(inst: *mut ThumbDb, key: *const c_char) -> c_int {
    let Some(db) = db_ref(inst) else {
        return -1;
    };
    let Some(key) = string_from_ptr(key) else {
        return -1;
    };
    match db.delete_async(&key) {
        Ok(()) => 0,
        Err(_) => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn thumbdb_validate(inst: *mut ThumbDb) -> c_int {
    let Some(db) = db_ref(inst) else {
        return -1;
    };
    db.validate()
}

#[no_mangle]
pub unsafe extern "C" fn thumbdb_verify_thumbnails(inst: *mut ThumbDb) -> c_int {
    let Some(db) = db_ref(inst) else {
        return -1;
    };
    match db.verify_thumbnails() {
        Ok((_total, _valid, orphaned)) => {
            if orphaned > 0 {
                1
            } else {
                0
            }
        }
        Err(_) => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn thumbdb_verify_records(inst: *mut ThumbDb) -> c_int {
    let Some(db) = db_ref(inst) else {
        return -1;
    };
    match db.verify_records() {
        Ok((_total, _valid, missing)) => {
            if missing > 0 {
                1
            } else {
                0
            }
        }
        Err(_) => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn thumbdb_seek_to_record(
    inst: *mut ThumbDb,
    filename: *const c_char,
    buf: *mut c_char,
    buflen: usize,
) -> c_int {
    let Some(db) = db_ref(inst) else {
        return -1;
    };
    let Some(filename) = string_from_ptr(filename) else {
        return -1;
    };
    if buf.is_null() || buflen == 0 {
        return -1;
    }
    match db.seek_to_record(&filename) {
        Some(value) => {
            if write_c_string(&value, buf, buflen) {
                0
            } else {
                -1
            }
        }
        None => -1,
    }
}

#[no_mangle]
pub unsafe extern "C" fn thumbdb_get_record_detail(
    inst: *mut ThumbDb,
    key: *const c_char,
) -> *mut c_char {
    let Some(db) = db_ref(inst) else {
        return std::ptr::null_mut();
    };
    let Some(key) = string_from_ptr(key) else {
        return std::ptr::null_mut();
    };
    let Some(detail) = db.get_record_detail(&key) else {
        return std::ptr::null_mut();
    };
    let detail = match CString::new(detail) {
        Ok(value) => value,
        Err(_) => return std::ptr::null_mut(),
    };
    let bytes = detail.as_bytes_with_nul();
    let memory = malloc(bytes.len()) as *mut u8;
    if memory.is_null() {
        return std::ptr::null_mut();
    }
    std::ptr::copy_nonoverlapping(bytes.as_ptr(), memory, bytes.len());
    memory as *mut c_char
}

#[no_mangle]
pub unsafe extern "C" fn thumbdb_find_instance(db_path: *const c_char) -> *mut ThumbDb {
    let Some(path) = string_from_ptr(db_path) else {
        return std::ptr::null_mut();
    };
    match find_registered(&path) {
        Some(db) => Arc::as_ptr(&db) as *mut ThumbDb,
        None => std::ptr::null_mut(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn thumbdb_process_wal_chunks(inst: *mut ThumbDb) -> c_int {
    let Some(db) = db_ref(inst) else {
        return -1;
    };
    match db.process_wal_chunks() {
        Ok(()) => 0,
        Err(_) => -1,
    }
}
