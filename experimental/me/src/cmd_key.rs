use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

use crate::storage::ServerDb;

pub async fn cmd_del(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    let mut count = 0;
    for k in args {
        if database.remove(k).is_some() {
            count += 1;
        }
    }
    format!(":{}\r\n", count).into_bytes()
}

pub async fn cmd_exists(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    let mut count = 0;
    for k in args {
        if database.get_mut(k).is_some() {
            count += 1;
        }
    }
    format!(":{}\r\n", count).into_bytes()
}

pub async fn cmd_expire(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 2 {
        return b"-ERR wrong number of arguments for 'expire' command\r\n".to_vec();
    }
    let key = &args[0];
    let secs = parse_u64(&args[1]).unwrap_or(0);

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    if let Some(entry) = database.get_mut(key) {
        entry.expires_at = Some(Instant::now() + Duration::from_secs(secs));
        return b":1\r\n".to_vec();
    }
    b":0\r\n".to_vec()
}

pub async fn cmd_expireat(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 2 {
        return b"-ERR wrong number of arguments for 'expireat' command\r\n".to_vec();
    }
    let key = &args[0];
    let ts = parse_i64(&args[1]).unwrap_or(0);

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    if let Some(entry) = database.get_mut(key) {
        let now = now_secs();
        if ts <= now {
            entry.expires_at = Some(Instant::now());
        } else {
            entry.expires_at = Some(Instant::now() + Duration::from_secs((ts - now) as u64));
        }
        return b":1\r\n".to_vec();
    }
    b":0\r\n".to_vec()
}

pub async fn cmd_pexpire(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 2 {
        return b"-ERR wrong number of arguments for 'pexpire' command\r\n".to_vec();
    }
    let key = &args[0];
    let ms = parse_u64(&args[1]).unwrap_or(0);

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    if let Some(entry) = database.get_mut(key) {
        entry.expires_at = Some(Instant::now() + Duration::from_millis(ms));
        return b":1\r\n".to_vec();
    }
    b":0\r\n".to_vec()
}

pub async fn cmd_pexpireat(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 2 {
        return b"-ERR wrong number of arguments for 'pexpireat' command\r\n".to_vec();
    }
    let key = &args[0];
    let ts_ms = parse_i64(&args[1]).unwrap_or(0);

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    if let Some(entry) = database.get_mut(key) {
        let now = now_ms();
        if ts_ms <= now {
            entry.expires_at = Some(Instant::now());
        } else {
            entry.expires_at = Some(Instant::now() + Duration::from_millis((ts_ms - now) as u64));
        }
        return b":1\r\n".to_vec();
    }
    b":0\r\n".to_vec()
}

pub async fn cmd_persist(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.is_empty() {
        return b"-ERR wrong number of arguments for 'persist' command\r\n".to_vec();
    }
    let key = &args[0];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    if let Some(entry) = database.get_mut(key) {
        if entry.expires_at.is_some() {
            entry.expires_at = None;
            return b":1\r\n".to_vec();
        }
        return b":0\r\n".to_vec();
    }
    b":0\r\n".to_vec()
}

pub async fn cmd_ttl(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.is_empty() {
        return b"-ERR wrong number of arguments for 'ttl' command\r\n".to_vec();
    }
    let key = &args[0];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    if let Some(entry) = database.get_mut(key) {
        match entry.expires_at {
            Some(exp) => {
                let remaining = exp.saturating_duration_since(Instant::now());
                return format!(":{}\r\n", remaining.as_secs() as i64).into_bytes();
            }
            None => return b":-1\r\n".to_vec(),
        }
    }
    b":-2\r\n".to_vec()
}

pub async fn cmd_pttl(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.is_empty() {
        return b"-ERR wrong number of arguments for 'pttl' command\r\n".to_vec();
    }
    let key = &args[0];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    if let Some(entry) = database.get_mut(key) {
        match entry.expires_at {
            Some(exp) => {
                let remaining = exp.saturating_duration_since(Instant::now());
                return format!(":{}\r\n", remaining.as_millis() as i64).into_bytes();
            }
            None => return b":-1\r\n".to_vec(),
        }
    }
    b":-2\r\n".to_vec()
}

pub async fn cmd_type(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.is_empty() {
        return b"-ERR wrong number of arguments for 'type' command\r\n".to_vec();
    }
    let key = &args[0];
    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    let type_name = match database.get_mut(key) {
        Some(entry) => entry.value.type_name(),
        None => "none",
    };
    format!("+{}\r\n", type_name).into_bytes()
}

pub async fn cmd_keys(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.is_empty() {
        return b"-ERR wrong number of arguments for 'keys' command\r\n".to_vec();
    }
    let pattern = &args[0];

    let server = db.lock().await;
    let database = &server.databases[dbi];
    let mut matched: Vec<Vec<u8>> = Vec::new();

    for (key, entry) in database.store.iter() {
        if entry.is_expired() {
            continue;
        }
        if wildcard_match(pattern, key) {
            matched.push(key.clone());
        }
    }

    let mut resp = format!("*{}\r\n", matched.len()).into_bytes();
    for key in matched {
        resp.extend_from_slice(&format!("${}\r\n", key.len()).into_bytes());
        resp.extend_from_slice(&key);
        resp.extend_from_slice(b"\r\n");
    }
    resp
}

pub async fn cmd_rename(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 2 {
        return b"-ERR wrong number of arguments for 'rename' command\r\n".to_vec();
    }
    let key = &args[0];
    let newkey = &args[1];

    if key == newkey {
        return b"-ERR source and destination objects are the same\r\n".to_vec();
    }

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];

    if database.get(key).map(|e| e.is_expired()).unwrap_or(true) {
        return b"-ERR no such key\r\n".to_vec();
    }

    let entry = database.remove(key);
    if let Some(entry) = entry {
        database.store.insert(newkey.to_vec(), entry);
    }
    b"+OK\r\n".to_vec()
}

pub async fn cmd_renamenx(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 2 {
        return b"-ERR wrong number of arguments for 'renamenx' command\r\n".to_vec();
    }
    let key = &args[0];
    let newkey = &args[1];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];

    if database.get(key).map(|e| e.is_expired()).unwrap_or(true) {
        return b"-ERR no such key\r\n".to_vec();
    }

    if database.get(newkey).is_some() && !database.get(newkey).unwrap().is_expired() {
        return b":0\r\n".to_vec();
    }

    let entry = database.remove(key);
    if let Some(entry) = entry {
        database.store.insert(newkey.to_vec(), entry);
    }
    b":1\r\n".to_vec()
}

pub async fn cmd_copy(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 2 {
        return b"-ERR wrong number of arguments for 'copy' command\r\n".to_vec();
    }
    let source = &args[0];
    let dest = &args[1];
    let mut target_db = dbi;
    let mut replace = false;

    let mut idx = 2;
    while idx < args.len() {
        if args[idx].eq_ignore_ascii_case(b"DB") && idx + 1 < args.len() {
            if let Ok(db_idx) = parse_usize(&args[idx + 1]) {
                if db_idx < 16 {
                    target_db = db_idx;
                }
            }
            idx += 2;
        } else if args[idx].eq_ignore_ascii_case(b"REPLACE") {
            replace = true;
            idx += 1;
        } else {
            idx += 1;
        }
    }

    if source == dest && dbi == target_db {
        return b"-ERR source and destination objects are the same\r\n".to_vec();
    }

    let mut server = db.lock().await;
    let source_val = match server.databases[dbi].get(source) {
        Some(entry) if !entry.is_expired() => entry.value.clone(),
        _ => return b":0\r\n".to_vec(),
    };

    let source_exp = server.databases[dbi].get(source).and_then(|e| e.expires_at);

    if !replace && server.databases[target_db].get(dest).is_some() && !server.databases[target_db].get(dest).unwrap().is_expired() {
        return b":0\r\n".to_vec();
    }

    server.databases[target_db].store.insert(dest.to_vec(), crate::storage::Entry::new(source_val, source_exp));
    b":1\r\n".to_vec()
}

pub fn wildcard_match(pattern: &[u8], text: &[u8]) -> bool {
    let mut p = 0;
    let mut t = 0;
    let mut star_idx = None;
    let mut match_idx = 0;

    while t < text.len() {
        if p < pattern.len() && (pattern[p] == b'?' || pattern[p] == text[t]) {
            p += 1;
            t += 1;
        } else if p < pattern.len() && pattern[p] == b'*' {
            star_idx = Some(p);
            match_idx = t;
            p += 1;
        } else if let Some(star) = star_idx {
            p = star + 1;
            match_idx += 1;
            t = match_idx;
        } else {
            return false;
        }
    }

    while p < pattern.len() && pattern[p] == b'*' {
        p += 1;
    }
    p == pattern.len()
}

fn parse_u64(data: &[u8]) -> Result<u64, ()> {
    std::str::from_utf8(data).map_err(|_| ())?.parse::<u64>().map_err(|_| ())
}

fn parse_i64(data: &[u8]) -> Result<i64, ()> {
    std::str::from_utf8(data).map_err(|_| ())?.parse::<i64>().map_err(|_| ())
}

fn parse_usize(data: &[u8]) -> Result<usize, ()> {
    std::str::from_utf8(data).map_err(|_| ())?.parse::<usize>().map_err(|_| ())
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
