use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::storage::{ServerDb, Value};

pub async fn cmd_hset(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 3 || args.len() % 2 != 1 {
        return b"-ERR wrong number of arguments for 'hset' command\r\n".to_vec();
    }
    let key = &args[0];
    let fields: &[Vec<u8>] = &args[1..];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &mut entry.value {
            Value::Hash(hash) => {
                let mut added = 0;
                for chunk in fields.chunks(2) {
                    if !hash.contains_key(&chunk[0]) {
                        added += 1;
                    }
                    hash.insert(chunk[0].clone(), chunk[1].clone());
                }
                format!(":{}\r\n", added).into_bytes()
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => {
            let mut hash = BTreeMap::new();
            for chunk in fields.chunks(2) {
                hash.insert(chunk[0].clone(), chunk[1].clone());
            }
            let added = hash.len();
            database.insert(key.to_vec(), Value::Hash(hash), None);
            format!(":{}\r\n", added).into_bytes()
        }
    }
}

pub async fn cmd_hget(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 2 {
        return b"-ERR wrong number of arguments for 'hget' command\r\n".to_vec();
    }
    let key = &args[0];
    let field = &args[1];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &entry.value {
            Value::Hash(hash) => match hash.get(field) {
                Some(v) => {
                    let mut resp = format!("${}\r\n", v.len()).into_bytes();
                    resp.extend_from_slice(v);
                    resp.extend_from_slice(b"\r\n");
                    resp
                }
                None => b"$-1\r\n".to_vec(),
            },
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => b"$-1\r\n".to_vec(),
    }
}

pub async fn cmd_hdel(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 2 {
        return b"-ERR wrong number of arguments for 'hdel' command\r\n".to_vec();
    }
    let key = &args[0];
    let fields = &args[1..];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &mut entry.value {
            Value::Hash(hash) => {
                let mut deleted = 0;
                for f in fields {
                    if hash.remove(f).is_some() {
                        deleted += 1;
                    }
                }
                if hash.is_empty() {
                    database.remove(key);
                }
                format!(":{}\r\n", deleted).into_bytes()
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => b":0\r\n".to_vec(),
    }
}

pub async fn cmd_hexists(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 2 {
        return b"-ERR wrong number of arguments for 'hexists' command\r\n".to_vec();
    }
    let key = &args[0];
    let field = &args[1];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &entry.value {
            Value::Hash(hash) => {
                if hash.contains_key(field) { b":1\r\n".to_vec() } else { b":0\r\n".to_vec() }
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => b":0\r\n".to_vec(),
    }
}

pub async fn cmd_hkeys(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.is_empty() {
        return b"-ERR wrong number of arguments for 'hkeys' command\r\n".to_vec();
    }
    let key = &args[0];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &entry.value {
            Value::Hash(hash) => {
                let mut resp = format!("*{}\r\n", hash.len()).into_bytes();
                for k in hash.keys() {
                    resp.extend_from_slice(&format!("${}\r\n", k.len()).into_bytes());
                    resp.extend_from_slice(k);
                    resp.extend_from_slice(b"\r\n");
                }
                resp
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => b"*0\r\n".to_vec(),
    }
}

pub async fn cmd_hvals(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.is_empty() {
        return b"-ERR wrong number of arguments for 'hvals' command\r\n".to_vec();
    }
    let key = &args[0];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &entry.value {
            Value::Hash(hash) => {
                let mut resp = format!("*{}\r\n", hash.len()).into_bytes();
                for v in hash.values() {
                    resp.extend_from_slice(&format!("${}\r\n", v.len()).into_bytes());
                    resp.extend_from_slice(v);
                    resp.extend_from_slice(b"\r\n");
                }
                resp
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => b"*0\r\n".to_vec(),
    }
}

pub async fn cmd_hgetall(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.is_empty() {
        return b"-ERR wrong number of arguments for 'hgetall' command\r\n".to_vec();
    }
    let key = &args[0];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &entry.value {
            Value::Hash(hash) => {
                let count = hash.len() * 2;
                let mut resp = format!("*{}\r\n", count).into_bytes();
                for (k, v) in hash {
                    resp.extend_from_slice(&format!("${}\r\n", k.len()).into_bytes());
                    resp.extend_from_slice(k);
                    resp.extend_from_slice(b"\r\n");
                    resp.extend_from_slice(&format!("${}\r\n", v.len()).into_bytes());
                    resp.extend_from_slice(v);
                    resp.extend_from_slice(b"\r\n");
                }
                resp
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => b"*0\r\n".to_vec(),
    }
}

pub async fn cmd_hlen(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.is_empty() {
        return b"-ERR wrong number of arguments for 'hlen' command\r\n".to_vec();
    }
    let key = &args[0];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &entry.value {
            Value::Hash(hash) => format!(":{}\r\n", hash.len()).into_bytes(),
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => b":0\r\n".to_vec(),
    }
}

pub async fn cmd_hincrby(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 3 {
        return b"-ERR wrong number of arguments for 'hincrby' command\r\n".to_vec();
    }
    let key = &args[0];
    let field = &args[1];
    let increment = parse_i64(&args[2]).unwrap_or(0);

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &mut entry.value {
            Value::Hash(hash) => {
                let current = hash.get(field).and_then(|v| String::from_utf8_lossy(v).trim().parse::<i64>().ok()).unwrap_or(0);
                match current.checked_add(increment) {
                    Some(new_val) => {
                        hash.insert(field.clone(), new_val.to_string().into_bytes());
                        format!(":{}\r\n", new_val).into_bytes()
                    }
                    None => b"-ERR increment would overflow\r\n".to_vec(),
                }
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => {
            let mut hash = BTreeMap::new();
            hash.insert(field.clone(), increment.to_string().into_bytes());
            database.insert(key.to_vec(), Value::Hash(hash), None);
            format!(":{}\r\n", increment).into_bytes()
        }
    }
}

pub async fn cmd_hincrbyfloat(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 3 {
        return b"-ERR wrong number of arguments for 'hincrbyfloat' command\r\n".to_vec();
    }
    let key = &args[0];
    let field = &args[1];
    let increment: f64 = match parse_f64(&args[2]) {
        Some(f) => f,
        None => return b"-ERR value is not a valid float\r\n".to_vec(),
    };

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &mut entry.value {
            Value::Hash(hash) => {
                let current: f64 = hash.get(field)
                    .and_then(|v| String::from_utf8_lossy(v).trim().parse::<f64>().ok())
                    .unwrap_or(0.0);
                let new_val = current + increment;
                if !new_val.is_finite() {
                    return b"-ERR increment would produce NaN or Infinity\r\n".to_vec();
                }
                let formatted = crate::cmd_string::format_f64_public(new_val);
                hash.insert(field.clone(), formatted.as_bytes().to_vec());
                let mut resp = format!("${}\r\n", formatted.len()).into_bytes();
                resp.extend_from_slice(formatted.as_bytes());
                resp.extend_from_slice(b"\r\n");
                resp
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => {
            let mut hash = BTreeMap::new();
            let formatted = crate::cmd_string::format_f64_public(increment);
            hash.insert(field.clone(), formatted.as_bytes().to_vec());
            database.insert(key.to_vec(), Value::Hash(hash), None);
            let mut resp = format!("${}\r\n", formatted.len()).into_bytes();
            resp.extend_from_slice(formatted.as_bytes());
            resp.extend_from_slice(b"\r\n");
            resp
        }
    }
}

pub async fn cmd_hmset(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 3 || args.len() % 2 != 1 {
        return b"-ERR wrong number of arguments for 'hmset' command\r\n".to_vec();
    }
    let key = &args[0];
    let fields: &[Vec<u8>] = &args[1..];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &mut entry.value {
            Value::Hash(hash) => {
                for chunk in fields.chunks(2) {
                    hash.insert(chunk[0].clone(), chunk[1].clone());
                }
            }
            _ => return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => {
            let mut hash = BTreeMap::new();
            for chunk in fields.chunks(2) {
                hash.insert(chunk[0].clone(), chunk[1].clone());
            }
            database.insert(key.to_vec(), Value::Hash(hash), None);
        }
    }
    b"+OK\r\n".to_vec()
}

pub async fn cmd_hmget(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 2 {
        return b"-ERR wrong number of arguments for 'hmget' command\r\n".to_vec();
    }
    let key = &args[0];
    let fields = &args[1..];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &entry.value {
            Value::Hash(hash) => {
                let mut resp = format!("*{}\r\n", fields.len()).into_bytes();
                for f in fields {
                    match hash.get(f) {
                        Some(v) => {
                            resp.extend_from_slice(&format!("${}\r\n", v.len()).into_bytes());
                            resp.extend_from_slice(v);
                            resp.extend_from_slice(b"\r\n");
                        }
                        None => resp.extend_from_slice(b"$-1\r\n"),
                    }
                }
                resp
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => {
            let mut resp = format!("*{}\r\n", fields.len()).into_bytes();
            for _ in fields {
                resp.extend_from_slice(b"$-1\r\n");
            }
            resp
        }
    }
}

pub async fn cmd_hsetnx(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 3 {
        return b"-ERR wrong number of arguments for 'hsetnx' command\r\n".to_vec();
    }
    let key = &args[0];
    let field = &args[1];
    let value = &args[2];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &mut entry.value {
            Value::Hash(hash) => {
                if hash.contains_key(field) {
                    return b":0\r\n".to_vec();
                }
                hash.insert(field.clone(), value.clone());
                b":1\r\n".to_vec()
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => {
            let mut hash = BTreeMap::new();
            hash.insert(field.clone(), value.clone());
            database.insert(key.to_vec(), Value::Hash(hash), None);
            b":1\r\n".to_vec()
        }
    }
}

pub async fn cmd_hscan(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.is_empty() {
        return b"-ERR wrong number of arguments for 'hscan' command\r\n".to_vec();
    }
    let key = &args[0];
    let cursor: usize = if args.len() >= 2 { parse_u64(&args[1]).unwrap_or(0) as usize } else { 0 };

    let mut match_pattern: Option<Vec<u8>> = None;
    let mut count = 10usize;

    let mut idx = 2;
    while idx < args.len() {
        if args[idx].eq_ignore_ascii_case(b"MATCH") && idx + 1 < args.len() {
            match_pattern = Some(args[idx + 1].clone());
            idx += 2;
        } else if args[idx].eq_ignore_ascii_case(b"COUNT") && idx + 1 < args.len() {
            if let Ok(c) = parse_u64(&args[idx + 1]) {
                count = c as usize;
            }
            idx += 2;
        } else {
            idx += 1;
        }
    }

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &entry.value {
            Value::Hash(hash) => {
                let mut matched: Vec<(&Vec<u8>, &Vec<u8>)> = Vec::new();
                let mut skipped = 0usize;
                for (i, (k, v)) in hash.iter().enumerate() {
                    if i < cursor {
                        skipped += 1;
                        continue;
                    }
                    let is_match = match &match_pattern {
                        Some(pattern) => crate::cmd_key::wildcard_match(pattern, k),
                        None => true,
                    };
                    if is_match {
                        matched.push((k, v));
                        if matched.len() >= count {
                            break;
                        }
                    }
                }

                let traversed = cursor + matched.len() + skipped;
                let total = hash.len();
                let next_cursor = if traversed >= total { 0 } else { traversed };

                let mut resp = format!("*2\r\n${}\r\n{}\r\n*{}\r\n", next_cursor.to_string().len(), next_cursor, matched.len() * 2).into_bytes();
                for (k, v) in matched {
                    resp.extend_from_slice(&format!("${}\r\n", k.len()).into_bytes());
                    resp.extend_from_slice(k);
                    resp.extend_from_slice(b"\r\n");
                    resp.extend_from_slice(&format!("${}\r\n", v.len()).into_bytes());
                    resp.extend_from_slice(v);
                    resp.extend_from_slice(b"\r\n");
                }
                resp
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => b"*2\r\n$1\r\n0\r\n*0\r\n".to_vec(),
    }
}

pub async fn cmd_hstrlen(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 2 {
        return b"-ERR wrong number of arguments for 'hstrlen' command\r\n".to_vec();
    }
    let key = &args[0];
    let field = &args[1];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &entry.value {
            Value::Hash(hash) => match hash.get(field) {
                Some(v) => format!(":{}\r\n", v.len()).into_bytes(),
                None => b":0\r\n".to_vec(),
            },
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => b":0\r\n".to_vec(),
    }
}

fn parse_u64(data: &[u8]) -> Result<u64, ()> {
    std::str::from_utf8(data).map_err(|_| ())?.parse::<u64>().map_err(|_| ())
}

fn parse_i64(data: &[u8]) -> Result<i64, ()> {
    std::str::from_utf8(data).map_err(|_| ())?.parse::<i64>().map_err(|_| ())
}

fn parse_f64(data: &[u8]) -> Option<f64> {
    std::str::from_utf8(data).ok()?.trim().parse::<f64>().ok()
}
