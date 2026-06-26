use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

use crate::storage::{ServerDb, Value};

pub async fn cmd_set(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 2 {
        return b"-ERR wrong number of arguments for 'set' command\r\n".to_vec();
    }

    let key = &args[0];
    let value = &args[1];
    let mut ttl: Option<Duration> = None;
    let mut nx = false;
    let mut xx = false;
    let mut get_flag = false;
    let mut keepttl = false;

    let mut idx = 2;
    while idx < args.len() {
        let opt = &args[idx];
        if opt.eq_ignore_ascii_case(b"EX") && idx + 1 < args.len() {
            if let Ok(secs) = parse_u64(&args[idx + 1]) {
                ttl = Some(Duration::from_secs(secs));
            }
            idx += 2;
        } else if opt.eq_ignore_ascii_case(b"PX") && idx + 1 < args.len() {
            if let Ok(ms) = parse_u64(&args[idx + 1]) {
                ttl = Some(Duration::from_millis(ms));
            }
            idx += 2;
        } else if opt.eq_ignore_ascii_case(b"EXAT") && idx + 1 < args.len() {
            if let Ok(ts) = parse_u64(&args[idx + 1]) {
                let now = Instant::now();
                let target = std::time::UNIX_EPOCH + Duration::from_secs(ts);
                let _elapsed = target.duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
                let now_elapsed = now.duration_since(Instant::now() - now.elapsed());
                let _ = now_elapsed;
                ttl = Some(Duration::from_secs(ts).saturating_sub(now.elapsed()));
            }
            idx += 2;
        } else if opt.eq_ignore_ascii_case(b"PXAT") && idx + 1 < args.len() {
            if let Ok(ts) = parse_u64(&args[idx + 1]) {
                ttl = Some(Duration::from_millis(ts).saturating_sub(now_ms()));
            }
            idx += 2;
        } else if opt.eq_ignore_ascii_case(b"NX") {
            nx = true;
            idx += 1;
        } else if opt.eq_ignore_ascii_case(b"XX") {
            xx = true;
            idx += 1;
        } else if opt.eq_ignore_ascii_case(b"GET") {
            get_flag = true;
            idx += 1;
        } else if opt.eq_ignore_ascii_case(b"KEEPTTL") {
            keepttl = true;
            idx += 1;
        } else {
            idx += 1;
        }
    }

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];

    if nx && database.get(key).is_some() {
        drop(server);
        return b"$-1\r\n".to_vec();
    }
    if xx && database.get(key).is_none() {
        drop(server);
        return b"$-1\r\n".to_vec();
    }

    let old_value = if get_flag {
        match database.get(key) {
            Some(entry) => match &entry.value {
                Value::String(v) => Some(v.clone()),
                _ => {
                    drop(server);
                    return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec();
                }
            },
            None => None,
        }
    } else {
        None
    };

    let expiry = if keepttl {
        database.get(key).and_then(|e| e.expires_at)
    } else {
        ttl.map(|d| Instant::now() + d)
    };

    database.insert(key.to_vec(), Value::String(value.to_vec()), expiry);

    drop(server);

    if get_flag {
        match old_value {
            Some(v) => {
                let mut resp = format!("${}\r\n", v.len()).into_bytes();
                resp.extend_from_slice(&v);
                resp.extend_from_slice(b"\r\n");
                resp
            }
            None => b"$-1\r\n".to_vec(),
        }
    } else {
        b"+OK\r\n".to_vec()
    }
}

pub async fn cmd_get(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.is_empty() {
        return b"-ERR wrong number of arguments for 'get' command\r\n".to_vec();
    }
    let key = &args[0];
    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &entry.value {
            Value::String(v) => {
                let mut resp = format!("${}\r\n", v.len()).into_bytes();
                resp.extend_from_slice(v);
                resp.extend_from_slice(b"\r\n");
                resp
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => b"$-1\r\n".to_vec(),
    }
}

pub async fn cmd_mset(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 2 || args.len() % 2 != 0 {
        return b"-ERR wrong number of arguments for 'mset' command\r\n".to_vec();
    }

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    for chunk in args.chunks(2) {
        database.insert(chunk[0].to_vec(), Value::String(chunk[1].to_vec()), None);
    }
    b"+OK\r\n".to_vec()
}

pub async fn cmd_mget(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.is_empty() {
        return b"-ERR wrong number of arguments for 'mget' command\r\n".to_vec();
    }

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    let mut results: Vec<Vec<u8>> = Vec::with_capacity(args.len());

    for key in args {
        match database.get_mut(key) {
            Some(entry) => match &entry.value {
                Value::String(v) => {
                    let mut resp = format!("${}\r\n", v.len()).into_bytes();
                    resp.extend_from_slice(v);
                    resp.extend_from_slice(b"\r\n");
                    results.push(resp);
                }
                _ => {
                    results.push(b"$-1\r\n".to_vec());
                }
            },
            None => {
                results.push(b"$-1\r\n".to_vec());
            }
        }
    }

    let mut resp = format!("*{}\r\n", results.len()).into_bytes();
    for r in &results {
        resp.extend_from_slice(r);
    }
    resp
}

pub async fn cmd_setnx(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 2 {
        return b"-ERR wrong number of arguments for 'setnx' command\r\n".to_vec();
    }
    let key = &args[0];
    let value = &args[1];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    if database.get(key).is_some() {
        return b":0\r\n".to_vec();
    }
    database.insert(key.to_vec(), Value::String(value.to_vec()), None);
    b":1\r\n".to_vec()
}

pub async fn cmd_setex(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 3 {
        return b"-ERR wrong number of arguments for 'setex' command\r\n".to_vec();
    }
    let key = &args[0];
    let secs = parse_u64(&args[1]).unwrap_or(0);
    let value = &args[2];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    let expiry = Some(Instant::now() + Duration::from_secs(secs));
    database.insert(key.to_vec(), Value::String(value.to_vec()), expiry);
    b"+OK\r\n".to_vec()
}

pub async fn cmd_psetex(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 3 {
        return b"-ERR wrong number of arguments for 'psetex' command\r\n".to_vec();
    }
    let key = &args[0];
    let ms = parse_u64(&args[1]).unwrap_or(0);
    let value = &args[2];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    let expiry = Some(Instant::now() + Duration::from_millis(ms));
    database.insert(key.to_vec(), Value::String(value.to_vec()), expiry);
    b"+OK\r\n".to_vec()
}

pub async fn cmd_getset(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 2 {
        return b"-ERR wrong number of arguments for 'getset' command\r\n".to_vec();
    }
    let key = &args[0];
    let value = &args[1];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    let old = match database.get_mut(key) {
        Some(entry) => match &entry.value {
            Value::String(v) => v.clone(),
            _ => return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => {
            database.insert(key.to_vec(), Value::String(value.to_vec()), None);
            return b"$-1\r\n".to_vec();
        }
    };

    database.insert(key.to_vec(), Value::String(value.to_vec()), None);

    let mut resp = format!("${}\r\n", old.len()).into_bytes();
    resp.extend_from_slice(&old);
    resp.extend_from_slice(b"\r\n");
    resp
}

pub async fn cmd_getdel(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.is_empty() {
        return b"-ERR wrong number of arguments for 'getdel' command\r\n".to_vec();
    }
    let key = &args[0];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &entry.value {
            Value::String(v) => {
                let val = v.clone();
                database.remove(key);
                let mut resp = format!("${}\r\n", val.len()).into_bytes();
                resp.extend_from_slice(&val);
                resp.extend_from_slice(b"\r\n");
                resp
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => b"$-1\r\n".to_vec(),
    }
}

pub async fn cmd_getex(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.is_empty() {
        return b"-ERR wrong number of arguments for 'getex' command\r\n".to_vec();
    }
    let key = &args[0];

    let mut ttl: Option<Option<Duration>> = None;
    let mut idx = 1;
    while idx < args.len() {
        let opt = &args[idx];
        if opt.eq_ignore_ascii_case(b"EX") && idx + 1 < args.len() {
            if let Ok(secs) = parse_u64(&args[idx + 1]) {
                ttl = Some(Some(Duration::from_secs(secs)));
            }
            idx += 2;
        } else if opt.eq_ignore_ascii_case(b"PX") && idx + 1 < args.len() {
            if let Ok(ms) = parse_u64(&args[idx + 1]) {
                ttl = Some(Some(Duration::from_millis(ms)));
            }
            idx += 2;
        } else if opt.eq_ignore_ascii_case(b"EXAT") && idx + 1 < args.len() {
            if let Ok(ts) = parse_u64(&args[idx + 1]) {
                let dur = Duration::from_secs(ts).saturating_sub(now_secs());
                ttl = Some(Some(dur));
            }
            idx += 2;
        } else if opt.eq_ignore_ascii_case(b"PXAT") && idx + 1 < args.len() {
            if let Ok(ts) = parse_u64(&args[idx + 1]) {
                let dur = Duration::from_millis(ts).saturating_sub(now_ms());
                ttl = Some(Some(dur));
            }
            idx += 2;
        } else if opt.eq_ignore_ascii_case(b"PERSIST") {
            ttl = Some(None);
            idx += 1;
        } else {
            idx += 1;
        }
    }

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &entry.value {
            Value::String(v) => {
                let val = v.clone();
                if let Some(new_ttl) = ttl {
                    entry.expires_at = new_ttl.map(|d| Instant::now() + d);
                }
                let mut resp = format!("${}\r\n", val.len()).into_bytes();
                resp.extend_from_slice(&val);
                resp.extend_from_slice(b"\r\n");
                resp
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => b"$-1\r\n".to_vec(),
    }
}

pub async fn cmd_append(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 2 {
        return b"-ERR wrong number of arguments for 'append' command\r\n".to_vec();
    }
    let key = &args[0];
    let value = &args[1];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &mut entry.value {
            Value::String(v) => {
                v.extend_from_slice(value);
                let new_len = v.len();
                format!(":{}\r\n", new_len).into_bytes()
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => {
            database.insert(key.to_vec(), Value::String(value.to_vec()), None);
            format!(":{}\r\n", value.len()).into_bytes()
        }
    }
}

pub async fn cmd_strlen(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.is_empty() {
        return b"-ERR wrong number of arguments for 'strlen' command\r\n".to_vec();
    }
    let key = &args[0];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &entry.value {
            Value::String(v) => format!(":{}\r\n", v.len()).into_bytes(),
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => b":0\r\n".to_vec(),
    }
}

pub async fn cmd_getrange(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 3 {
        return b"-ERR wrong number of arguments for 'getrange' command\r\n".to_vec();
    }
    let key = &args[0];
    let start = parse_i64(&args[1]).unwrap_or(0);
    let end = parse_i64(&args[2]).unwrap_or(0);

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &entry.value {
            Value::String(v) => {
                let len = v.len() as i64;
                if len == 0 {
                    return b"$0\r\n\r\n".to_vec();
                }
                let s = if start < 0 { (len + start).max(0) } else { start.min(len - 1) } as usize;
                let e = if end < 0 {
                    (len + end).max(0) as usize
                } else {
                    (end.min(len - 1) + 1) as usize
                };

                let slice = if s <= e && s < v.len() {
                    let e = e.min(v.len());
                    &v[s..e]
                } else {
                    &[]
                };

                let mut resp = format!("${}\r\n", slice.len()).into_bytes();
                resp.extend_from_slice(slice);
                resp.extend_from_slice(b"\r\n");
                resp
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => b"$0\r\n\r\n".to_vec(),
    }
}

pub async fn cmd_setrange(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 3 {
        return b"-ERR wrong number of arguments for 'setrange' command\r\n".to_vec();
    }
    let key = &args[0];
    let offset = parse_u64(&args[1]).unwrap_or(0) as usize;
    let value = &args[2];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &mut entry.value {
            Value::String(v) => {
                if offset + value.len() > v.len() {
                    v.resize(offset + value.len(), 0);
                }
                v[offset..offset + value.len()].copy_from_slice(value);
                format!(":{}\r\n", v.len()).into_bytes()
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => {
            let mut new_val = vec![0u8; offset + value.len()];
            new_val[offset..offset + value.len()].copy_from_slice(value);
            database.insert(key.to_vec(), Value::String(new_val.clone()), None);
            format!(":{}\r\n", new_val.len()).into_bytes()
        }
    }
}

pub async fn cmd_incr(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    cmd_incrby_impl(db, dbi, args, 1).await
}

pub async fn cmd_incrby(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 2 {
        return b"-ERR wrong number of arguments for 'incrby' command\r\n".to_vec();
    }
    let increment = parse_i64(&args[1]).unwrap_or(0);
    let key_args = vec![args[0].clone()];
    cmd_incrby_impl(db, dbi, &key_args, increment).await
}

pub async fn cmd_decr(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    cmd_incrby_impl(db, dbi, args, -1).await
}

pub async fn cmd_decrby(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 2 {
        return b"-ERR wrong number of arguments for 'decrby' command\r\n".to_vec();
    }
    let decrement = parse_i64(&args[1]).unwrap_or(0);
    let key_args = vec![args[0].clone()];
    cmd_incrby_impl(db, dbi, &key_args, -decrement).await
}

async fn cmd_incrby_impl(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
    increment: i64,
) -> Vec<u8> {
    if args.is_empty() {
        return b"-ERR wrong number of arguments\r\n".to_vec();
    }
    let key = &args[0];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &entry.value {
            Value::String(v) => {
                let s = String::from_utf8_lossy(v);
                match s.trim().parse::<i64>() {
                    Ok(current) => {
                        let new_val = match current.checked_add(increment) {
                            Some(n) => n,
                            None => return b"-ERR increment or decrement would overflow\r\n".to_vec(),
                        };
                        let new_str = new_val.to_string();
                        entry.value = Value::String(new_str.as_bytes().to_vec());
                        format!(":{}\r\n", new_val).into_bytes()
                    }
                    Err(_) => b"-ERR value is not an integer or out of range\r\n".to_vec(),
                }
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => {
            let new_val = increment;
            let new_str = new_val.to_string();
            database.insert(key.to_vec(), Value::String(new_str.as_bytes().to_vec()), None);
            format!(":{}\r\n", new_val).into_bytes()
        }
    }
}

pub async fn cmd_incrbyfloat(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 2 {
        return b"-ERR wrong number of arguments for 'incrbyfloat' command\r\n".to_vec();
    }
    let key = &args[0];
    let increment: f64 = match parse_f64(&args[1]) {
        Some(f) => f,
        None => return b"-ERR value is not a valid float\r\n".to_vec(),
    };

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &entry.value {
            Value::String(v) => {
                let s = String::from_utf8_lossy(v);
                let current: f64 = match s.trim().parse() {
                    Ok(f) => f,
                    Err(_) => return b"-ERR value is not a valid float\r\n".to_vec(),
                };
                let new_val = current + increment;
                if !new_val.is_finite() {
                    return b"-ERR increment would produce NaN or Infinity\r\n".to_vec();
                }

                let formatted = format_f64(new_val);
                entry.value = Value::String(formatted.as_bytes().to_vec());

                let mut resp = format!("${}\r\n", formatted.len()).into_bytes();
                resp.extend_from_slice(formatted.as_bytes());
                resp.extend_from_slice(b"\r\n");
                resp
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => {
            let new_val = increment;
            if !new_val.is_finite() {
                return b"-ERR increment would produce NaN or Infinity\r\n".to_vec();
            }
            let formatted = format_f64(new_val);
            database.insert(key.to_vec(), Value::String(formatted.as_bytes().to_vec()), None);

            let mut resp = format!("${}\r\n", formatted.len()).into_bytes();
            resp.extend_from_slice(formatted.as_bytes());
            resp.extend_from_slice(b"\r\n");
            resp
        }
    }
}

fn parse_u64(data: &[u8]) -> Result<u64, ()> {
    std::str::from_utf8(data)
        .map_err(|_| ())?
        .parse::<u64>()
        .map_err(|_| ())
}

fn parse_i64(data: &[u8]) -> Result<i64, ()> {
    std::str::from_utf8(data)
        .map_err(|_| ())?
        .parse::<i64>()
        .map_err(|_| ())
}

fn parse_f64(data: &[u8]) -> Option<f64> {
    std::str::from_utf8(data)
        .ok()?
        .trim()
        .parse::<f64>()
        .ok()
}

pub fn format_f64_public(f: f64) -> String {
    if f == 0.0 {
        return "0".to_string();
    }
    let s = format!("{:.17}", f);
    let s = s.trim_end_matches('0');
    let s = s.trim_end_matches('.');
    if s.is_empty() { "0".to_string() } else { s.to_string() }
}

fn format_f64(f: f64) -> String {
    format_f64_public(f)
}

fn now_secs() -> Duration {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
}

fn now_ms() -> Duration {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
}
