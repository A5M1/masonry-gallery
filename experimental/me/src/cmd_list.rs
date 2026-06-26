use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::storage::{ServerDb, Value};

pub async fn cmd_lpush(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 2 {
        return b"-ERR wrong number of arguments for 'lpush' command\r\n".to_vec();
    }
    let key = &args[0];
    let elements = &args[1..];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &mut entry.value {
            Value::List(list) => {
                for elem in elements.iter().rev() {
                    list.push_front(elem.clone());
                }
                format!(":{}\r\n", list.len()).into_bytes()
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => {
            let mut list = VecDeque::new();
            for elem in elements.iter().rev() {
                list.push_front(elem.clone());
            }
            let len = list.len();
            database.insert(key.to_vec(), Value::List(list), None);
            format!(":{}\r\n", len).into_bytes()
        }
    }
}

pub async fn cmd_rpush(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 2 {
        return b"-ERR wrong number of arguments for 'rpush' command\r\n".to_vec();
    }
    let key = &args[0];
    let elements = &args[1..];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &mut entry.value {
            Value::List(list) => {
                for elem in elements {
                    list.push_back(elem.clone());
                }
                format!(":{}\r\n", list.len()).into_bytes()
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => {
            let mut list = VecDeque::new();
            for elem in elements {
                list.push_back(elem.clone());
            }
            let len = list.len();
            database.insert(key.to_vec(), Value::List(list), None);
            format!(":{}\r\n", len).into_bytes()
        }
    }
}

pub async fn cmd_lpop(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.is_empty() {
        return b"-ERR wrong number of arguments for 'lpop' command\r\n".to_vec();
    }
    let key = &args[0];
    let count: usize = if args.len() >= 2 {
        parse_u64(&args[1]).unwrap_or(1) as usize
    } else {
        1
    };

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &mut entry.value {
            Value::List(list) => {
                if list.is_empty() {
                    return b"$-1\r\n".to_vec();
                }
                let take = count.min(list.len());
                if count <= 1 {
                    if let Some(val) = list.pop_front() {
                        if list.is_empty() {
                            database.remove(key);
                        }
                        let mut resp = format!("${}\r\n", val.len()).into_bytes();
                        resp.extend_from_slice(&val);
                        resp.extend_from_slice(b"\r\n");
                        return resp;
                    }
                    b"$-1\r\n".to_vec()
                } else {
                    let mut vals = Vec::with_capacity(take);
                    for _ in 0..take {
                        if let Some(val) = list.pop_front() {
                            vals.push(val);
                        }
                    }
                    if list.is_empty() {
                        database.remove(key);
                    }
                    let mut resp = format!("*{}\r\n", vals.len()).into_bytes();
                    for val in vals {
                        resp.extend_from_slice(&format!("${}\r\n", val.len()).into_bytes());
                        resp.extend_from_slice(&val);
                        resp.extend_from_slice(b"\r\n");
                    }
                    resp
                }
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => b"$-1\r\n".to_vec(),
    }
}

pub async fn cmd_rpop(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.is_empty() {
        return b"-ERR wrong number of arguments for 'rpop' command\r\n".to_vec();
    }
    let key = &args[0];
    let count: usize = if args.len() >= 2 {
        parse_u64(&args[1]).unwrap_or(1) as usize
    } else {
        1
    };

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &mut entry.value {
            Value::List(list) => {
                if list.is_empty() {
                    return b"$-1\r\n".to_vec();
                }
                let take = count.min(list.len());
                if count <= 1 {
                    if let Some(val) = list.pop_back() {
                        if list.is_empty() {
                            database.remove(key);
                        }
                        let mut resp = format!("${}\r\n", val.len()).into_bytes();
                        resp.extend_from_slice(&val);
                        resp.extend_from_slice(b"\r\n");
                        return resp;
                    }
                    b"$-1\r\n".to_vec()
                } else {
                    let mut vals = Vec::with_capacity(take);
                    for _ in 0..take {
                        if let Some(val) = list.pop_back() {
                            vals.push(val);
                        }
                    }
                    if list.is_empty() {
                        database.remove(key);
                    }
                    vals.reverse();
                    let mut resp = format!("*{}\r\n", vals.len()).into_bytes();
                    for val in vals {
                        resp.extend_from_slice(&format!("${}\r\n", val.len()).into_bytes());
                        resp.extend_from_slice(&val);
                        resp.extend_from_slice(b"\r\n");
                    }
                    resp
                }
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => b"$-1\r\n".to_vec(),
    }
}

pub async fn cmd_llen(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.is_empty() {
        return b"-ERR wrong number of arguments for 'llen' command\r\n".to_vec();
    }
    let key = &args[0];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &entry.value {
            Value::List(list) => format!(":{}\r\n", list.len()).into_bytes(),
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => b":0\r\n".to_vec(),
    }
}

pub async fn cmd_lrange(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 3 {
        return b"-ERR wrong number of arguments for 'lrange' command\r\n".to_vec();
    }
    let key = &args[0];
    let start = parse_i64(&args[1]).unwrap_or(0);
    let stop = parse_i64(&args[2]).unwrap_or(0);

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &entry.value {
            Value::List(list) => {
                let len = list.len() as i64;
                let (s, e) = normalize_range(start, stop, len);
                let result: Vec<&Vec<u8>> = list.iter().skip(s).take(e.saturating_sub(s).max(1)).collect();

                let mut resp = format!("*{}\r\n", result.len()).into_bytes();
                for val in result {
                    resp.extend_from_slice(&format!("${}\r\n", val.len()).into_bytes());
                    resp.extend_from_slice(val);
                    resp.extend_from_slice(b"\r\n");
                }
                resp
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => b"*0\r\n".to_vec(),
    }
}

pub async fn cmd_lindex(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 2 {
        return b"-ERR wrong number of arguments for 'lindex' command\r\n".to_vec();
    }
    let key = &args[0];
    let index = parse_i64(&args[1]).unwrap_or(0);

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &entry.value {
            Value::List(list) => {
                let len = list.len() as i64;
                let idx = if index < 0 { (len + index).max(0) } else { index.min(len - 1) } as usize;
                if idx < list.len() {
                    let val = &list[idx];
                    let mut resp = format!("${}\r\n", val.len()).into_bytes();
                    resp.extend_from_slice(val);
                    resp.extend_from_slice(b"\r\n");
                    resp
                } else {
                    b"$-1\r\n".to_vec()
                }
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => b"$-1\r\n".to_vec(),
    }
}

pub async fn cmd_lset(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 3 {
        return b"-ERR wrong number of arguments for 'lset' command\r\n".to_vec();
    }
    let key = &args[0];
    let index = parse_i64(&args[1]).unwrap_or(0);
    let value = &args[2];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &mut entry.value {
            Value::List(list) => {
                let len = list.len() as i64;
                if index < 0 || index >= len {
                    return b"-ERR index out of range\r\n".to_vec();
                }
                let idx = if index < 0 { (len + index) as usize } else { index as usize };
                if idx < list.len() {
                    list[idx] = value.clone();
                    return b"+OK\r\n".to_vec();
                }
                b"-ERR index out of range\r\n".to_vec()
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => b"-ERR no such key\r\n".to_vec(),
    }
}

pub async fn cmd_lrem(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 3 {
        return b"-ERR wrong number of arguments for 'lrem' command\r\n".to_vec();
    }
    let key = &args[0];
    let count = parse_i64(&args[1]).unwrap_or(0);
    let value = &args[2];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &mut entry.value {
            Value::List(list) => {
                let mut removed = 0i64;
                if count == 0 {
                    list.retain(|v| {
                        if v == value { removed += 1; false } else { true }
                    });
                } else if count > 0 {
                    let mut to_remove = count as usize;
                    list.retain(|v| {
                        if to_remove > 0 && v == value { removed += 1; to_remove -= 1; false } else { true }
                    });
                } else {
                    let mut to_remove = (-count) as usize;
                    let mut indices_to_remove = Vec::new();
                    for (i, v) in list.iter().enumerate().rev() {
                        if to_remove > 0 && v == value {
                            indices_to_remove.push(i);
                            to_remove -= 1;
                        }
                    }
                    for idx in indices_to_remove.iter().rev() {
                        list.remove(*idx);
                        removed += 1;
                    }
                }
                if list.is_empty() {
                    database.remove(key);
                }
                format!(":{}\r\n", removed).into_bytes()
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => b":0\r\n".to_vec(),
    }
}

pub async fn cmd_ltrim(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 3 {
        return b"-ERR wrong number of arguments for 'ltrim' command\r\n".to_vec();
    }
    let key = &args[0];
    let start = parse_i64(&args[1]).unwrap_or(0);
    let stop = parse_i64(&args[2]).unwrap_or(0);

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &mut entry.value {
            Value::List(list) => {
                let len = list.len() as i64;
                let (s, e) = normalize_range(start, stop, len);
                let take = e.saturating_sub(s).max(1);
                *list = list.iter().skip(s).take(take).cloned().collect();
                b"+OK\r\n".to_vec()
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => b"+OK\r\n".to_vec(),
    }
}

pub async fn cmd_linsert(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 4 {
        return b"-ERR wrong number of arguments for 'linsert' command\r\n".to_vec();
    }
    let key = &args[0];
    let where_str = &args[1];
    let pivot = &args[2];
    let value = &args[3];

    let before = where_str.eq_ignore_ascii_case(b"BEFORE");

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &mut entry.value {
            Value::List(list) => {
                let pos = list.iter().position(|v| v == pivot);
                match pos {
                    Some(idx) => {
                        let insert_at = if before { idx } else { idx + 1 };
                        list.insert(insert_at, value.clone());
                        format!(":{}\r\n", list.len()).into_bytes()
                    }
                    None => b":-1\r\n".to_vec(),
                }
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => b":0\r\n".to_vec(),
    }
}

pub async fn cmd_rpoplpush(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 2 {
        return b"-ERR wrong number of arguments for 'rpoplpush' command\r\n".to_vec();
    }
    let source = &args[0];
    let dest = &args[1];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];

    let val = match database.get_mut(source) {
        Some(entry) => match &mut entry.value {
            Value::List(list) => {
                let v = list.pop_back();
                if list.is_empty() {
                    database.remove(source);
                }
                v
            }
            _ => return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => return b"$-1\r\n".to_vec(),
    };

    match val {
        Some(v) => {
            match database.get_mut(dest) {
                Some(entry) => match &mut entry.value {
                    Value::List(list) => list.push_front(v.clone()),
                    _ => return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
                },
                None => {
                    let mut list = VecDeque::new();
                    list.push_front(v.clone());
                    database.insert(dest.to_vec(), Value::List(list), None);
                }
            }
            let mut resp = format!("${}\r\n", v.len()).into_bytes();
            resp.extend_from_slice(&v);
            resp.extend_from_slice(b"\r\n");
            resp
        }
        None => b"$-1\r\n".to_vec(),
    }
}

pub async fn cmd_lmove(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 4 {
        return b"-ERR wrong number of arguments for 'lmove' command\r\n".to_vec();
    }
    let source = &args[0];
    let dest = &args[1];
    let wherefrom = &args[2];
    let whereto = &args[3];

    let from_left = wherefrom.eq_ignore_ascii_case(b"LEFT");
    let to_left = whereto.eq_ignore_ascii_case(b"LEFT");

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];

    let val = match database.get_mut(source) {
        Some(entry) => match &mut entry.value {
            Value::List(list) => {
                let v = if from_left { list.pop_front() } else { list.pop_back() };
                if list.is_empty() {
                    database.remove(source);
                }
                v
            }
            _ => return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => return b"$-1\r\n".to_vec(),
    };

    match val {
        Some(v) => {
            match database.get_mut(dest) {
                Some(entry) => match &mut entry.value {
                    Value::List(list) => {
                        if to_left { list.push_front(v.clone()) } else { list.push_back(v.clone()) }
                    }
                    _ => return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
                },
                None => {
                    let mut list = VecDeque::new();
                    if to_left { list.push_front(v.clone()) } else { list.push_back(v.clone()) }
                    database.insert(dest.to_vec(), Value::List(list), None);
                }
            }
            let mut resp = format!("${}\r\n", v.len()).into_bytes();
            resp.extend_from_slice(&v);
            resp.extend_from_slice(b"\r\n");
            resp
        }
        None => b"$-1\r\n".to_vec(),
    }
}

fn parse_u64(data: &[u8]) -> Result<u64, ()> {
    std::str::from_utf8(data).map_err(|_| ())?.parse::<u64>().map_err(|_| ())
}

fn parse_i64(data: &[u8]) -> Result<i64, ()> {
    std::str::from_utf8(data).map_err(|_| ())?.parse::<i64>().map_err(|_| ())
}

fn normalize_range(start: i64, stop: i64, len: i64) -> (usize, usize) {
    if len == 0 {
        return (0, 0);
    }
    let s = if start < 0 { (len + start).max(0) } else { start.min(len - 1) } as usize;
    let e = if stop < 0 {
        (len + stop).max(0) as usize
    } else {
        (stop.min(len - 1)) as usize
    };
    (s, e)
}
