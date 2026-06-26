use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::storage::{ServerDb, Value};

pub async fn cmd_sadd(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 2 {
        return b"-ERR wrong number of arguments for 'sadd' command\r\n".to_vec();
    }
    let key = &args[0];
    let members = &args[1..];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &mut entry.value {
            Value::Set(set) => {
                let mut added = 0;
                for m in members {
                    if set.insert(m.clone()) {
                        added += 1;
                    }
                }
                format!(":{}\r\n", added).into_bytes()
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => {
            let mut set = HashSet::new();
            for m in members {
                set.insert(m.clone());
            }
            let added = set.len();
            database.insert(key.to_vec(), Value::Set(set), None);
            format!(":{}\r\n", added).into_bytes()
        }
    }
}

pub async fn cmd_srem(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 2 {
        return b"-ERR wrong number of arguments for 'srem' command\r\n".to_vec();
    }
    let key = &args[0];
    let members = &args[1..];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &mut entry.value {
            Value::Set(set) => {
                let mut removed = 0;
                for m in members {
                    if set.remove(m) {
                        removed += 1;
                    }
                }
                if set.is_empty() {
                    database.remove(key);
                }
                format!(":{}\r\n", removed).into_bytes()
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => b":0\r\n".to_vec(),
    }
}

pub async fn cmd_smembers(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.is_empty() {
        return b"-ERR wrong number of arguments for 'smembers' command\r\n".to_vec();
    }
    let key = &args[0];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &entry.value {
            Value::Set(set) => {
                let mut resp = format!("*{}\r\n", set.len()).into_bytes();
                for member in set {
                    resp.extend_from_slice(&format!("${}\r\n", member.len()).into_bytes());
                    resp.extend_from_slice(member);
                    resp.extend_from_slice(b"\r\n");
                }
                resp
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => b"*0\r\n".to_vec(),
    }
}

pub async fn cmd_sismember(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 2 {
        return b"-ERR wrong number of arguments for 'sismember' command\r\n".to_vec();
    }
    let key = &args[0];
    let member = &args[1];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &entry.value {
            Value::Set(set) => {
                if set.contains(member) { b":1\r\n".to_vec() } else { b":0\r\n".to_vec() }
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => b":0\r\n".to_vec(),
    }
}

pub async fn cmd_scard(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.is_empty() {
        return b"-ERR wrong number of arguments for 'scard' command\r\n".to_vec();
    }
    let key = &args[0];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &entry.value {
            Value::Set(set) => format!(":{}\r\n", set.len()).into_bytes(),
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => b":0\r\n".to_vec(),
    }
}

pub async fn cmd_sdiff(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.is_empty() {
        return b"-ERR wrong number of arguments for 'sdiff' command\r\n".to_vec();
    }
    let keys = args;

    let server = db.lock().await;
    let database = &server.databases[dbi];

    let first_set = match database.get(&keys[0]) {
        Some(entry) => match &entry.value {
            Value::Set(set) => set.clone(),
            _ => return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => return b"*0\r\n".to_vec(),
    };

    let mut result: HashSet<Vec<u8>> = first_set;
    for key in &keys[1..] {
        if let Some(entry) = database.get(key) {
            match &entry.value {
                Value::Set(set) => {
                    result = result.difference(set).cloned().collect();
                }
                _ => {}
            }
        }
    }

    let mut resp = format!("*{}\r\n", result.len()).into_bytes();
    for member in &result {
        resp.extend_from_slice(&format!("${}\r\n", member.len()).into_bytes());
        resp.extend_from_slice(member);
        resp.extend_from_slice(b"\r\n");
    }
    resp
}

pub async fn cmd_sdiffstore(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 2 {
        return b"-ERR wrong number of arguments for 'sdiffstore' command\r\n".to_vec();
    }
    let dest = &args[0];
    let keys = &args[1..];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];

    let first_set = match database.get(&keys[0]) {
        Some(entry) => match &entry.value {
            Value::Set(set) => set.clone(),
            _ => return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => {
            database.insert(dest.to_vec(), Value::Set(HashSet::new()), None);
            return b":0\r\n".to_vec();
        }
    };

    let mut result: HashSet<Vec<u8>> = first_set;
    for key in &keys[1..] {
        if let Some(entry) = database.get(key) {
            match &entry.value {
                Value::Set(set) => {
                    result = result.difference(set).cloned().collect();
                }
                _ => {}
            }
        }
    }

    let count = result.len();
    database.insert(dest.to_vec(), Value::Set(result), None);
    format!(":{}\r\n", count).into_bytes()
}

pub async fn cmd_sinter(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.is_empty() {
        return b"-ERR wrong number of arguments for 'sinter' command\r\n".to_vec();
    }
    let keys = args;

    let server = db.lock().await;
    let database = &server.databases[dbi];

    let first_set = match database.get(&keys[0]) {
        Some(entry) => match &entry.value {
            Value::Set(set) => set.clone(),
            _ => return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => return b"*0\r\n".to_vec(),
    };

    let mut result: HashSet<Vec<u8>> = first_set;
    for key in &keys[1..] {
        if let Some(entry) = database.get(key) {
            match &entry.value {
                Value::Set(set) => {
                    result = result.intersection(set).cloned().collect();
                }
                _ => { result.clear(); break; }
            }
        } else {
            result.clear();
            break;
        }
    }

    let mut resp = format!("*{}\r\n", result.len()).into_bytes();
    for member in &result {
        resp.extend_from_slice(&format!("${}\r\n", member.len()).into_bytes());
        resp.extend_from_slice(member);
        resp.extend_from_slice(b"\r\n");
    }
    resp
}

pub async fn cmd_sinterstore(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 2 {
        return b"-ERR wrong number of arguments for 'sinterstore' command\r\n".to_vec();
    }
    let dest = &args[0];
    let keys = &args[1..];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];

    let first_set = match database.get(&keys[0]) {
        Some(entry) => match &entry.value {
            Value::Set(set) => set.clone(),
            _ => return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => {
            database.insert(dest.to_vec(), Value::Set(HashSet::new()), None);
            return b":0\r\n".to_vec();
        }
    };

    let mut result: HashSet<Vec<u8>> = first_set;
    for key in &keys[1..] {
        if let Some(entry) = database.get(key) {
            match &entry.value {
                Value::Set(set) => {
                    result = result.intersection(set).cloned().collect();
                }
                _ => { result.clear(); break; }
            }
        } else {
            result.clear();
            break;
        }
    }

    let count = result.len();
    database.insert(dest.to_vec(), Value::Set(result), None);
    format!(":{}\r\n", count).into_bytes()
}

pub async fn cmd_sunion(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.is_empty() {
        return b"-ERR wrong number of arguments for 'sunion' command\r\n".to_vec();
    }
    let keys = args;

    let server = db.lock().await;
    let database = &server.databases[dbi];

    let mut result = HashSet::new();
    for key in keys {
        if let Some(entry) = database.get(key) {
            match &entry.value {
                Value::Set(set) => {
                    for m in set {
                        result.insert(m.clone());
                    }
                }
                _ => {}
            }
        }
    }

    let mut resp = format!("*{}\r\n", result.len()).into_bytes();
    for member in &result {
        resp.extend_from_slice(&format!("${}\r\n", member.len()).into_bytes());
        resp.extend_from_slice(member);
        resp.extend_from_slice(b"\r\n");
    }
    resp
}

pub async fn cmd_sunionstore(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 2 {
        return b"-ERR wrong number of arguments for 'sunionstore' command\r\n".to_vec();
    }
    let dest = &args[0];
    let keys = &args[1..];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];

    let mut result = HashSet::new();
    for key in keys {
        if let Some(entry) = database.get(key) {
            match &entry.value {
                Value::Set(set) => {
                    for m in set {
                        result.insert(m.clone());
                    }
                }
                _ => {}
            }
        }
    }

    let count = result.len();
    database.insert(dest.to_vec(), Value::Set(result), None);
    format!(":{}\r\n", count).into_bytes()
}

pub async fn cmd_srandmember(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.is_empty() {
        return b"-ERR wrong number of arguments for 'srandmember' command\r\n".to_vec();
    }
    let key = &args[0];
    let count: i64 = if args.len() >= 2 { parse_i64(&args[1]).unwrap_or(1) } else { 1 };

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &entry.value {
            Value::Set(set) => {
                if set.is_empty() {
                    return if count < 0 { b"*0\r\n".to_vec() } else { b"$-1\r\n".to_vec() };
                }
                let members: Vec<&Vec<u8>> = set.iter().collect();
                if count >= 0 {
                    let take = (count as usize).min(members.len());
                    if count == 1 && take == 1 {
                        let idx = fast_rand_idx(members.len());
                        let val = members[idx];
                        let mut resp = format!("${}\r\n", val.len()).into_bytes();
                        resp.extend_from_slice(val);
                        resp.extend_from_slice(b"\r\n");
                        resp
                    } else {
                        let chosen = pick_distinct(&members, take);
                        let mut resp = format!("*{}\r\n", chosen.len()).into_bytes();
                        for val in chosen {
                            resp.extend_from_slice(&format!("${}\r\n", val.len()).into_bytes());
                            resp.extend_from_slice(val);
                            resp.extend_from_slice(b"\r\n");
                        }
                        resp
                    }
                } else {
                    let take = (-count) as usize;
                    let chosen = pick_with_repeats(&members, take);
                    let mut resp = format!("*{}\r\n", chosen.len()).into_bytes();
                    for val in chosen {
                        resp.extend_from_slice(&format!("${}\r\n", val.len()).into_bytes());
                        resp.extend_from_slice(val);
                        resp.extend_from_slice(b"\r\n");
                    }
                    resp
                }
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => {
            if count < 0 { b"*0\r\n".to_vec() } else { b"$-1\r\n".to_vec() }
        }
    }
}

pub async fn cmd_spop(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.is_empty() {
        return b"-ERR wrong number of arguments for 'spop' command\r\n".to_vec();
    }
    let key = &args[0];
    let count: usize = if args.len() >= 2 { parse_u64(&args[1]).unwrap_or(1) as usize } else { 1 };

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &mut entry.value {
            Value::Set(set) => {
                if set.is_empty() {
                    return b"$-1\r\n".to_vec();
                }
                let members: Vec<Vec<u8>> = set.iter().cloned().collect();
                let take = count.min(members.len());
                let chosen = pick_distinct(&members, take);
                for m in &chosen {
                    set.remove(m);
                }
                if set.is_empty() {
                    database.remove(key);
                }
                if count <= 1 {
                    if let Some(val) = chosen.first() {
                        let mut resp = format!("${}\r\n", val.len()).into_bytes();
                        resp.extend_from_slice(val);
                        resp.extend_from_slice(b"\r\n");
                        resp
                    } else {
                        b"$-1\r\n".to_vec()
                    }
                } else {
                    let mut resp = format!("*{}\r\n", chosen.len()).into_bytes();
                    for val in chosen {
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

pub async fn cmd_smove(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 3 {
        return b"-ERR wrong number of arguments for 'smove' command\r\n".to_vec();
    }
    let source = &args[0];
    let dest = &args[1];
    let member = &args[2];

    if source == dest {
        return cmd_sismember(db, dbi, &[source.clone(), member.clone()]).await;
    }

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];

    let removed = match database.get_mut(source) {
        Some(entry) => match &mut entry.value {
            Value::Set(set) => {
                let r = set.remove(member);
                if set.is_empty() {
                    database.remove(source);
                }
                r
            }
            _ => return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => return b":0\r\n".to_vec(),
    };

    if !removed {
        return b":0\r\n".to_vec();
    }

    match database.get_mut(dest) {
        Some(entry) => match &mut entry.value {
            Value::Set(set) => { set.insert(member.clone()); }
            _ => return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => {
            let mut set = HashSet::new();
            set.insert(member.clone());
            database.insert(dest.to_vec(), Value::Set(set), None);
        }
    }

    b":1\r\n".to_vec()
}

pub async fn cmd_sscan(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.is_empty() {
        return b"-ERR wrong number of arguments for 'sscan' command\r\n".to_vec();
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
            Value::Set(set) => {
                let mut members: Vec<&Vec<u8>> = set.iter().collect();
                members.sort_by(|a, b| a.cmp(b));

                let mut matched = Vec::new();
                let mut skipped = 0usize;
                for (i, m) in members.iter().enumerate() {
                    if i < cursor {
                        skipped += 1;
                        continue;
                    }
                    let is_match = match &match_pattern {
                        Some(pattern) => crate::cmd_key::wildcard_match(pattern, m),
                        None => true,
                    };
                    if is_match {
                        matched.push((*m).clone());
                        if matched.len() >= count {
                            break;
                        }
                    }
                }

                let traversed = cursor + matched.len() + skipped;
                let total = set.len();
                let next_cursor = if traversed >= total { 0 } else { traversed };

                let mut resp = format!("*2\r\n${}\r\n{}\r\n*{}\r\n", next_cursor.to_string().len(), next_cursor, matched.len()).into_bytes();
                for m in matched {
                    resp.extend_from_slice(&format!("${}\r\n", m.len()).into_bytes());
                    resp.extend_from_slice(&m);
                    resp.extend_from_slice(b"\r\n");
                }
                resp
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => b"*2\r\n$1\r\n0\r\n*0\r\n".to_vec(),
    }
}

fn parse_u64(data: &[u8]) -> Result<u64, ()> {
    std::str::from_utf8(data).map_err(|_| ())?.parse::<u64>().map_err(|_| ())
}

fn parse_i64(data: &[u8]) -> Result<i64, ()> {
    std::str::from_utf8(data).map_err(|_| ())?.parse::<i64>().map_err(|_| ())
}

fn fast_rand_idx(max: usize) -> usize {
    if max <= 1 { return 0; }
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().subsec_nanos();
    (nanos as usize) % max
}

fn pick_distinct<T: Clone>(items: &[T], count: usize) -> Vec<T> {
    if count >= items.len() {
        return items.to_vec();
    }
    let mut result = Vec::with_capacity(count);
    let mut available: Vec<usize> = (0..items.len()).collect();
    for _ in 0..count {
        let idx = fast_rand_idx(available.len());
        let pos = available.swap_remove(idx);
        result.push(items[pos].clone());
    }
    result
}

fn pick_with_repeats<T: Clone>(items: &[T], count: usize) -> Vec<T> {
    let mut result = Vec::with_capacity(count);
    for _ in 0..count {
        let idx = fast_rand_idx(items.len());
        result.push(items[idx].clone());
    }
    result
}
