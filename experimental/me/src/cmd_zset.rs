use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::storage::{ServerDb, Value};

pub async fn cmd_zadd(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 3 || args.len() % 2 != 1 {
        return b"-ERR wrong number of arguments for 'zadd' command\r\n".to_vec();
    }
    let key = &args[0];
    let score_members = &args[1..];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];

    let (mut by_score, mut by_member) = match database.get_mut(key) {
        Some(entry) => match &mut entry.value {
            Value::ZSet { by_score, by_member } => {
                let bs = std::mem::take(by_score);
                let bm = std::mem::take(by_member);
                (bs, bm)
            }
            _ => return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => (BTreeMap::new(), BTreeMap::new()),
    };

    let mut added = 0i64;
    for chunk in score_members.chunks(2) {
        let score = parse_f64_as_i64(&chunk[0]);
        let member = &chunk[1];

        if let Some(old_score) = by_member.get(member) {
            by_score.remove(&(*old_score, member.clone()));
        } else {
            added += 1;
        }

        by_member.insert(member.clone(), score);
        by_score.insert((score, member.clone()), ());
    }

    match database.get_mut(key) {
        Some(entry) => {
            entry.value = Value::ZSet { by_score, by_member };
        }
        None => {
            database.store.insert(key.to_vec(), crate::storage::Entry::new(
                Value::ZSet { by_score, by_member },
                None,
            ));
        }
    }

    format!(":{}\r\n", added).into_bytes()
}

pub async fn cmd_zrem(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 2 {
        return b"-ERR wrong number of arguments for 'zrem' command\r\n".to_vec();
    }
    let key = &args[0];
    let members = &args[1..];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &mut entry.value {
            Value::ZSet { by_score, by_member } => {
                let mut removed = 0;
                for m in members {
                    if let Some(score) = by_member.remove(m) {
                        by_score.remove(&(score, m.clone()));
                        removed += 1;
                    }
                }
                if by_member.is_empty() {
                    database.remove(key);
                }
                format!(":{}\r\n", removed).into_bytes()
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => b":0\r\n".to_vec(),
    }
}

pub async fn cmd_zcard(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.is_empty() {
        return b"-ERR wrong number of arguments for 'zcard' command\r\n".to_vec();
    }
    let key = &args[0];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &entry.value {
            Value::ZSet { by_member, .. } => format!(":{}\r\n", by_member.len()).into_bytes(),
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => b":0\r\n".to_vec(),
    }
}

pub async fn cmd_zscore(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 2 {
        return b"-ERR wrong number of arguments for 'zscore' command\r\n".to_vec();
    }
    let key = &args[0];
    let member = &args[1];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &entry.value {
            Value::ZSet { by_member, .. } => match by_member.get(member) {
                Some(score) => {
                    let formatted = i64_to_score_str(*score);
                    let mut resp = format!("${}\r\n", formatted.len()).into_bytes();
                    resp.extend_from_slice(formatted.as_bytes());
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

pub async fn cmd_zrank(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 2 {
        return b"-ERR wrong number of arguments for 'zrank' command\r\n".to_vec();
    }
    let key = &args[0];
    let member = &args[1];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &entry.value {
            Value::ZSet { by_score, by_member } => match by_member.get(member) {
                Some(target_score) => {
                    let mut rank = 0i64;
                    for ((score, _), _) in by_score.iter() {
                        if score < target_score {
                            rank += 1;
                        } else {
                            break;
                        }
                    }
                    format!(":{}\r\n", rank).into_bytes()
                }
                None => b"$-1\r\n".to_vec(),
            },
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => b"$-1\r\n".to_vec(),
    }
}

pub async fn cmd_zrevrank(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 2 {
        return b"-ERR wrong number of arguments for 'zrevrank' command\r\n".to_vec();
    }
    let key = &args[0];
    let member = &args[1];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &entry.value {
            Value::ZSet { by_score, by_member } => match by_member.get(member) {
                Some(target_score) => {
                    let mut rank = 0i64;
                    for ((score, _), _) in by_score.iter().rev() {
                        if score > target_score {
                            rank += 1;
                        } else {
                            break;
                        }
                    }
                    format!(":{}\r\n", rank).into_bytes()
                }
                None => b"$-1\r\n".to_vec(),
            },
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => b"$-1\r\n".to_vec(),
    }
}

pub async fn cmd_zrange(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 3 {
        return b"-ERR wrong number of arguments for 'zrange' command\r\n".to_vec();
    }
    let key = &args[0];
    let start = parse_i64(&args[1]).unwrap_or(0);
    let stop = parse_i64(&args[2]).unwrap_or(0);
    let withscores = args.len() >= 4 && args[3].eq_ignore_ascii_case(b"WITHSCORES");

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &entry.value {
            Value::ZSet { by_score, .. } => {
                let len = by_score.len() as i64;
                let (s, e) = normalize_range(start, stop, len);
                let items: Vec<(&Vec<u8>, i64)> = by_score.iter()
                    .skip(s)
                    .take((e.saturating_sub(s)).max(1))
                    .map(|((score, member), _)| (member, *score))
                    .collect();

                let count = if withscores { items.len() * 2 } else { items.len() };
                let mut resp = format!("*{}\r\n", count).into_bytes();
                for (member, score) in items {
                    resp.extend_from_slice(&format!("${}\r\n", member.len()).into_bytes());
                    resp.extend_from_slice(member);
                    resp.extend_from_slice(b"\r\n");
                    if withscores {
                        let score_str = i64_to_score_str(score);
                        resp.extend_from_slice(&format!("${}\r\n", score_str.len()).into_bytes());
                        resp.extend_from_slice(score_str.as_bytes());
                        resp.extend_from_slice(b"\r\n");
                    }
                }
                resp
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => b"*0\r\n".to_vec(),
    }
}

pub async fn cmd_zrevrange(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 3 {
        return b"-ERR wrong number of arguments for 'zrevrange' command\r\n".to_vec();
    }
    let key = &args[0];
    let start = parse_i64(&args[1]).unwrap_or(0);
    let stop = parse_i64(&args[2]).unwrap_or(0);
    let withscores = args.len() >= 4 && args[3].eq_ignore_ascii_case(b"WITHSCORES");

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &entry.value {
            Value::ZSet { by_score, .. } => {
                let len = by_score.len() as i64;
                let (s, e) = normalize_range(start, stop, len);
                let items: Vec<(&Vec<u8>, i64)> = by_score.iter()
                    .rev()
                    .skip(s)
                    .take((e.saturating_sub(s)).max(1))
                    .map(|((score, member), _)| (member, *score))
                    .collect();

                let count = if withscores { items.len() * 2 } else { items.len() };
                let mut resp = format!("*{}\r\n", count).into_bytes();
                for (member, score) in items {
                    resp.extend_from_slice(&format!("${}\r\n", member.len()).into_bytes());
                    resp.extend_from_slice(member);
                    resp.extend_from_slice(b"\r\n");
                    if withscores {
                        let score_str = i64_to_score_str(score);
                        resp.extend_from_slice(&format!("${}\r\n", score_str.len()).into_bytes());
                        resp.extend_from_slice(score_str.as_bytes());
                        resp.extend_from_slice(b"\r\n");
                    }
                }
                resp
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => b"*0\r\n".to_vec(),
    }
}

pub async fn cmd_zrangebyscore(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 3 {
        return b"-ERR wrong number of arguments for 'zrangebyscore' command\r\n".to_vec();
    }
    let key = &args[0];
    let min_str = &args[1];
    let max_str = &args[2];
    let withscores = args.len() >= 4 && args.get(3).map(|a| a.eq_ignore_ascii_case(b"WITHSCORES")).unwrap_or(false);

    let (min, min_exclusive) = parse_score_range(min_str);
    let (max, max_exclusive) = parse_score_range(max_str);

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &entry.value {
            Value::ZSet { by_score, .. } => {
                let items: Vec<(&Vec<u8>, i64)> = by_score.iter()
                    .filter(|((score, _), _)| {
                        let s = *score;
                        let above_min = if min_exclusive { s > min } else { s >= min };
                        let below_max = if max_exclusive { s < max } else { s <= max };
                        above_min && below_max
                    })
                    .map(|((score, member), _)| (member, *score))
                    .collect();

                let count = if withscores { items.len() * 2 } else { items.len() };
                let mut resp = format!("*{}\r\n", count).into_bytes();
                for (member, score) in items {
                    resp.extend_from_slice(&format!("${}\r\n", member.len()).into_bytes());
                    resp.extend_from_slice(member);
                    resp.extend_from_slice(b"\r\n");
                    if withscores {
                        let score_str = i64_to_score_str(score);
                        resp.extend_from_slice(&format!("${}\r\n", score_str.len()).into_bytes());
                        resp.extend_from_slice(score_str.as_bytes());
                        resp.extend_from_slice(b"\r\n");
                    }
                }
                resp
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => b"*0\r\n".to_vec(),
    }
}

pub async fn cmd_zremrangebyrank(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 3 {
        return b"-ERR wrong number of arguments for 'zremrangebyrank' command\r\n".to_vec();
    }
    let key = &args[0];
    let start = parse_i64(&args[1]).unwrap_or(0);
    let stop = parse_i64(&args[2]).unwrap_or(0);

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &mut entry.value {
            Value::ZSet { by_score, by_member } => {
                let len = by_score.len() as i64;
                let (s, e) = normalize_range(start, stop, len);
                let to_remove: Vec<(i64, Vec<u8>)> = by_score.iter()
                    .skip(s)
                    .take((e.saturating_sub(s)).max(1))
                    .map(|((score, member), _)| (*score, member.clone()))
                    .collect();

                for (score, member) in to_remove {
                    by_score.remove(&(score, member.clone()));
                    by_member.remove(&member);
                }

                if by_member.is_empty() {
                    database.remove(key);
                    return b":1\r\n".to_vec();
                }
                let removed = (e.saturating_sub(s).max(1)).min(by_member.len() as usize + (e - s + 1) as usize);
                format!(":{}\r\n", removed).into_bytes()
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => b":0\r\n".to_vec(),
    }
}

pub async fn cmd_zremrangebyscore(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 3 {
        return b"-ERR wrong number of arguments for 'zremrangebyscore' command\r\n".to_vec();
    }
    let key = &args[0];
    let min_str = &args[1];
    let max_str = &args[2];

    let (min, min_exclusive) = parse_score_range(min_str);
    let (max, max_exclusive) = parse_score_range(max_str);

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &mut entry.value {
            Value::ZSet { by_score, by_member } => {
                let to_remove: Vec<(i64, Vec<u8>)> = by_score.iter()
                    .filter(|((score, _), _)| {
                        let s = *score;
                        let above_min = if min_exclusive { s > min } else { s >= min };
                        let below_max = if max_exclusive { s < max } else { s <= max };
                        above_min && below_max
                    })
                    .map(|((score, member), _)| (*score, member.clone()))
                    .collect();

                let removed = to_remove.len();
                for (score, member) in to_remove {
                    by_score.remove(&(score, member.clone()));
                    by_member.remove(&member);
                }

                if by_member.is_empty() {
                    database.remove(key);
                }
                format!(":{}\r\n", removed).into_bytes()
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => b":0\r\n".to_vec(),
    }
}

pub async fn cmd_zincrby(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 3 {
        return b"-ERR wrong number of arguments for 'zincrby' command\r\n".to_vec();
    }
    let key = &args[0];
    let increment = parse_f64_as_i64(&args[1]);
    let member = &args[2];

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &mut entry.value {
            Value::ZSet { by_score, by_member } => {
                let new_score = if let Some(old_score) = by_member.get(member) {
                    let old = *old_score;
                    by_score.remove(&(old, member.clone()));
                    old + increment
                } else {
                    increment
                };

                by_member.insert(member.clone(), new_score);
                by_score.insert((new_score, member.clone()), ());

                let formatted = i64_to_score_str(new_score);
                let mut resp = format!("${}\r\n", formatted.len()).into_bytes();
                resp.extend_from_slice(formatted.as_bytes());
                resp.extend_from_slice(b"\r\n");
                resp
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => {
            let mut by_score = BTreeMap::new();
            let mut by_member = BTreeMap::new();
            by_member.insert(member.clone(), increment);
            by_score.insert((increment, member.clone()), ());
            database.store.insert(key.to_vec(), crate::storage::Entry::new(
                Value::ZSet { by_score, by_member },
                None,
            ));

            let formatted = i64_to_score_str(increment);
            let mut resp = format!("${}\r\n", formatted.len()).into_bytes();
            resp.extend_from_slice(formatted.as_bytes());
            resp.extend_from_slice(b"\r\n");
            resp
        }
    }
}

pub async fn cmd_zcount(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.len() < 3 {
        return b"-ERR wrong number of arguments for 'zcount' command\r\n".to_vec();
    }
    let key = &args[0];
    let min_str = &args[1];
    let max_str = &args[2];

    let (min, min_exclusive) = parse_score_range(min_str);
    let (max, max_exclusive) = parse_score_range(max_str);

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &entry.value {
            Value::ZSet { by_score, .. } => {
                let count = by_score.iter()
                    .filter(|((score, _), _)| {
                        let s = *score;
                        let above_min = if min_exclusive { s > min } else { s >= min };
                        let below_max = if max_exclusive { s < max } else { s <= max };
                        above_min && below_max
                    })
                    .count();
                format!(":{}\r\n", count).into_bytes()
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => b":0\r\n".to_vec(),
    }
}

pub async fn cmd_zpopmin(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.is_empty() {
        return b"-ERR wrong number of arguments for 'zpopmin' command\r\n".to_vec();
    }
    let key = &args[0];
    let count: usize = if args.len() >= 2 { parse_u64(&args[1]).unwrap_or(1) as usize } else { 1 };

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &mut entry.value {
            Value::ZSet { by_score, by_member } => {
                let to_pop: Vec<(i64, Vec<u8>)> = by_score.iter()
                    .take(count)
                    .map(|((score, member), _)| (*score, member.clone()))
                    .collect();

                for (score, member) in &to_pop {
                    by_score.remove(&(*score, member.clone()));
                    by_member.remove(member);
                }

                if by_member.is_empty() {
                    database.remove(key);
                }

                let mut resp = format!("*{}\r\n", to_pop.len() * 2).into_bytes();
                for (score, member) in to_pop {
                    resp.extend_from_slice(&format!("${}\r\n", member.len()).into_bytes());
                    resp.extend_from_slice(&member);
                    resp.extend_from_slice(b"\r\n");
                    let score_str = i64_to_score_str(score);
                    resp.extend_from_slice(&format!("${}\r\n", score_str.len()).into_bytes());
                    resp.extend_from_slice(score_str.as_bytes());
                    resp.extend_from_slice(b"\r\n");
                }
                resp
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => b"*0\r\n".to_vec(),
    }
}

pub async fn cmd_zpopmax(
    db: Arc<Mutex<ServerDb>>,
    dbi: usize,
    args: &[Vec<u8>],
) -> Vec<u8> {
    if args.is_empty() {
        return b"-ERR wrong number of arguments for 'zpopmax' command\r\n".to_vec();
    }
    let key = &args[0];
    let count: usize = if args.len() >= 2 { parse_u64(&args[1]).unwrap_or(1) as usize } else { 1 };

    let mut server = db.lock().await;
    let database = &mut server.databases[dbi];
    match database.get_mut(key) {
        Some(entry) => match &mut entry.value {
            Value::ZSet { by_score, by_member } => {
                let to_pop: Vec<(i64, Vec<u8>)> = by_score.iter()
                    .rev()
                    .take(count)
                    .map(|((score, member), _)| (*score, member.clone()))
                    .collect();

                for (score, member) in &to_pop {
                    by_score.remove(&(*score, member.clone()));
                    by_member.remove(member);
                }

                if by_member.is_empty() {
                    database.remove(key);
                }

                let mut resp = format!("*{}\r\n", to_pop.len() * 2).into_bytes();
                for (score, member) in to_pop {
                    resp.extend_from_slice(&format!("${}\r\n", member.len()).into_bytes());
                    resp.extend_from_slice(&member);
                    resp.extend_from_slice(b"\r\n");
                    let score_str = i64_to_score_str(score);
                    resp.extend_from_slice(&format!("${}\r\n", score_str.len()).into_bytes());
                    resp.extend_from_slice(score_str.as_bytes());
                    resp.extend_from_slice(b"\r\n");
                }
                resp
            }
            _ => b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        },
        None => b"*0\r\n".to_vec(),
    }
}

fn parse_f64_as_i64(data: &[u8]) -> i64 {
    let s = String::from_utf8_lossy(data);
    if let Ok(f) = s.trim().parse::<f64>() {
        (f * 1_000_000.0) as i64
    } else if let Ok(i) = s.trim().parse::<i64>() {
        i * 1_000_000
    } else {
        0
    }
}

fn i64_to_score_str(score: i64) -> String {
    if score == 0 {
        return "0".to_string();
    }
    if score % 1_000_000 == 0 {
        return (score / 1_000_000).to_string();
    }
    let whole = score / 1_000_000;
    let frac = (score % 1_000_000).abs();
    let s = format!("{}.{:06}", whole, frac);
    let s = s.trim_end_matches('0');
    let s = s.trim_end_matches('.');
    if s.is_empty() { "0".to_string() } else { s.to_string() }
}

fn parse_score_range(data: &[u8]) -> (i64, bool) {
    let s = String::from_utf8_lossy(data);
    if s == "-inf" { return (i64::MIN, false); }
    if s == "+inf" { return (i64::MAX, false); }
    let exclusive = s.starts_with('(');
    let num_str = if exclusive { &s[1..] } else { &s[..] };
    let val = if let Ok(f) = num_str.parse::<f64>() {
        (f * 1_000_000.0) as i64
    } else {
        num_str.parse::<i64>().unwrap_or(0) * 1_000_000
    };
    (val, exclusive)
}

fn parse_i64(data: &[u8]) -> Result<i64, ()> {
    std::str::from_utf8(data).map_err(|_| ())?.parse::<i64>().map_err(|_| ())
}

fn parse_u64(data: &[u8]) -> Result<u64, ()> {
    std::str::from_utf8(data).map_err(|_| ())?.parse::<u64>().map_err(|_| ())
}

fn normalize_range(start: i64, stop: i64, len: i64) -> (usize, usize) {
    if len == 0 { return (0, 0); }
    let s = if start < 0 { (len + start).max(0) } else { start.min(len - 1) } as usize;
    let e = if stop < 0 { (len + stop).max(0) as usize } else { (stop.min(len - 1)) as usize };
    (s, e)
}
