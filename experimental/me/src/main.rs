mod aof;
mod cmd_hash;
mod cmd_key;
mod cmd_list;
mod cmd_set;
mod cmd_string;
mod cmd_zset;
mod config;
mod pubsub;
mod storage;

use bytes::{Buf, BytesMut};
use std::io;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, Mutex};

use config::Config;
use storage::ServerDb;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cfg = Config::load("redis.conf").unwrap_or_default();
    let bind = cfg.get_or("bind", "127.0.0.1");
    let port: u16 = cfg.get_int("port", 6379) as u16;
    let tls_port: u16 = cfg.get_int("tls-port", 0) as u16;
    let use_aof = cfg.get_bool("appendonly", false);

    let addr = format!("{}:{}", bind, port);
    let listener = TcpListener::bind(&addr).await?;
    eprintln!("[tiny-redis] Listening on {} (TCP)", addr);

    let db = Arc::new(Mutex::new(ServerDb::new()));

    if use_aof {
        let mut _lock = db.lock().await;
        let _ = _lock.aof.enable("appendonly.aof").await;
    }

    {
        let cmds = aof::load_aof_commands("appendonly.aof").await.unwrap_or_default();
        if !cmds.is_empty() {
            let _server = db.lock().await;
            for cmd_bytes in cmds {
                let mut buf = BytesMut::from(cmd_bytes.as_slice());
                if let Ok(Some(cmd)) = parse_command(&mut buf) {
                    let _ = dispatch_command(
                        db.clone(),
                        &mut 0,
                        &mut ConnectionMode::Normal,
                        &cmd.command,
                        &cmd.args,
                    )
                    .await;
                }
            }
        }
    }

    let _tls_acceptor: Option<tokio_rustls::TlsAcceptor> = if tls_port > 0 {
        let cert_file = cfg.get_or("tls-cert-file", "redis.crt");
        let key_file = cfg.get_or("tls-key-file", "redis.key");
        match load_tls_config(&cert_file, &key_file) {
            Ok(acceptor) => {
                let tls_addr = format!("{}:{}", bind, tls_port);
                let tls_listener = TcpListener::bind(&tls_addr).await?;
                let tls_db = db.clone();
                let tls_acc = acceptor.clone();
                tokio::spawn(async move {
                    loop {
                        let (stream, _) = match tls_listener.accept().await {
                            Ok(s) => s,
                            Err(_) => continue,
                        };
                        let acc = tls_acc.clone();
                        let d = tls_db.clone();
                        tokio::spawn(async move {
                            match acc.accept(stream).await {
                                Ok(tls_stream) => {
                                    let _ = handle_connection(tls_stream, d).await;
                                }
                                Err(_) => {}
                            }
                        });
                    }
                });
                eprintln!("[tiny-redis] Listening on {} (TLS)", tls_addr);
                Some(acceptor)
            }
            Err(e) => {
                eprintln!("[tiny-redis] TLS disabled: {}", e);
                None
            }
        }
    } else {
        None
    };

    loop {
        let (stream, _) = listener.accept().await?;
        let db = db.clone();
        tokio::spawn(async move {
            let _ = handle_connection(stream, db).await;
        });
    }
}

fn load_tls_config(
    cert_file: &str,
    key_file: &str,
) -> Result<tokio_rustls::TlsAcceptor, Box<dyn std::error::Error + Send + Sync>> {
    let certs = rustls_pemfile::certs(&mut io::BufReader::new(std::fs::File::open(cert_file)?))
        .collect::<Result<Vec<_>, _>>()?;
    let key = rustls_pemfile::private_key(&mut io::BufReader::new(std::fs::File::open(key_file)?))?
        .ok_or("no private key found")?;

    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;

    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
}

enum ConnectionMode {
    Normal,
    Subscribed {
        sub_rxs: Vec<broadcast::Receiver<pubsub::Message>>,
    },
}

async fn handle_connection<S>(
    mut stream: S,
    db: Arc<Mutex<ServerDb>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut buf = BytesMut::with_capacity(4096);
    let mut current_db: usize = 0;
    let mut mode = ConnectionMode::Normal;

    loop {
        let _result: Option<()> = match &mut mode {
            ConnectionMode::Normal => {
                let n = stream.read_buf(&mut buf).await?;
                if n == 0 {
                    if buf.is_empty() {
                        return Ok(());
                    } else {
                        return Err("connection reset by peer".into());
                    }
                }
                None
            }
            ConnectionMode::Subscribed { sub_rxs } => {
                tokio::select! {
                    read_result = stream.read_buf(&mut buf) => {
                        match read_result {
                            Ok(0) if buf.is_empty() => return Ok(()),
                            Ok(0) => return Err("connection reset by peer".into()),
                            Ok(_) => None,
                            Err(e) => return Err(e.into()),
                        }
                    }
                    msg_result = async {
                        if let Some(rx) = sub_rxs.first_mut() {
                            rx.recv().await
                        } else {
                            std::future::pending().await
                        }
                    } => {
                        match msg_result {
                            Ok(msg) => {
                                let resp = format_pubsub_message(&msg);
                                stream.write_all(&resp).await?;
                            }
                            Err(broadcast::error::RecvError::Lagged(_)) => {}
                            Err(broadcast::error::RecvError::Closed) => {}
                        }
                        None
                    }
                }
            }
        };

        while let Some(cmd) = parse_command(&mut buf)? {
            let cmd_upper = cmd.command.clone();
            let is_subscribed = matches!(&mode, ConnectionMode::Subscribed { .. });

            if is_subscribed && !is_allowed_subscribed_cmd(&cmd_upper) {
                stream
                    .write_all(b"-ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this state\r\n")
                    .await?;
                continue;
            }

            let (response, mode_update) = dispatch_command(
                db.clone(),
                &mut current_db,
                &mut mode,
                &cmd.command,
                &cmd.args,
            )
            .await;

            if let Some(resp) = response {
                stream.write_all(&resp).await?;
            }

            if is_mutating_command(&cmd.command) {
                let aof_resp = command_to_resp(&cmd.command, &cmd.args);
                let mut s = db.lock().await;
                let _ = s.aof.append_raw(&aof_resp).await;
            }

            if let Some(new_mode) = mode_update {
                mode = new_mode;
            }
        }
    }
}

fn is_allowed_subscribed_cmd(cmd: &[u8]) -> bool {
    matches!(
        cmd,
        b"SUBSCRIBE"
            | b"UNSUBSCRIBE"
            | b"PSUBSCRIBE"
            | b"PUNSUBSCRIBE"
            | b"PING"
            | b"QUIT"
            | b"RESET"
    )
}

fn format_pubsub_message(msg: &pubsub::Message) -> Vec<u8> {
    match &msg.pattern {
        Some(pattern) => {
            let pat_s = String::from_utf8_lossy(pattern);
            let ch_s = String::from_utf8_lossy(&msg.channel);
            let header = format!(
                "*4\r\n$8\r\npmessage\r\n${}\r\n{}\r\n${}\r\n{}\r\n${}\r\n",
                pat_s.len(),
                pat_s,
                ch_s.len(),
                ch_s,
                msg.payload.len()
            );
            let mut resp = header.into_bytes();
            resp.extend_from_slice(&msg.payload);
            resp.extend_from_slice(b"\r\n");
            resp
        }
        None => {
            let ch_s = String::from_utf8_lossy(&msg.channel);
            let header = format!(
                "*3\r\n$7\r\nmessage\r\n${}\r\n{}\r\n${}\r\n",
                ch_s.len(),
                ch_s,
                msg.payload.len()
            );
            let mut resp = header.into_bytes();
            resp.extend_from_slice(&msg.payload);
            resp.extend_from_slice(b"\r\n");
            resp
        }
    }
}

async fn dispatch_command(
    db: Arc<Mutex<ServerDb>>,
    current_db: &mut usize,
    mode: &mut ConnectionMode,
    command: &[u8],
    args: &[Vec<u8>],
) -> (Option<Vec<u8>>, Option<ConnectionMode>) {
    let db_idx = *current_db;

    let response = match command {
        b"PING" => b"+PONG\r\n".to_vec(),

        b"QUIT" => {
            return (Some(b"+OK\r\n".to_vec()), None);
        }

        b"SELECT" => {
            if args.is_empty() {
                b"-ERR wrong number of arguments for 'select' command\r\n".to_vec()
            } else {
                let idx_str = String::from_utf8_lossy(&args[0]);
                match idx_str.parse::<usize>() {
                    Ok(idx) if idx < 16 => {
                        *current_db = idx;
                        b"+OK\r\n".to_vec()
                    }
                    _ => b"-ERR DB index is out of range\r\n".to_vec(),
                }
            }
        }

        b"SUBSCRIBE" => {
            if args.is_empty() {
                return (
                    Some(b"-ERR wrong number of arguments for 'subscribe' command\r\n".to_vec()),
                    None,
                );
            }
            let mut server = db.lock().await;
            let mut responses = Vec::new();
            let mut sub_rxs: Vec<broadcast::Receiver<pubsub::Message>> = Vec::new();

            if let ConnectionMode::Subscribed {
                sub_rxs: existing, ..
            } = mode
            {
                sub_rxs = std::mem::take(existing);
            }

            for channel in args {
                let rx = server.pubsub.subscribe(channel.clone());
                sub_rxs.push(rx);
                let ch_str = String::from_utf8_lossy(channel);
                let resp = format!(
                    "*3\r\n$9\r\nsubscribe\r\n${}\r\n{}\r\n:{}\r\n",
                    channel.len(),
                    ch_str,
                    sub_rxs.len()
                )
                .into_bytes();
                responses.push(resp);
            }
            drop(server);

            let combined: Vec<u8> = responses.into_iter().flatten().collect();
            return (Some(combined), Some(ConnectionMode::Subscribed { sub_rxs }));
        }

        b"UNSUBSCRIBE" => {
            if let ConnectionMode::Subscribed { sub_rxs } = mode {
                let rxs = sub_rxs;
                let mut server = db.lock().await;
                let mut responses = Vec::new();

                if args.is_empty() {
                    let remaining = rxs.len();
                    rxs.clear();
                    responses.push(
                        format!(
                            "*3\r\n$11\r\nunsubscribe\r\n$0\r\n\r\n:{}\r\n",
                            remaining
                        )
                        .into_bytes(),
                    );
                } else {
                    for channel in args {
                        server.pubsub.unsubscribe(channel);
                        let ch_str = String::from_utf8_lossy(channel);
                        rxs.retain(|_| true);
                        responses.push(
                            format!(
                                "*3\r\n$11\r\nunsubscribe\r\n${}\r\n{}\r\n:{}\r\n",
                                channel.len(),
                                ch_str,
                                rxs.len()
                            )
                            .into_bytes(),
                        );
                    }
                }
                drop(server);

                let combined: Vec<u8> = responses.into_iter().flatten().collect();
                if rxs.is_empty() {
                    return (Some(combined), Some(ConnectionMode::Normal));
                }
                return (Some(combined), None);
            }
            b"*3\r\n$11\r\nunsubscribe\r\n$0\r\n\r\n:0\r\n".to_vec()
        }

        b"PSUBSCRIBE" => {
            if args.is_empty() {
                return (
                    Some(
                        b"-ERR wrong number of arguments for 'psubscribe' command\r\n".to_vec(),
                    ),
                    None,
                );
            }
            let mut server = db.lock().await;
            let mut responses = Vec::new();
            let mut sub_rxs: Vec<broadcast::Receiver<pubsub::Message>> = Vec::new();

            if let ConnectionMode::Subscribed {
                sub_rxs: existing, ..
            } = mode
            {
                sub_rxs = std::mem::take(existing);
            }

            for pattern in args {
                let rx = server.pubsub.psubscribe(pattern.clone());
                sub_rxs.push(rx);
                let pat_str = String::from_utf8_lossy(pattern);
                let resp = format!(
                    "*3\r\n$10\r\npsubscribe\r\n${}\r\n{}\r\n:{}\r\n",
                    pattern.len(),
                    pat_str,
                    sub_rxs.len()
                )
                .into_bytes();
                responses.push(resp);
            }
            drop(server);

            let combined: Vec<u8> = responses.into_iter().flatten().collect();
            return (Some(combined), Some(ConnectionMode::Subscribed { sub_rxs }));
        }

        b"PUNSUBSCRIBE" => {
            if let ConnectionMode::Subscribed { sub_rxs } = mode {
                let rxs = sub_rxs;
                let mut server = db.lock().await;
                let mut responses = Vec::new();

                if args.is_empty() {
                    let remaining = rxs.len();
                    rxs.clear();
                    responses.push(
                        format!(
                            "*3\r\n$12\r\npunsubscribe\r\n$0\r\n\r\n:{}\r\n",
                            remaining
                        )
                        .into_bytes(),
                    );
                } else {
                    for pattern in args {
                        server.pubsub.punsubscribe(pattern);
                        let pat_str = String::from_utf8_lossy(pattern);
                        responses.push(
                            format!(
                                "*3\r\n$12\r\npunsubscribe\r\n${}\r\n{}\r\n:{}\r\n",
                                pattern.len(),
                                pat_str,
                                rxs.len()
                            )
                            .into_bytes(),
                        );
                    }
                }
                drop(server);

                let combined: Vec<u8> = responses.into_iter().flatten().collect();
                if rxs.is_empty() {
                    return (Some(combined), Some(ConnectionMode::Normal));
                }
                return (Some(combined), None);
            }
            b"*3\r\n$12\r\npunsubscribe\r\n$0\r\n\r\n:0\r\n".to_vec()
        }

        b"PUBLISH" => {
            if args.len() < 2 {
                b"-ERR wrong number of arguments for 'publish' command\r\n".to_vec()
            } else {
                let mut server = db.lock().await;
                let count = server.pubsub.publish(&args[0], args[1].clone());
                format!(":{}\r\n", count).into_bytes()
            }
        }

        b"PUBSUB" => {
            if args.is_empty() {
                b"-ERR wrong number of arguments for 'pubsub' command\r\n".to_vec()
            } else if args[0].eq_ignore_ascii_case(b"CHANNELS") {
                let pattern = args.get(1);
                let server = db.lock().await;
                let channels: Vec<&Vec<u8>> = server.pubsub.channels.keys().collect();
                let filtered: Vec<&Vec<u8>> = match pattern {
                    Some(pat) => channels
                        .into_iter()
                        .filter(|c| cmd_key::wildcard_match(pat, c))
                        .collect(),
                    None => channels,
                };
                let mut resp = format!("*{}\r\n", filtered.len()).into_bytes();
                for c in filtered {
                    resp.extend_from_slice(&format!("${}\r\n", c.len()).into_bytes());
                    resp.extend_from_slice(c);
                    resp.extend_from_slice(b"\r\n");
                }
                resp
            } else if args[0].eq_ignore_ascii_case(b"NUMSUB") {
                let server = db.lock().await;
                let sub_args = if args.len() > 1 {
                    &args[1..]
                } else {
                    &[] as &[Vec<u8>]
                };
                let subs = server.pubsub.numsub(sub_args);
                let mut resp = format!("*{}\r\n", subs.len() * 2).into_bytes();
                for (ch, count) in subs {
                    resp.extend_from_slice(&format!("${}\r\n", ch.len()).into_bytes());
                    resp.extend_from_slice(&ch);
                    resp.extend_from_slice(b"\r\n");
                    resp.extend_from_slice(format!(":{}\r\n", count).as_bytes());
                }
                resp
            } else if args[0].eq_ignore_ascii_case(b"NUMPAT") {
                let server = db.lock().await;
                let count = server.pubsub.num_patterns();
                format!(":{}\r\n", count).into_bytes()
            } else {
                b"-ERR unknown pubsub subcommand\r\n".to_vec()
            }
        }

        b"FLUSHDB" => {
            let mut server = db.lock().await;
            server.databases[db_idx].clear();
            b"+OK\r\n".to_vec()
        }
        b"FLUSHALL" => {
            let mut server = db.lock().await;
            for dbi in 0..16 {
                server.databases[dbi].clear();
            }
            b"+OK\r\n".to_vec()
        }

        b"DBSIZE" => {
            let server = db.lock().await;
            let len = server.databases[db_idx].len();
            format!(":{}\r\n", len).into_bytes()
        }

        b"RANDOMKEY" => {
            let server = db.lock().await;
            let dbi = &server.databases[db_idx];
            let mut found_key: Option<Vec<u8>> = None;
            for (key, entry) in dbi.store.iter() {
                if entry.is_expired() {
                    continue;
                }
                found_key = Some(key.clone());
                break;
            }
            match found_key {
                Some(k) => {
                    let mut resp = format!("${}\r\n", k.len()).into_bytes();
                    resp.extend_from_slice(&k);
                    resp.extend_from_slice(b"\r\n");
                    resp
                }
                None => b"$-1\r\n".to_vec(),
            }
        }

        b"TYPE" => cmd_key::cmd_type(db, db_idx, args).await,
        b"ECHO" => {
            if args.is_empty() {
                b"$-1\r\n".to_vec()
            } else {
                let val = &args[0];
                let mut resp = format!("${}\r\n", val.len()).into_bytes();
                resp.extend_from_slice(val);
                resp.extend_from_slice(b"\r\n");
                resp
            }
        }

        b"TIME" => {
            use std::time::SystemTime;
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default();
            let secs = format!("{}", now.as_secs());
            let micros = format!("{}", now.subsec_micros());
            format!(
                "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                secs.len(),
                secs,
                micros.len(),
                micros
            )
            .into_bytes()
        }

        b"COMMAND" => b"*0\r\n".to_vec(),

        b"SET" => cmd_string::cmd_set(db, db_idx, args).await,
        b"GET" => cmd_string::cmd_get(db, db_idx, args).await,
        b"MSET" => cmd_string::cmd_mset(db, db_idx, args).await,
        b"MGET" => cmd_string::cmd_mget(db, db_idx, args).await,
        b"SETNX" => cmd_string::cmd_setnx(db, db_idx, args).await,
        b"SETEX" => cmd_string::cmd_setex(db, db_idx, args).await,
        b"PSETEX" => cmd_string::cmd_psetex(db, db_idx, args).await,
        b"GETSET" => cmd_string::cmd_getset(db, db_idx, args).await,
        b"GETDEL" => cmd_string::cmd_getdel(db, db_idx, args).await,
        b"GETEX" => cmd_string::cmd_getex(db, db_idx, args).await,
        b"APPEND" => cmd_string::cmd_append(db, db_idx, args).await,
        b"STRLEN" => cmd_string::cmd_strlen(db, db_idx, args).await,
        b"GETRANGE" => cmd_string::cmd_getrange(db, db_idx, args).await,
        b"SETRANGE" => cmd_string::cmd_setrange(db, db_idx, args).await,
        b"INCR" => cmd_string::cmd_incr(db, db_idx, args).await,
        b"INCRBY" => cmd_string::cmd_incrby(db, db_idx, args).await,
        b"DECR" => cmd_string::cmd_decr(db, db_idx, args).await,
        b"DECRBY" => cmd_string::cmd_decrby(db, db_idx, args).await,
        b"INCRBYFLOAT" => cmd_string::cmd_incrbyfloat(db, db_idx, args).await,

        b"DEL" => cmd_key::cmd_del(db, db_idx, args).await,
        b"EXISTS" => cmd_key::cmd_exists(db, db_idx, args).await,
        b"EXPIRE" => cmd_key::cmd_expire(db, db_idx, args).await,
        b"EXPIREAT" => cmd_key::cmd_expireat(db, db_idx, args).await,
        b"PEXPIRE" => cmd_key::cmd_pexpire(db, db_idx, args).await,
        b"PEXPIREAT" => cmd_key::cmd_pexpireat(db, db_idx, args).await,
        b"PERSIST" => cmd_key::cmd_persist(db, db_idx, args).await,
        b"TTL" => cmd_key::cmd_ttl(db, db_idx, args).await,
        b"PTTL" => cmd_key::cmd_pttl(db, db_idx, args).await,
        b"KEYS" => cmd_key::cmd_keys(db, db_idx, args).await,
        b"RENAME" => cmd_key::cmd_rename(db, db_idx, args).await,
        b"RENAMENX" => cmd_key::cmd_renamenx(db, db_idx, args).await,
        b"COPY" => cmd_key::cmd_copy(db, db_idx, args).await,

        b"LPUSH" => cmd_list::cmd_lpush(db, db_idx, args).await,
        b"RPUSH" => cmd_list::cmd_rpush(db, db_idx, args).await,
        b"LPOP" => cmd_list::cmd_lpop(db, db_idx, args).await,
        b"RPOP" => cmd_list::cmd_rpop(db, db_idx, args).await,
        b"LLEN" => cmd_list::cmd_llen(db, db_idx, args).await,
        b"LRANGE" => cmd_list::cmd_lrange(db, db_idx, args).await,
        b"LINDEX" => cmd_list::cmd_lindex(db, db_idx, args).await,
        b"LSET" => cmd_list::cmd_lset(db, db_idx, args).await,
        b"LREM" => cmd_list::cmd_lrem(db, db_idx, args).await,
        b"LTRIM" => cmd_list::cmd_ltrim(db, db_idx, args).await,
        b"LINSERT" => cmd_list::cmd_linsert(db, db_idx, args).await,
        b"RPOPLPUSH" => cmd_list::cmd_rpoplpush(db, db_idx, args).await,
        b"LMOVE" => cmd_list::cmd_lmove(db, db_idx, args).await,

        b"SADD" => cmd_set::cmd_sadd(db, db_idx, args).await,
        b"SREM" => cmd_set::cmd_srem(db, db_idx, args).await,
        b"SMEMBERS" => cmd_set::cmd_smembers(db, db_idx, args).await,
        b"SISMEMBER" => cmd_set::cmd_sismember(db, db_idx, args).await,
        b"SCARD" => cmd_set::cmd_scard(db, db_idx, args).await,
        b"SDIFF" => cmd_set::cmd_sdiff(db, db_idx, args).await,
        b"SDIFFSTORE" => cmd_set::cmd_sdiffstore(db, db_idx, args).await,
        b"SINTER" => cmd_set::cmd_sinter(db, db_idx, args).await,
        b"SINTERSTORE" => cmd_set::cmd_sinterstore(db, db_idx, args).await,
        b"SUNION" => cmd_set::cmd_sunion(db, db_idx, args).await,
        b"SUNIONSTORE" => cmd_set::cmd_sunionstore(db, db_idx, args).await,
        b"SRANDMEMBER" => cmd_set::cmd_srandmember(db, db_idx, args).await,
        b"SPOP" => cmd_set::cmd_spop(db, db_idx, args).await,
        b"SMOVE" => cmd_set::cmd_smove(db, db_idx, args).await,
        b"SSCAN" => cmd_set::cmd_sscan(db, db_idx, args).await,

        b"HSET" => cmd_hash::cmd_hset(db, db_idx, args).await,
        b"HGET" => cmd_hash::cmd_hget(db, db_idx, args).await,
        b"HDEL" => cmd_hash::cmd_hdel(db, db_idx, args).await,
        b"HEXISTS" => cmd_hash::cmd_hexists(db, db_idx, args).await,
        b"HKEYS" => cmd_hash::cmd_hkeys(db, db_idx, args).await,
        b"HVALS" => cmd_hash::cmd_hvals(db, db_idx, args).await,
        b"HGETALL" => cmd_hash::cmd_hgetall(db, db_idx, args).await,
        b"HLEN" => cmd_hash::cmd_hlen(db, db_idx, args).await,
        b"HINCRBY" => cmd_hash::cmd_hincrby(db, db_idx, args).await,
        b"HINCRBYFLOAT" => cmd_hash::cmd_hincrbyfloat(db, db_idx, args).await,
        b"HMSET" => cmd_hash::cmd_hmset(db, db_idx, args).await,
        b"HMGET" => cmd_hash::cmd_hmget(db, db_idx, args).await,
        b"HSETNX" => cmd_hash::cmd_hsetnx(db, db_idx, args).await,
        b"HSCAN" => cmd_hash::cmd_hscan(db, db_idx, args).await,
        b"HSTRLEN" => cmd_hash::cmd_hstrlen(db, db_idx, args).await,

        b"ZADD" => cmd_zset::cmd_zadd(db, db_idx, args).await,
        b"ZREM" => cmd_zset::cmd_zrem(db, db_idx, args).await,
        b"ZCARD" => cmd_zset::cmd_zcard(db, db_idx, args).await,
        b"ZSCORE" => cmd_zset::cmd_zscore(db, db_idx, args).await,
        b"ZRANK" => cmd_zset::cmd_zrank(db, db_idx, args).await,
        b"ZREVRANK" => cmd_zset::cmd_zrevrank(db, db_idx, args).await,
        b"ZRANGE" => cmd_zset::cmd_zrange(db, db_idx, args).await,
        b"ZREVRANGE" => cmd_zset::cmd_zrevrange(db, db_idx, args).await,
        b"ZRANGEBYSCORE" => cmd_zset::cmd_zrangebyscore(db, db_idx, args).await,
        b"ZREMRANGEBYRANK" => cmd_zset::cmd_zremrangebyrank(db, db_idx, args).await,
        b"ZREMRANGEBYSCORE" => cmd_zset::cmd_zremrangebyscore(db, db_idx, args).await,
        b"ZINCRBY" => cmd_zset::cmd_zincrby(db, db_idx, args).await,
        b"ZCOUNT" => cmd_zset::cmd_zcount(db, db_idx, args).await,
        b"ZPOPMIN" => cmd_zset::cmd_zpopmin(db, db_idx, args).await,
        b"ZPOPMAX" => cmd_zset::cmd_zpopmax(db, db_idx, args).await,

        b"CONFIG" => {
            if args.len() >= 2 && args[0].eq_ignore_ascii_case(b"GET") {
                let param = String::from_utf8_lossy(&args[1]);
                let mut configs = Vec::new();
                if param == "databases" || param == "*" {
                    configs.push(b"databases".to_vec());
                    configs.push(b"16".to_vec());
                }
                if param == "save" || param == "*" {
                    configs.push(b"save".to_vec());
                    configs.push(b"".to_vec());
                }
                if param == "maxmemory" || param == "*" {
                    configs.push(b"maxmemory".to_vec());
                    configs.push(b"0".to_vec());
                }
                build_resp_array(&configs)
            } else if args.len() >= 3 && args[0].eq_ignore_ascii_case(b"SET") {
                b"+OK\r\n".to_vec()
            } else {
                b"-ERR unknown or unsupported CONFIG command\r\n".to_vec()
            }
        }

        b"INFO" => {
            let server = db.lock().await;
            let mut keys_count = 0usize;
            for dbi in 0..16 {
                keys_count += server.databases[dbi].len();
            }
            let uptime = server.start_time.elapsed().as_secs();
            let info_str = format!(
                "# Server\r\nredis_version:7.0.0\r\nos:Rust_BTree\r\nmultiplexing_api:tokio\r\nuptime_in_seconds:{}\r\n\r\n# Keyspace\r\ndb0:keys={},expires=0,avg_ttl=0\r\n\r\n# Pubsub\r\npubsub_channels:{}\r\npubsub_patterns:{}\r\n",
                uptime, keys_count, server.pubsub.num_channels(), server.pubsub.num_patterns()
            );
            format!("${}\r\n{}\r\n", info_str.len(), info_str).into_bytes()
        }

        b"SAVE" | b"BGSAVE" => {
            let server = db.lock().await;
            let mut resp_data = Vec::new();
            for dbi in 0..16 {
                let dbx = &server.databases[dbi];
                if dbi > 0 {
                    let select_cmd = format!("*2\r\n$6\r\nSELECT\r\n$1\r\n{}\r\n", dbi);
                    resp_data.extend_from_slice(select_cmd.as_bytes());
                }
                for (key, entry) in dbx.store.iter() {
                    if entry.is_expired() {
                        continue;
                    }
                    let val_bytes = match &entry.value {
                        storage::Value::String(v) => String::from_utf8_lossy(v).into_owned(),
                        _ => continue,
                    };
                    let key_escaped = format!("${}\r\n", key.len());
                    let key_str = String::from_utf8_lossy(key);
                    let set_cmd = format!(
                        "*3\r\n$3\r\nSET\r\n{}{}\r\n${}\r\n{}\r\n",
                        key_escaped, key_str, val_bytes.len(), val_bytes
                    );
                    resp_data.extend_from_slice(set_cmd.as_bytes());
                }
            }
            let _ = tokio::fs::write("dump.rdb", &resp_data).await;
            drop(server);
            let mut s = db.lock().await;
            s.last_save = std::time::Instant::now();
            b"+OK\r\n".to_vec()
        }

        b"LASTSAVE" => {
            let server = db.lock().await;
            let ts = server
                .last_save
                .duration_since(server.start_time)
                .as_secs() as i64;
            format!(":{}\r\n", ts).into_bytes()
        }

        b"SCAN" => {
            let mut cursor = 0;
            let mut match_pattern: Option<Vec<u8>> = None;
            let mut count = 10;
            if !args.is_empty() {
                if let Ok(c) = String::from_utf8_lossy(&args[0]).parse::<usize>() {
                    cursor = c;
                }
            }
            let mut idx = 1;
            while idx < args.len() {
                if args[idx].eq_ignore_ascii_case(b"MATCH") && idx + 1 < args.len() {
                    match_pattern = Some(args[idx + 1].clone());
                    idx += 2;
                } else if args[idx].eq_ignore_ascii_case(b"COUNT") && idx + 1 < args.len() {
                    if let Ok(c) = String::from_utf8_lossy(&args[idx + 1]).parse::<usize>() {
                        count = c;
                    }
                    idx += 2;
                } else {
                    idx += 1;
                }
            }
            let server = db.lock().await;
            let dbi = &server.databases[db_idx];
            let mut matched_keys = Vec::new();
            let mut skipped = 0usize;
            let mut iter = dbi.store.iter().skip(cursor);
            for (key, entry) in &mut iter {
                if entry.is_expired() {
                    skipped += 1;
                    continue;
                }
                let is_match = match &match_pattern {
                    Some(pattern) => cmd_key::wildcard_match(pattern, key),
                    None => true,
                };
                if is_match {
                    matched_keys.push(key.clone());
                    if matched_keys.len() >= count {
                        break;
                    }
                }
            }
            drop(iter);
            let traversed = cursor + matched_keys.len() + skipped;
            let next_cursor = if traversed >= dbi.store.len() {
                0
            } else {
                traversed
            };
            let mut resp = format!(
                "*2\r\n${}\r\n{}\r\n*{}\r\n",
                next_cursor.to_string().len(),
                next_cursor,
                matched_keys.len()
            )
            .into_bytes();
            for key in matched_keys {
                resp.extend_from_slice(&format!("${}\r\n", key.len()).into_bytes());
                resp.extend_from_slice(&key);
                resp.extend_from_slice(b"\r\n");
            }
            resp
        }

        _ => b"-ERR unknown command\r\n".to_vec(),
    };

    (Some(response), None)
}

struct Command {
    command: Vec<u8>,
    args: Vec<Vec<u8>>,
}

fn parse_command(
    buf: &mut BytesMut,
) -> Result<Option<Command>, Box<dyn std::error::Error + Send + Sync>> {
    if buf.is_empty() {
        return Ok(None);
    }

    if buf[0] != b'*' {
        let pos = match buf.iter().position(|&b| b == b'\n') {
            Some(p) => p + 1,
            None => return Ok(None),
        };
        let line = &buf[..pos];
        let parts: Vec<&[u8]> = line
            .split(|&b| b == b' ' || b == b'\r' || b == b'\n')
            .filter(|p| !p.is_empty())
            .collect();
        if parts.is_empty() {
            buf.advance(pos);
            return Ok(None);
        }
        let mut args: Vec<Vec<u8>> = parts.into_iter().map(|p| p.to_vec()).collect();
        let mut command = args.remove(0);
        command.make_ascii_uppercase();
        buf.advance(pos);
        return Ok(Some(Command { command, args }));
    }

    let idx = match buf.windows(2).position(|w| w == b"\r\n") {
        Some(pos) => pos,
        None => return Ok(None),
    };
    let array_len = std::str::from_utf8(&buf[1..idx])?.parse::<usize>()?;
    let mut current_pos = idx + 2;
    let mut args = Vec::with_capacity(array_len);

    for _ in 0..array_len {
        if current_pos >= buf.len() {
            return Ok(None);
        }
        if buf[current_pos] != b'$' {
            return Err("Protocol error: expected bulk string".into());
        }
        let remaining = &buf[current_pos..];
        let next_line = match remaining.windows(2).position(|w| w == b"\r\n") {
            Some(pos) => pos,
            None => return Ok(None),
        };
        let bulk_len = std::str::from_utf8(&remaining[1..next_line])?.parse::<isize>()?;
        if bulk_len < 0 {
            args.push(Vec::new());
            current_pos += next_line + 2;
            continue;
        }
        let bulk_len = bulk_len as usize;
        let str_start = current_pos + next_line + 2;
        let str_end = str_start + bulk_len;
        if str_end + 2 > buf.len() {
            return Ok(None);
        }
        if &buf[str_end..str_end + 2] != b"\r\n" {
            return Err("Protocol error: missing trailing CRLF".into());
        }
        args.push(buf[str_start..str_end].to_vec());
        current_pos = str_end + 2;
    }

    buf.advance(current_pos);
    if args.is_empty() {
        return Ok(None);
    }
    let mut command = args.remove(0);
    command.make_ascii_uppercase();
    Ok(Some(Command { command, args }))
}

fn is_mutating_command(cmd: &[u8]) -> bool {
    matches!(
        cmd,
        b"SET"
            | b"MSET"
            | b"SETNX"
            | b"SETEX"
            | b"PSETEX"
            | b"GETSET"
            | b"GETDEL"
            | b"DEL"
            | b"EXPIRE"
            | b"EXPIREAT"
            | b"PEXPIRE"
            | b"PEXPIREAT"
            | b"PERSIST"
            | b"RENAME"
            | b"RENAMENX"
            | b"COPY"
            | b"LPUSH"
            | b"RPUSH"
            | b"LPOP"
            | b"RPOP"
            | b"LSET"
            | b"LREM"
            | b"LTRIM"
            | b"LINSERT"
            | b"RPOPLPUSH"
            | b"LMOVE"
            | b"SADD"
            | b"SREM"
            | b"SDIFFSTORE"
            | b"SINTERSTORE"
            | b"SUNIONSTORE"
            | b"SMOVE"
            | b"SPOP"
            | b"HSET"
            | b"HDEL"
            | b"HMSET"
            | b"HSETNX"
            | b"HINCRBY"
            | b"HINCRBYFLOAT"
            | b"ZADD"
            | b"ZREM"
            | b"ZREMRANGEBYRANK"
            | b"ZREMRANGEBYSCORE"
            | b"ZINCRBY"
            | b"ZPOPMIN"
            | b"ZPOPMAX"
            | b"INCR"
            | b"INCRBY"
            | b"DECR"
            | b"DECRBY"
            | b"INCRBYFLOAT"
            | b"APPEND"
            | b"SETRANGE"
            | b"FLUSHDB"
            | b"FLUSHALL"
    )
}

fn command_to_resp(command: &[u8], args: &[Vec<u8>]) -> Vec<u8> {
    let total = 1 + args.len();
    let mut resp = format!("*{}\r\n", total).into_bytes();
    resp.extend_from_slice(&format!("${}\r\n", command.len()).into_bytes());
    resp.extend_from_slice(command);
    resp.extend_from_slice(b"\r\n");
    for arg in args {
        resp.extend_from_slice(&format!("${}\r\n", arg.len()).into_bytes());
        resp.extend_from_slice(arg);
        resp.extend_from_slice(b"\r\n");
    }
    resp
}

fn build_resp_array(items: &[Vec<u8>]) -> Vec<u8> {
    let mut resp = format!("*{}\r\n", items.len()).into_bytes();
    for item in items {
        resp.extend_from_slice(&format!("${}\r\n", item.len()).into_bytes());
        resp.extend_from_slice(item);
        resp.extend_from_slice(b"\r\n");
    }
    resp
}
