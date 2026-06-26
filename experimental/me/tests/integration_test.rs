use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn resp_command(cmd: &[&[u8]]) -> Vec<u8> {
    let mut buf = format!("*{}\r\n", cmd.len()).into_bytes();
    for arg in cmd {
        buf.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        buf.extend_from_slice(arg);
        buf.extend_from_slice(b"\r\n");
    }
    buf
}

async fn send_and_recv(stream: &mut TcpStream, cmd: &[&[u8]]) -> Vec<u8> {
    let req = resp_command(cmd);
    stream.write_all(&req).await.unwrap();
    let mut buf = BytesMut::with_capacity(4096);
    let n = stream.read_buf(&mut buf).await.unwrap();
    if n == 0 { return Vec::new(); }
    buf.to_vec()
}

fn parse_resp_integer(resp: &[u8]) -> Option<i64> {
    let s = std::str::from_utf8(resp).ok()?;
    if s.starts_with(':') {
        s[1..].trim().parse().ok()
    } else {
        None
    }
}

fn parse_resp_bulk(resp: &[u8]) -> Option<Vec<u8>> {
    if resp.starts_with(b"$-1") { return None; }
    let s = std::str::from_utf8(resp).ok()?;
    if !s.starts_with('$') { return None; }
    let crlf = s.find("\r\n")?;
    let len: usize = s[1..crlf].parse().ok()?;
    let start = crlf + 2;
    if start + len > resp.len() { return None; }
    Some(resp[start..start + len].to_vec())
}

fn parse_resp_ok(resp: &[u8]) -> bool {
    resp.starts_with(b"+OK")
}

#[tokio::test]
async fn test_ping() {
    let mut stream = TcpStream::connect("127.0.0.1:6379").await.unwrap();
    let resp = send_and_recv(&mut stream, &[b"PING"]).await;
    assert!(resp.starts_with(b"+PONG"));
}

#[tokio::test]
async fn test_set_get() {
    let mut stream = TcpStream::connect("127.0.0.1:6379").await.unwrap();
    let _ = send_and_recv(&mut stream, &[b"SET", b"testkey", b"hello"]).await;
    let resp = send_and_recv(&mut stream, &[b"GET", b"testkey"]).await;
    assert_eq!(parse_resp_bulk(&resp), Some(b"hello".to_vec()));
}

#[tokio::test]
async fn test_del() {
    let mut stream = TcpStream::connect("127.0.0.1:6379").await.unwrap();
    let _ = send_and_recv(&mut stream, &[b"SET", b"deltest", b"val"]).await;
    let resp = send_and_recv(&mut stream, &[b"DEL", b"deltest"]).await;
    assert_eq!(parse_resp_integer(&resp), Some(1));
    let resp2 = send_and_recv(&mut stream, &[b"GET", b"deltest"]).await;
    assert!(resp2.starts_with(b"$-1"));
}

#[tokio::test]
async fn test_incr() {
    let mut stream = TcpStream::connect("127.0.0.1:6379").await.unwrap();
    let _ = send_and_recv(&mut stream, &[b"SET", b"counter", b"10"]).await;
    let resp = send_and_recv(&mut stream, &[b"INCR", b"counter"]).await;
    assert_eq!(parse_resp_integer(&resp), Some(11));
}

#[tokio::test]
async fn test_list_ops() {
    let mut stream = TcpStream::connect("127.0.0.1:6379").await.unwrap();
    let _ = send_and_recv(&mut stream, &[b"DEL", b"mylist"]).await;
    let _ = send_and_recv(&mut stream, &[b"RPUSH", b"mylist", b"a", b"b", b"c"]).await;
    let resp = send_and_recv(&mut stream, &[b"LLEN", b"mylist"]).await;
    assert_eq!(parse_resp_integer(&resp), Some(3));
    let resp2 = send_and_recv(&mut stream, &[b"LPOP", b"mylist"]).await;
    assert_eq!(parse_resp_bulk(&resp2), Some(b"a".to_vec()));
}

#[tokio::test]
async fn test_set_ops() {
    let mut stream = TcpStream::connect("127.0.0.1:6379").await.unwrap();
    let _ = send_and_recv(&mut stream, &[b"DEL", b"myset"]).await;
    let _ = send_and_recv(&mut stream, &[b"SADD", b"myset", b"x", b"y", b"z"]).await;
    let resp = send_and_recv(&mut stream, &[b"SISMEMBER", b"myset", b"x"]).await;
    assert_eq!(parse_resp_integer(&resp), Some(1));
    let resp2 = send_and_recv(&mut stream, &[b"SISMEMBER", b"myset", b"w"]).await;
    assert_eq!(parse_resp_integer(&resp2), Some(0));
}

#[tokio::test]
async fn test_hash_ops() {
    let mut stream = TcpStream::connect("127.0.0.1:6379").await.unwrap();
    let _ = send_and_recv(&mut stream, &[b"DEL", b"myhash"]).await;
    let _ = send_and_recv(&mut stream, &[b"HSET", b"myhash", b"f1", b"v1"]).await;
    let resp = send_and_recv(&mut stream, &[b"HGET", b"myhash", b"f1"]).await;
    assert_eq!(parse_resp_bulk(&resp), Some(b"v1".to_vec()));
}

#[tokio::test]
async fn test_zset_ops() {
    let mut stream = TcpStream::connect("127.0.0.1:6379").await.unwrap();
    let _ = send_and_recv(&mut stream, &[b"DEL", b"myzset"]).await;
    let _ = send_and_recv(&mut stream, &[b"ZADD", b"myzset", b"1", b"one", b"2", b"two"]).await;
    let resp = send_and_recv(&mut stream, &[b"ZCARD", b"myzset"]).await;
    assert_eq!(parse_resp_integer(&resp), Some(2));
}

#[tokio::test]
async fn test_exists() {
    let mut stream = TcpStream::connect("127.0.0.1:6379").await.unwrap();
    let _ = send_and_recv(&mut stream, &[b"SET", b"extest", b"yes"]).await;
    let resp = send_and_recv(&mut stream, &[b"EXISTS", b"extest"]).await;
    assert_eq!(parse_resp_integer(&resp), Some(1));
    let resp2 = send_and_recv(&mut stream, &[b"EXISTS", b"nonexistent"]).await;
    assert_eq!(parse_resp_integer(&resp2), Some(0));
}

#[tokio::test]
async fn test_keys() {
    let mut stream = TcpStream::connect("127.0.0.1:6379").await.unwrap();
    let _ = send_and_recv(&mut stream, &[b"SET", b"keytest_a", b"1"]).await;
    let _ = send_and_recv(&mut stream, &[b"SET", b"keytest_b", b"2"]).await;
    let resp = send_and_recv(&mut stream, &[b"KEYS", b"keytest_*"]).await;
    let s = std::str::from_utf8(&resp).unwrap();
    assert!(s.contains("keytest_a"));
    assert!(s.contains("keytest_b"));
}

#[tokio::test]
async fn test_expire_ttl() {
    let mut stream = TcpStream::connect("127.0.0.1:6379").await.unwrap();
    let _ = send_and_recv(&mut stream, &[b"SET", b"ttltest", b"val"]).await;
    let _ = send_and_recv(&mut stream, &[b"EXPIRE", b"ttltest", b"100"]).await;
    let resp = send_and_recv(&mut stream, &[b"TTL", b"ttltest"]).await;
    let ttl = parse_resp_integer(&resp).unwrap();
    assert!(ttl > 0 && ttl <= 100);
}

#[tokio::test]
async fn test_select_flushdb() {
    let mut stream = TcpStream::connect("127.0.0.1:6379").await.unwrap();
    let resp = send_and_recv(&mut stream, &[b"SELECT", b"1"]).await;
    assert!(parse_resp_ok(&resp));
    let _ = send_and_recv(&mut stream, &[b"SET", b"db1key", b"val"]).await;
    let _ = send_and_recv(&mut stream, &[b"SELECT", b"0"]).await;
    let resp2 = send_and_recv(&mut stream, &[b"GET", b"db1key"]).await;
    assert!(resp2.starts_with(b"$-1"));
}

#[tokio::test]
async fn test_type_cmd() {
    let mut stream = TcpStream::connect("127.0.0.1:6379").await.unwrap();
    let _ = send_and_recv(&mut stream, &[b"SET", b"typestr", b"x"]).await;
    let resp = send_and_recv(&mut stream, &[b"TYPE", b"typestr"]).await;
    assert!(std::str::from_utf8(&resp).unwrap().contains("string"));
}

#[tokio::test]
async fn test_mset_mget() {
    let mut stream = TcpStream::connect("127.0.0.1:6379").await.unwrap();
    let _ = send_and_recv(&mut stream, &[b"MSET", b"k1", b"v1", b"k2", b"v2"]).await;
    let resp = send_and_recv(&mut stream, &[b"MGET", b"k1", b"k2"]).await;
    let s = std::str::from_utf8(&resp).unwrap();
    assert!(s.contains("v1"));
    assert!(s.contains("v2"));
}

#[tokio::test]
async fn test_echo() {
    let mut stream = TcpStream::connect("127.0.0.1:6379").await.unwrap();
    let resp = send_and_recv(&mut stream, &[b"ECHO", b"hello world"]).await;
    assert_eq!(parse_resp_bulk(&resp), Some(b"hello world".to_vec()));
}

#[tokio::test]
async fn test_time() {
    let mut stream = TcpStream::connect("127.0.0.1:6379").await.unwrap();
    let resp = send_and_recv(&mut stream, &[b"TIME"]).await;
    assert!(resp.starts_with(b"*2"));
}
