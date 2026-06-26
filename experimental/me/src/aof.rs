use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;

pub struct Aof {
    file: Option<File>,
    enabled: bool,
}

impl Aof {
    pub fn new() -> Self {
        Aof {
            file: None,
            enabled: false,
        }
    }

    pub async fn enable(&mut self, path: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;
        self.file = Some(file);
        self.enabled = true;
        Ok(())
    }

    pub async fn append_raw(&mut self, data: &[u8]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(ref mut file) = self.file {
            file.write_all(data).await?;
            file.flush().await?;
        }
        Ok(())
    }

}

pub async fn load_aof_commands(path: &str) -> Result<Vec<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>> {
    let data = tokio::fs::read(path).await;
    match data {
        Ok(bytes) => {
            let mut cmds = Vec::new();
            let mut pos = 0;
            while pos < bytes.len() {
                if bytes[pos] != b'*' {
                    pos += 1;
                    continue;
                }
                let line_end = match bytes[pos..].iter().position(|&b| b == b'\n') {
                    Some(p) => pos + p + 1,
                    None => break,
                };
                let header = String::from_utf8_lossy(&bytes[pos..line_end]);
                let count: usize = match header[1..].trim().parse() {
                    Ok(c) => c,
                    Err(_) => { pos = line_end; continue; }
                };
                let cmd_end = find_resp_end(&bytes, pos, count);
                if let Some(end) = cmd_end {
                    cmds.push(bytes[pos..end].to_vec());
                    pos = end;
                } else {
                    break;
                }
            }
            Ok(cmds)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(e.into()),
    }
}

fn find_resp_end(data: &[u8], start: usize, count: usize) -> Option<usize> {
    let mut pos = start;
    for _ in 0..count + 1 {
        if pos >= data.len() { return None; }
        if data[pos] == b'*' {
            let line_end = data[pos..].iter().position(|&b| b == b'\n')?;
            let header = String::from_utf8_lossy(&data[pos..pos + line_end + 1]);
            let inner: usize = header[1..].trim().parse().ok()?;
            pos = pos + line_end + 1;
            for _ in 0..inner {
                if pos >= data.len() || data[pos] != b'$' { return None; }
                let dollar_end = data[pos..].iter().position(|&b| b == b'\n')?;
                let len_str = String::from_utf8_lossy(&data[pos + 1..pos + dollar_end]);
                let blen: isize = len_str.trim().parse().ok()?;
                pos = pos + dollar_end + 1;
                if blen < 0 { continue; }
                pos = pos + blen as usize + 2;
            }
        } else if data[pos] == b'$' {
            let dollar_end = data[pos..].iter().position(|&b| b == b'\n')?;
            let len_str = String::from_utf8_lossy(&data[pos + 1..pos + dollar_end]);
            let blen: isize = len_str.trim().parse().ok()?;
            pos = pos + dollar_end + 1;
            if blen >= 0 { pos = pos + blen as usize + 2; }
        } else {
            let line_end = data[pos..].iter().position(|&b| b == b'\n')?;
            pos = pos + line_end + 1;
        }
    }
    Some(pos)
}
