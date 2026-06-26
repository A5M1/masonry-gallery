use std::collections::BTreeMap;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct Config {
    values: BTreeMap<String, String>,
}

impl Config {
    pub fn load(path: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let content = std::fs::read_to_string(Path::new(path))?;
        let mut values = BTreeMap::new();

        for line in content.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') || trimmed.starts_with(';') {
                continue;
            }
            if let Some(eq_pos) = trimmed.find('=') {
                let key = trimmed[..eq_pos].trim().to_lowercase();
                let val = trimmed[eq_pos + 1..].trim().to_string();
                values.insert(key, val);
            } else if let Some(space_pos) = trimmed.find(char::is_whitespace) {
                let key = trimmed[..space_pos].trim().to_lowercase();
                let val = trimmed[space_pos + 1..].trim().to_string();
                values.insert(key, val);
            }
        }

        Ok(Config { values })
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.values.get(&key.to_lowercase()).map(|s| s.as_str())
    }

    pub fn get_or(&self, key: &str, default: &str) -> String {
        self.get(key).unwrap_or(default).to_string()
    }

    pub fn get_int(&self, key: &str, default: i64) -> i64 {
        self.get(key)
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(default)
    }

    pub fn get_bool(&self, key: &str, default: bool) -> bool {
        self.get(key)
            .map(|v| matches!(v.to_lowercase().as_str(), "yes" | "true" | "1"))
            .unwrap_or(default)
    }
}

impl Default for Config {
    fn default() -> Self {
        let mut values = BTreeMap::new();
        values.insert("bind".to_string(), "127.0.0.1".to_string());
        values.insert("port".to_string(), "6379".to_string());
        values.insert("tls-port".to_string(), "0".to_string());
        values.insert("databases".to_string(), "16".to_string());
        values.insert("appendonly".to_string(), "no".to_string());
        values.insert("appendfsync".to_string(), "everysec".to_string());
        Config { values }
    }
}
