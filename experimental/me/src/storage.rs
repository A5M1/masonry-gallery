use std::collections::{BTreeMap, HashSet, VecDeque};
use std::time::Instant;

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub enum Value {
    String(Vec<u8>),
    List(VecDeque<Vec<u8>>),
    Set(HashSet<Vec<u8>>),
    Hash(BTreeMap<Vec<u8>, Vec<u8>>),
    ZSet {
        by_score: BTreeMap<(i64, Vec<u8>), ()>,
        by_member: BTreeMap<Vec<u8>, i64>,
    },
}

impl Value {
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::String(_) => "string",
            Value::List(_) => "list",
            Value::Set(_) => "set",
            Value::Hash(_) => "hash",
            Value::ZSet { .. } => "zset",
        }
    }
}

pub struct Entry {
    pub value: Value,
    pub expires_at: Option<Instant>,
}

impl Entry {
    pub fn new(value: Value, expires_at: Option<Instant>) -> Self {
        Entry { value, expires_at }
    }

    pub fn is_expired(&self) -> bool {
        match self.expires_at {
            Some(exp) => Instant::now() > exp,
            None => false,
        }
    }
}

pub struct Database {
    pub store: BTreeMap<Vec<u8>, Entry>,
}

impl Database {
    pub fn len(&self) -> usize {
        self.store.len()
    }

    pub fn clear(&mut self) {
        self.store.clear();
    }

    pub fn remove_expired(&mut self, key: &[u8]) -> bool {
        if let Some(entry) = self.store.get(key) {
            if entry.is_expired() {
                self.store.remove(key);
                return true;
            }
        }
        false
    }

    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut Entry> {
        if self.remove_expired(key) {
            return None;
        }
        self.store.get_mut(key)
    }

    pub fn get(&self, key: &[u8]) -> Option<&Entry> {
        self.store.get(key)
    }

    pub fn insert(&mut self, key: Vec<u8>, value: Value, expires_at: Option<Instant>) {
        self.store.insert(key, Entry::new(value, expires_at));
    }

    pub fn remove(&mut self, key: &[u8]) -> Option<Entry> {
        self.store.remove(key)
    }
}

pub struct ServerDb {
    pub databases: [Database; 16],
    pub start_time: Instant,
    pub pubsub: crate::pubsub::PubSub,
    pub aof: crate::aof::Aof,
    pub last_save: Instant,
}

impl ServerDb {
    pub fn new() -> Self {
        ServerDb {
            databases: [
                Database { store: BTreeMap::new() },
                Database { store: BTreeMap::new() },
                Database { store: BTreeMap::new() },
                Database { store: BTreeMap::new() },
                Database { store: BTreeMap::new() },
                Database { store: BTreeMap::new() },
                Database { store: BTreeMap::new() },
                Database { store: BTreeMap::new() },
                Database { store: BTreeMap::new() },
                Database { store: BTreeMap::new() },
                Database { store: BTreeMap::new() },
                Database { store: BTreeMap::new() },
                Database { store: BTreeMap::new() },
                Database { store: BTreeMap::new() },
                Database { store: BTreeMap::new() },
                Database { store: BTreeMap::new() },
            ],
            pubsub: crate::pubsub::PubSub::new(),
            start_time: Instant::now(),
            aof: crate::aof::Aof::new(),
            last_save: Instant::now(),
        }
    }
}
