use std::path::Path;

pub mod db;

pub use self::db::{MediaVaultDB, ThumbDb};

pub fn open(path: &Path) -> std::io::Result<ThumbDb> {
    ThumbDb::open(path)
}

pub fn create(path: &Path) -> std::io::Result<ThumbDb> {
    ThumbDb::create(path)
}
