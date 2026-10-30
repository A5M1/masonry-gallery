use std::io;

use super::codec::{
    crc32, invalid_data, put_signed_varint, put_u32_le, put_u64_le, put_varint,
    read_bit_packed_indexes, write_bit_packed_indexes, zigzag_decode, Reader,
};

pub const MAGIC_HI: u8 = 0x4D;
pub const MAGIC_LO: u8 = 0x56;
pub const VERSION: u8 = 0x21;
pub const INDEX_MAGIC_HI: u8 = 0x49;
pub const INDEX_MAGIC_LO: u8 = 0x58;

pub const OP_BEGIN: u8 = 0xAA;
pub const OP_END: u8 = 0xFF;
pub const OP_TX_BEGIN: u8 = 0xBB;
pub const OP_TX_END: u8 = 0xCC;
pub const OP_DELETE: u8 = 0xDD;
pub const OP_CHUNK_MARKER: u8 = 0xEE;

pub const EXT_DIMENSIONS_DELTA: u8 = 0x01;
pub const EXT_ORIENTATION: u8 = 0x02;
pub const EXT_CODEC_INFO: u8 = 0x03;
pub const EXT_GPS_COORDS: u8 = 0x04;
pub const EXT_PERCEPTUAL_HASH: u8 = 0x05;

pub const FLAG_HAS_INDEX: u8 = 0x80;
pub const FLAG_TIMESTAMP_PRECISION: u8 = 0x40;
pub const DEFAULT_HASH_SHIFT: u8 = 4;
pub const HASH_MODE_MASK: u8 = 0x03;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum HashMode {
    FullMd5 = 0,
    HalfMd5 = 1,
    Ripemd128 = 2,
    None = 3,
}

impl HashMode {
    pub fn from_bits(bits: u8) -> Self {
        match bits & HASH_MODE_MASK {
            0 => Self::FullMd5,
            1 => Self::HalfMd5,
            2 => Self::Ripemd128,
            _ => Self::None,
        }
    }

    pub fn length(self) -> usize {
        match self {
            Self::HalfMd5 => 8,
            Self::None => 0,
            Self::FullMd5 | Self::Ripemd128 => 16,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::FullMd5 => "FULL_MD5",
            Self::HalfMd5 => "HALF_MD5",
            Self::Ripemd128 => "RIPEMD128",
            Self::None => "NONE",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum MediaType {
    Jpg = 0,
    Png = 1,
    Gif = 2,
    Webp = 3,
    Mp4 = 4,
    Webm = 5,
}

impl MediaType {
    pub fn from_bits(bits: u8) -> Self {
        match bits & 0x07 {
            0 => Self::Jpg,
            1 => Self::Png,
            2 => Self::Gif,
            3 => Self::Webp,
            4 => Self::Mp4,
            5 => Self::Webm,
            _ => Self::Jpg,
        }
    }

    pub fn from_path(path: &str) -> Self {
        let dot = match path.rfind('.') {
            Some(index) => &path[index..],
            None => return Self::Jpg,
        };
        if dot.eq_ignore_ascii_case(".jpg") || dot.eq_ignore_ascii_case(".jpeg") {
            Self::Jpg
        } else if dot.eq_ignore_ascii_case(".png") {
            Self::Png
        } else if dot.eq_ignore_ascii_case(".gif") {
            Self::Gif
        } else if dot.eq_ignore_ascii_case(".webp") {
            Self::Webp
        } else if dot.eq_ignore_ascii_case(".mp4") {
            Self::Mp4
        } else if dot.eq_ignore_ascii_case(".webm") {
            Self::Webm
        } else {
            Self::Jpg
        }
    }

    pub fn extension(self) -> &'static str {
        match self {
            Self::Jpg => ".jpg",
            Self::Png => ".png",
            Self::Gif => ".gif",
            Self::Webp => ".webp",
            Self::Mp4 => ".mp4",
            Self::Webm => ".webm",
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Jpg => "JPG",
            Self::Png => "PNG",
            Self::Gif => "GIF",
            Self::Webp => "WEBP",
            Self::Mp4 => "MP4",
            Self::Webm => "WEBM",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct MetaByte {
    pub media_type: MediaType,
    pub animated: bool,
    pub thumb_mode: u8,
    pub hash_override: bool,
    pub has_extensions: bool,
    pub hash_mode: HashMode,
}

impl MetaByte {
    pub fn encode(self) -> u8 {
        ((self.media_type as u8 & 0x07) << 5)
            | ((self.animated as u8) << 4)
            | ((self.thumb_mode & 0x03) << 2)
            | ((self.hash_override as u8) << 1)
            | (self.has_extensions as u8)
    }

    pub fn decode(byte: u8, default_hash_mode: HashMode) -> Self {
        Self {
            media_type: MediaType::from_bits(byte >> 5),
            animated: (byte >> 4) & 1 != 0,
            thumb_mode: (byte >> 2) & 0x03,
            hash_override: (byte >> 1) & 1 != 0,
            has_extensions: byte & 1 != 0,
            hash_mode: default_hash_mode,
        }
    }
}

#[derive(Clone, Debug)]
pub struct FileHeader {
    pub version: u8,
    pub flags: u8,
    pub record_count: u64,
    pub base_timestamp: u64,
    pub dir_table_size: u64,
    pub has_index: bool,
    pub timestamp_precision: u8,
    pub default_hash_mode: HashMode,
}

impl FileHeader {
    pub fn new(base_timestamp: u64, dir_table_size: usize) -> Self {
        Self {
            version: VERSION,
            flags: 0,
            record_count: 0,
            base_timestamp,
            dir_table_size: dir_table_size as u64,
            has_index: false,
            timestamp_precision: 0,
            default_hash_mode: HashMode::FullMd5,
        }
    }

    pub fn from_flags(flags: u8) -> Self {
        Self {
            version: VERSION,
            flags,
            record_count: 0,
            base_timestamp: 0,
            dir_table_size: 0,
            has_index: flags & FLAG_HAS_INDEX != 0,
            timestamp_precision: (flags >> 6) & 1,
            default_hash_mode: HashMode::from_bits((flags >> DEFAULT_HASH_SHIFT) & HASH_MODE_MASK),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Record {
    pub sequence: u64,
    pub filename: String,
    pub timestamp: u64,
    pub meta: MetaByte,
    pub hash: Vec<u8>,
    pub hash_len: usize,
    pub dir_count: usize,
    pub dir_indexes: Vec<u32>,
    pub width: u32,
    pub height: u32,
    pub duration: u32,
    pub crc32: u32,
    pub orientation: u8,
    pub perceptual_hash: [u8; 8],
    pub perceptual_hash_valid: bool,
    pub codec_info: Option<String>,
    pub gps_lat: f64,
    pub gps_lon: f64,
}

impl Record {
    pub fn new(sequence: u64, filename: String, timestamp: u64, meta: MetaByte) -> Self {
        let hash_len = meta.hash_mode.length();
        Self {
            sequence,
            filename,
            timestamp,
            meta,
            hash: vec![0u8; hash_len],
            hash_len,
            dir_count: 0,
            dir_indexes: Vec::new(),
            width: 0,
            height: 0,
            duration: 0,
            crc32: 0,
            orientation: 0,
            perceptual_hash: [0u8; 8],
            perceptual_hash_valid: false,
            codec_info: None,
            gps_lat: 0.0,
            gps_lon: 0.0,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RecordDeltaState {
    pub last_filename_value: u64,
    pub last_timestamp_value: u64,
}

#[derive(Debug)]
pub struct RecordReadError {
    pub error: io::Error,
    pub end_pos: usize,
    pub crc_mismatch: bool,
}

pub fn read_header(reader: &mut Reader<'_>) -> io::Result<(FileHeader, Vec<String>)> {
    let magic_hi = reader.read_u8()?;
    let magic_lo = reader.read_u8()?;
    if magic_hi != MAGIC_HI || magic_lo != MAGIC_LO {
        return Err(invalid_data(format!(
            "invalid magic header 0x{:02X}{:02X}",
            magic_hi, magic_lo
        )));
    }
    let version = reader.read_u8()?;
    let flags = reader.read_u8()?;
    let record_count = reader.read_varint()?;
    let base_timestamp = reader.read_u64_le()?;
    let dir_table_size = reader.read_varint()?;
    if dir_table_size > 100_000 {
        return Err(invalid_data("directory table size exceeds limit"));
    }

    let mut dir_entries: Vec<String> = Vec::with_capacity(dir_table_size as usize);
    for _ in 0..dir_table_size {
        let prefix_len = reader.read_varint()? as usize;
        let suffix_len = reader.read_varint()? as usize;
        let suffix = reader.read_bytes(suffix_len)?;
        let mut bytes = if prefix_len > 0 {
            match dir_entries.last() {
                Some(previous) => {
                    let previous_bytes = previous.as_bytes();
                    let take = prefix_len.min(previous_bytes.len());
                    previous_bytes[..take].to_vec()
                }
                None => Vec::new(),
            }
        } else {
            Vec::new()
        };
        bytes.extend_from_slice(suffix);
        dir_entries.push(String::from_utf8_lossy(&bytes).into_owned());
    }

    let mut header = FileHeader::from_flags(flags);
    header.version = version;
    header.record_count = record_count;
    header.base_timestamp = base_timestamp;
    header.dir_table_size = dir_table_size;
    Ok((header, dir_entries))
}

pub fn write_header(buf: &mut Vec<u8>, header: &FileHeader, dir_entries: &[String]) {
    buf.push(MAGIC_HI);
    buf.push(MAGIC_LO);
    buf.push(header.version);
    buf.push(header.flags);
    put_varint(buf, header.record_count);
    put_u64_le(buf, header.base_timestamp);
    put_varint(buf, dir_entries.len() as u64);

    let mut previous: &[u8] = &[];
    for entry in dir_entries {
        let current = entry.as_bytes();
        let mut prefix_len = 0usize;
        while prefix_len < previous.len()
            && prefix_len < current.len()
            && previous[prefix_len] == current[prefix_len]
        {
            prefix_len += 1;
        }
        let suffix = &current[prefix_len..];
        put_varint(buf, prefix_len as u64);
        put_varint(buf, suffix.len() as u64);
        buf.extend_from_slice(suffix);
        previous = current;
    }
}

pub fn encode_record(
    record: &Record,
    dir_table: &[String],
    state: &mut RecordDeltaState,
) -> io::Result<Vec<u8>> {
    if record.dir_indexes.len() != record.dir_count {
        return Err(invalid_data("directory index count mismatch"));
    }
    let mut buf = Vec::with_capacity(256);
    buf.push(OP_BEGIN);
    put_varint(&mut buf, record.sequence);
    put_varint(&mut buf, record.dir_count as u64);
    write_bit_packed_indexes(&mut buf, &record.dir_indexes, dir_table.len());

    if let Some(filename_value) = super::codec::parse_numeric_filename(&record.filename) {
        if record.sequence == 0 {
            put_varint(&mut buf, filename_value);
        } else {
            let delta = filename_value.wrapping_sub(state.last_filename_value) as i64;
            put_signed_varint(&mut buf, delta);
        }
        state.last_filename_value = filename_value;
    } else {
        buf.push(0);
        let bytes = record.filename.as_bytes();
        let length = bytes.len().min(255);
        put_varint(&mut buf, length as u64);
        buf.extend_from_slice(&bytes[..length]);
    }

    buf.push(record.meta.encode());
    if record.meta.hash_override {
        buf.push(record.meta.hash_mode as u8 & HASH_MODE_MASK);
    }

    let dims_in_extensions = record.meta.has_extensions && record.width > 0 && record.height > 0;
    if record.meta.has_extensions {
        let mut extensions = Vec::new();
        if dims_in_extensions {
            extensions.push(EXT_DIMENSIONS_DELTA);
            extensions.push(0);
        }
        if record.orientation > 0 {
            extensions.push(EXT_ORIENTATION);
            put_varint(&mut extensions, 1);
            extensions.push(record.orientation);
        }
        if let Some(codec) = &record.codec_info {
            if !codec.is_empty() {
                let bytes = codec.as_bytes();
                let length = bytes.len().min(255);
                extensions.push(EXT_CODEC_INFO);
                put_varint(&mut extensions, length as u64);
                extensions.extend_from_slice(&bytes[..length]);
            }
        }
        if record.gps_lat != 0.0 || record.gps_lon != 0.0 {
            extensions.push(EXT_GPS_COORDS);
            put_varint(&mut extensions, 8);
            extensions.extend_from_slice(&(record.gps_lat as f32).to_le_bytes());
            extensions.extend_from_slice(&(record.gps_lon as f32).to_le_bytes());
        }
        if record.perceptual_hash_valid {
            extensions.push(EXT_PERCEPTUAL_HASH);
            put_varint(&mut extensions, 8);
            extensions.extend_from_slice(&record.perceptual_hash);
        }
        put_varint(&mut buf, extensions.len() as u64);
        buf.extend_from_slice(&extensions);
    }

    let timestamp_delta = record.timestamp.wrapping_sub(state.last_timestamp_value) as i64;
    put_signed_varint(&mut buf, timestamp_delta);
    state.last_timestamp_value = record.timestamp;

    if dims_in_extensions {
        put_varint(&mut buf, u64::from(record.width));
        put_varint(&mut buf, u64::from(record.height));
        put_varint(&mut buf, u64::from(record.duration));
    }

    let hash_len = record.hash_len.min(record.hash.len());
    buf.extend_from_slice(&record.hash[..hash_len]);
    if hash_len < record.hash_len {
        buf.resize(buf.len() + (record.hash_len - hash_len), 0);
    }

    let checksum = crc32(&buf);
    put_u32_le(&mut buf, checksum);
    buf.push(OP_END);
    Ok(buf)
}

pub fn decode_record(
    data: &[u8],
    start: usize,
    dir_table: &[String],
    state: &mut RecordDeltaState,
    is_first: bool,
    default_hash_mode: HashMode,
) -> Result<(Record, usize), RecordReadError> {
    let mut reader = Reader::with_pos(data, start);
    let result = decode_record_inner(
        &mut reader,
        data,
        start,
        dir_table,
        state,
        is_first,
        default_hash_mode,
    );
    match result {
        Ok(record) => {
            let end = reader.position();
            Ok((record, end))
        }
        Err(error) => {
            let end_pos = if reader.position() > start {
                reader.position()
            } else {
                start
            };
            Err(RecordReadError {
                error,
                end_pos,
                crc_mismatch: false,
            })
        }
    }
}

fn decode_record_inner(
    reader: &mut Reader<'_>,
    data: &[u8],
    start: usize,
    dir_table: &[String],
    state: &mut RecordDeltaState,
    is_first: bool,
    default_hash_mode: HashMode,
) -> io::Result<Record> {
    let begin = reader.read_u8()?;
    if begin != OP_BEGIN {
        return Err(invalid_data("record does not start with OP_BEGIN"));
    }

    let sequence = reader.read_varint()?;
    let dir_count = reader.read_varint()? as usize;
    if dir_count > 256 {
        return Err(invalid_data("directory index count exceeds 256"));
    }
    let mut pos = reader.position();
    let dir_indexes = read_bit_packed_indexes(data, &mut pos, dir_count, dir_table.len().max(1))?;
    reader.set_position(pos)?;

    let raw_filename_value = reader.read_varint()?;
    let filename = if raw_filename_value == 0 {
        let before = reader.position();
        let string_value = match reader.read_varint_with_size() {
            Ok((length, _)) if length > 0 && length <= 255 => {
                let length = length as usize;
                if reader.remaining() >= length {
                    let bytes = reader.read_bytes(length)?;
                    Some(String::from_utf8_lossy(bytes).into_owned())
                } else {
                    None
                }
            }
            _ => None,
        };
        match string_value {
            Some(value) => value,
            None => {
                reader.set_position(before)?;
                "0".to_string()
            }
        }
    } else if is_first {
        state.last_filename_value = raw_filename_value;
        raw_filename_value.to_string()
    } else {
        let delta = zigzag_decode(raw_filename_value);
        let abs_value = state.last_filename_value.wrapping_add(delta as u64);
        state.last_filename_value = abs_value;
        abs_value.to_string()
    };

    let meta_byte = reader.read_u8()?;
    let mut meta = MetaByte::decode(meta_byte, default_hash_mode);
    if meta.hash_override {
        let hash_mode_byte = reader.read_u8()?;
        meta.hash_mode = HashMode::from_bits(hash_mode_byte);
    }

    let mut dims_in_extensions = false;
    let mut orientation = 0u8;
    let mut perceptual_hash = [0u8; 8];
    let mut perceptual_hash_valid = false;
    let mut codec_info = None;
    let mut gps_lat = 0.0f64;
    let mut gps_lon = 0.0f64;

    if meta.has_extensions {
        let extension_length = reader.read_varint()? as usize;
        if extension_length > 65535 {
            return Err(invalid_data("extension block too large"));
        }
        let extension_end = reader
            .position()
            .checked_add(extension_length)
            .ok_or_else(|| invalid_data("extension length overflow"))?;
        if extension_end > data.len() {
            return Err(invalid_data("truncated extension block"));
        }
        while reader.position() < extension_end {
            let tag = reader.read_u8()?;
            let value_length = reader.read_varint()? as usize;
            match tag {
                EXT_DIMENSIONS_DELTA => {
                    dims_in_extensions = true;
                    if value_length > 0 {
                        reader.skip(value_length)?;
                    }
                }
                EXT_ORIENTATION if value_length == 1 => {
                    orientation = reader.read_u8()?;
                }
                EXT_CODEC_INFO if value_length > 0 && value_length < 256 => {
                    let bytes = reader.read_bytes(value_length)?;
                    codec_info = Some(String::from_utf8_lossy(bytes).into_owned());
                }
                EXT_GPS_COORDS if value_length == 8 => {
                    let lat_bytes = reader.read_bytes(4)?;
                    let lon_bytes = reader.read_bytes(4)?;
                    gps_lat = f32::from_le_bytes([
                        lat_bytes[0],
                        lat_bytes[1],
                        lat_bytes[2],
                        lat_bytes[3],
                    ]) as f64;
                    gps_lon = f32::from_le_bytes([
                        lon_bytes[0],
                        lon_bytes[1],
                        lon_bytes[2],
                        lon_bytes[3],
                    ]) as f64;
                }
                EXT_PERCEPTUAL_HASH if value_length == 8 => {
                    perceptual_hash.copy_from_slice(reader.read_bytes(8)?);
                    perceptual_hash_valid = true;
                }
                _ => {
                    reader.skip(value_length)?;
                }
            }
        }
    }

    let timestamp_delta = reader.read_signed_varint()?;
    let timestamp = state
        .last_timestamp_value
        .wrapping_add(timestamp_delta as u64);
    state.last_timestamp_value = timestamp;

    let mut width = 0u32;
    let mut height = 0u32;
    let mut duration = 0u32;
    if dims_in_extensions {
        width = reader.read_varint()? as u32;
        height = reader.read_varint()? as u32;
        duration = reader.read_varint()? as u32;
    }

    let hash_len = meta.hash_mode.length();
    let hash = reader.read_bytes(hash_len)?.to_vec();

    let crc_start = start;
    let crc_end = reader.position();
    let stored_crc = reader.read_u32_le()?;
    let end = reader.read_u8()?;
    if end != OP_END {
        return Err(invalid_data("record does not end with OP_END"));
    }
    let computed_crc = crc32(&data[crc_start..crc_end]);
    if computed_crc != stored_crc {
        return Err(invalid_data(format!(
            "CRC32 mismatch for record sequence {}",
            sequence
        )));
    }

    Ok(Record {
        sequence,
        filename,
        timestamp,
        meta,
        hash,
        hash_len,
        dir_count,
        dir_indexes,
        width,
        height,
        duration,
        crc32: stored_crc,
        orientation,
        perceptual_hash,
        perceptual_hash_valid,
        codec_info,
        gps_lat,
        gps_lon,
    })
}

fn has_known_media_extension(path: &str) -> bool {
    let Some(dot) = path.rfind('.') else {
        return false;
    };
    let extension = &path[dot..];
    [".jpg", ".jpeg", ".png", ".gif", ".webp", ".mp4", ".webm"]
        .iter()
        .any(|candidate| extension.eq_ignore_ascii_case(candidate))
}

pub fn serialize_record_to_value(record: &Record, dir_table: &[String]) -> Option<String> {
    let entries: Vec<&str> = record
        .dir_indexes
        .iter()
        .map(|&index| dir_table.get(index as usize).map(String::as_str))
        .collect::<Option<Vec<&str>>>()?;

    if let Some(first) = entries.first() {
        if has_known_media_extension(first) {
            return Some((*first).to_string());
        }
    }
    if let Some(last) = entries.last() {
        if has_known_media_extension(last) {
            return Some((*last).to_string());
        }
    }
    if entries.len() > 1
        && entries
            .windows(2)
            .all(|window| window[1].starts_with(window[0]))
    {
        let mut path = (*entries.last()?).to_string();
        path.push(std::path::MAIN_SEPARATOR);
        path.push_str(&record.filename);
        path.push_str(record.meta.media_type.extension());
        return Some(path);
    }

    let mut path = entries.join(std::path::MAIN_SEPARATOR_STR);
    let extension = record.meta.media_type.extension();
    if !path.is_empty() && has_known_media_extension(&path) {
        return Some(path);
    }
    if !path.is_empty() {
        path.push(std::path::MAIN_SEPARATOR);
    }
    path.push_str(&record.filename);
    path.push_str(extension);
    Some(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_record(sequence: u64, filename: &str, timestamp: u64) -> Record {
        let mut record = Record::new(
            sequence,
            filename.to_string(),
            timestamp,
            MetaByte {
                media_type: MediaType::Jpg,
                animated: false,
                thumb_mode: 0,
                hash_override: false,
                has_extensions: false,
                hash_mode: HashMode::HalfMd5,
            },
        );
        record.hash = vec![1, 2, 3, 4, 5, 6, 7, 8];
        record.dir_count = 2;
        record.dir_indexes = vec![0, 1];
        record
    }

    #[test]
    fn header_round_trip() {
        let dirs = vec![
            "C:".to_string(),
            "photos".to_string(),
            "2026".to_string(),
            "photos2".to_string(),
        ];
        let mut header = FileHeader::new(12345, dirs.len());
        header.flags = FLAG_HAS_INDEX | 0x10;
        header.record_count = 7;
        let mut buf = Vec::new();
        write_header(&mut buf, &header, &dirs);
        let mut reader = Reader::new(&buf);
        let (decoded, decoded_dirs) = read_header(&mut reader).unwrap();
        assert_eq!(decoded.record_count, 7);
        assert_eq!(decoded.base_timestamp, 12345);
        assert_eq!(decoded_dirs, dirs);
        assert!(decoded.has_index);
        assert_eq!(decoded.default_hash_mode, HashMode::HalfMd5);
    }

    #[test]
    fn record_round_trip() {
        let dirs = vec!["C:".to_string(), "photos".to_string()];
        let mut write_state = RecordDeltaState {
            last_filename_value: 0,
            last_timestamp_value: 1000,
        };
        let record = sample_record(0, "123", 1050);
        let bytes = encode_record(&record, &dirs, &mut write_state).unwrap();
        let mut read_state = RecordDeltaState {
            last_filename_value: 0,
            last_timestamp_value: 1000,
        };
        let (decoded, end) =
            decode_record(&bytes, 0, &dirs, &mut read_state, true, HashMode::HalfMd5).unwrap();
        assert_eq!(end, bytes.len());
        assert_eq!(decoded.filename, "123");
        assert_eq!(decoded.timestamp, 1050);
        assert_eq!(decoded.dir_indexes, vec![0, 1]);
    }

    #[test]
    fn first_string_record_round_trip() {
        let dirs = vec!["C:".to_string(), "photos".to_string()];
        let mut write_state = RecordDeltaState {
            last_filename_value: 0,
            last_timestamp_value: 1000,
        };
        let record = sample_record(0, "abcdef", 1001);
        let bytes = encode_record(&record, &dirs, &mut write_state).unwrap();
        let mut read_state = RecordDeltaState {
            last_filename_value: 0,
            last_timestamp_value: 1000,
        };
        let (decoded, _) =
            decode_record(&bytes, 0, &dirs, &mut read_state, true, HashMode::HalfMd5).unwrap();
        assert_eq!(decoded.filename, "abcdef");
    }

    #[test]
    fn string_record_round_trip() {
        let dirs = vec!["C:".to_string()];
        let mut write_state = RecordDeltaState {
            last_filename_value: 0,
            last_timestamp_value: 1000,
        };
        let record = sample_record(1, "abcdef", 1001);
        let bytes = encode_record(&record, &dirs, &mut write_state).unwrap();
        let mut read_state = RecordDeltaState {
            last_filename_value: 0,
            last_timestamp_value: 1000,
        };
        let (decoded, _) =
            decode_record(&bytes, 0, &dirs, &mut read_state, false, HashMode::HalfMd5).unwrap();
        assert_eq!(decoded.filename, "abcdef");
    }
}
