use std::io;

pub const VARINT_CONTINUE: u8 = 0x80;
pub const VARINT_MASK: u8 = 0x7F;

pub fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[derive(Clone)]
pub struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    pub fn with_pos(data: &'a [u8], pos: usize) -> Self {
        Self { data, pos }
    }

    pub fn position(&self) -> usize {
        self.pos
    }

    pub fn set_position(&mut self, pos: usize) -> io::Result<()> {
        if pos > self.data.len() {
            return Err(invalid_data("seek past end of data"));
        }
        self.pos = pos;
        Ok(())
    }

    pub fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    pub fn peek_u8(&self) -> io::Result<u8> {
        self.data
            .get(self.pos)
            .copied()
            .ok_or_else(|| invalid_data("unexpected end of data"))
    }

    pub fn read_u8(&mut self) -> io::Result<u8> {
        let value = self.peek_u8()?;
        self.pos += 1;
        Ok(value)
    }

    pub fn read_bytes(&mut self, len: usize) -> io::Result<&'a [u8]> {
        if len > self.remaining() {
            return Err(invalid_data("unexpected end of data"));
        }
        let out = &self.data[self.pos..self.pos + len];
        self.pos += len;
        Ok(out)
    }

    pub fn skip(&mut self, len: usize) -> io::Result<()> {
        self.read_bytes(len).map(|_| ())
    }

    pub fn read_u32_le(&mut self) -> io::Result<u32> {
        let bytes = self.read_bytes(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub fn read_u64_le(&mut self) -> io::Result<u64> {
        let bytes = self.read_bytes(8)?;
        Ok(u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    pub fn read_varint(&mut self) -> io::Result<u64> {
        let mut result: u64 = 0;
        let mut shift: u32 = 0;
        loop {
            let byte = self.read_u8()?;
            if shift >= 64 {
                return Err(invalid_data("varint overflow"));
            }
            result |= u64::from(byte & VARINT_MASK) << shift;
            shift += 7;
            if byte & VARINT_CONTINUE == 0 {
                return Ok(result);
            }
            if shift > 63 {
                return Err(invalid_data("varint overflow"));
            }
        }
    }

    pub fn read_signed_varint(&mut self) -> io::Result<i64> {
        Ok(zigzag_decode(self.read_varint()?))
    }

    pub fn read_varint_with_size(&mut self) -> io::Result<(u64, usize)> {
        let start = self.pos;
        let value = self.read_varint()?;
        Ok((value, self.pos - start))
    }
}

pub fn zigzag_encode(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

pub fn zigzag_decode(value: u64) -> i64 {
    ((value >> 1) as i64) ^ -((value & 1) as i64)
}

pub fn put_varint(buf: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        buf.push(((value & 0x7F) as u8) | 0x80);
        value >>= 7;
    }
    buf.push((value & 0x7F) as u8);
}

pub fn put_signed_varint(buf: &mut Vec<u8>, value: i64) {
    put_varint(buf, zigzag_encode(value));
}

pub fn put_u32_le(buf: &mut Vec<u8>, value: u32) {
    buf.extend_from_slice(&value.to_le_bytes());
}

pub fn put_u64_le(buf: &mut Vec<u8>, value: u64) {
    buf.extend_from_slice(&value.to_le_bytes());
}

pub fn bits_per_index(table_size: usize) -> u32 {
    let mut bits = 1u32;
    let mut tmp = table_size;
    while tmp > 1 {
        bits += 1;
        tmp >>= 1;
    }
    bits
}

pub fn write_bit_packed_indexes(buf: &mut Vec<u8>, indexes: &[u32], table_size: usize) {
    if indexes.is_empty() {
        return;
    }
    let bits = bits_per_index(table_size) as usize;
    let total_bits = indexes.len() * bits;
    let byte_len = total_bits.div_ceil(8);
    let start = buf.len();
    buf.resize(start + byte_len, 0);
    for (index_pos, index) in indexes.iter().enumerate() {
        let value = *index;
        let base = index_pos * bits;
        for bit in 0..bits {
            let set = value & (1 << (bits - 1 - bit)) != 0;
            if set {
                let absolute = base + bit;
                buf[start + absolute / 8] |= 1 << (7 - (absolute % 8));
            }
        }
    }
}

pub fn read_bit_packed_indexes(
    data: &[u8],
    pos: &mut usize,
    count: usize,
    table_size: usize,
) -> io::Result<Vec<u32>> {
    if count == 0 {
        return Ok(Vec::new());
    }
    let bits = bits_per_index(table_size) as usize;
    let total_bits = count
        .checked_mul(bits)
        .ok_or_else(|| invalid_data("bit-packed index size overflow"))?;
    let byte_len = total_bits.div_ceil(8);
    if pos.saturating_add(byte_len) > data.len() {
        return Err(invalid_data("truncated bit-packed indexes"));
    }
    let mut indexes = Vec::with_capacity(count);
    for index_pos in 0..count {
        let mut value = 0u32;
        let base = index_pos * bits;
        for bit in 0..bits {
            let absolute = base + bit;
            let byte = data[*pos + absolute / 8];
            let set = (byte >> (7 - (absolute % 8))) & 1;
            value = (value << 1) | u32::from(set);
        }
        indexes.push(value);
    }
    *pos += byte_len;
    Ok(indexes)
}

const fn make_crc32_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0usize;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 {
                0xEDB8_8320 ^ (c >> 1)
            } else {
                c >> 1
            };
            k += 1;
        }
        table[i] = c;
        i += 1;
    }
    table
}

const CRC32_TABLE: [u32; 256] = make_crc32_table();

pub fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &byte in data {
        crc = CRC32_TABLE[((crc ^ u32::from(byte)) & 0xFF) as usize] ^ (crc >> 8);
    }
    crc ^ 0xFFFF_FFFF
}

fn f_func(x: u32, y: u32, z: u32) -> u32 {
    (x & y) | ((!x) & z)
}

fn g_func(x: u32, y: u32, z: u32) -> u32 {
    (x & z) | (y & !z)
}

fn h_func(x: u32, y: u32, z: u32) -> u32 {
    x ^ y ^ z
}

fn i_func(x: u32, y: u32, z: u32) -> u32 {
    y ^ (x | !z)
}

pub fn md5(data: &[u8]) -> [u8; 16] {
    let mut a0: u32 = 0x6745_2301;
    let mut b0: u32 = 0xEFCD_AB89;
    let mut c0: u32 = 0x98BA_DCFE;
    let mut d0: u32 = 0x1032_5476;

    let mut message = Vec::with_capacity(data.len() + 72);
    message.extend_from_slice(data);
    message.push(0x80);
    while message.len() % 64 != 56 {
        message.push(0);
    }
    let bit_len = (data.len() as u64).wrapping_mul(8);
    message.extend_from_slice(&bit_len.to_le_bytes());

    let mut chunk = [0u32; 16];
    for block in message.as_chunks::<64>().0 {
        for (i, word) in chunk.iter_mut().enumerate() {
            *word = u32::from_le_bytes([
                block[i * 4],
                block[i * 4 + 1],
                block[i * 4 + 2],
                block[i * 4 + 3],
            ]);
        }

        let mut a = a0;
        let mut b = b0;
        let mut c = c0;
        let mut d = d0;

        let shifts = [
            7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 5, 9, 14, 20, 5, 9, 14, 20,
            5, 9, 14, 20, 5, 9, 14, 20, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23,
            6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21,
        ];
        let constants = [
            0xd76aa478u32,
            0xe8c7b756,
            0x242070db,
            0xc1bdceee,
            0xf57c0faf,
            0x4787c62a,
            0xa8304613,
            0xfd469501,
            0x698098d8,
            0x8b44f7af,
            0xffff5bb1,
            0x895cd7be,
            0x6b901122,
            0xfd987193,
            0xa679438e,
            0x49b40821,
            0xf61e2562,
            0xc040b340,
            0x265e5a51,
            0xe9b6c7aa,
            0xd62f105d,
            0x02441453,
            0xd8a1e681,
            0xe7d3fbc8,
            0x21e1cde6,
            0xc33707d6,
            0xf4d50d87,
            0x455a14ed,
            0xa9e3e905,
            0xfcefa3f8,
            0x676f02d9,
            0x8d2a4c8a,
            0xfffa3942,
            0x8771f681,
            0x6d9d6122,
            0xfde5380c,
            0xa4beea44,
            0x4bdecfa9,
            0xf6bb4b60,
            0xbebfbc70,
            0x289b7ec6,
            0xeaa127fa,
            0xd4ef3085,
            0x04881d05,
            0xd9d4d039,
            0xe6db99e5,
            0x1fa27cf8,
            0xc4ac5665,
            0xf4292244,
            0x432aff97,
            0xab9423a7,
            0xfc93a039,
            0x655b59c3,
            0x8f0ccc92,
            0xffeff47d,
            0x85845dd1,
            0x6fa87e4f,
            0xfe2ce6e0,
            0xa3014314,
            0x4e0811a1,
            0xf7537e82,
            0xbd3af235,
            0x2ad7d2bb,
            0xeb86d391,
        ];

        for i in 0..64 {
            let (f, g) = match i {
                0..=15 => (f_func(b, c, d), i),
                16..=31 => (g_func(b, c, d), (5 * i + 1) % 16),
                32..=47 => (h_func(b, c, d), (3 * i + 5) % 16),
                _ => (i_func(b, c, d), (7 * i) % 16),
            };
            let tmp = d;
            d = c;
            c = b;
            b = b.wrapping_add(
                a.wrapping_add(f)
                    .wrapping_add(constants[i])
                    .wrapping_add(chunk[g])
                    .rotate_left(shifts[i]),
            );
            a = tmp;
        }

        a0 = a0.wrapping_add(a);
        b0 = b0.wrapping_add(b);
        c0 = c0.wrapping_add(c);
        d0 = d0.wrapping_add(d);
    }

    let mut out = [0u8; 16];
    out[0..4].copy_from_slice(&a0.to_le_bytes());
    out[4..8].copy_from_slice(&b0.to_le_bytes());
    out[8..12].copy_from_slice(&c0.to_le_bytes());
    out[12..16].copy_from_slice(&d0.to_le_bytes());
    out
}

pub fn md5_file(path: &std::path::Path) -> io::Result<[u8; 16]> {
    let data = std::fs::read(path)?;
    Ok(md5(&data))
}

pub fn fnv1a64(data: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for &byte in data {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

pub fn normalize_path(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    let separator = std::path::MAIN_SEPARATOR;
    let mut previous_separator = false;
    for ch in path.chars() {
        let ch = if separator == '\\' && ch == '/' {
            '\\'
        } else if separator == '/' && ch == '\\' {
            '/'
        } else {
            ch
        };
        if ch == separator {
            if previous_separator {
                continue;
            }
            previous_separator = true;
        } else {
            previous_separator = false;
        }
        out.push(ch);
    }
    out
}

pub fn path_components(path: &str) -> Vec<String> {
    let normalized = normalize_path(path);
    normalized
        .split(['/', '\\'])
        .filter(|part| !part.is_empty())
        .map(|part| part.to_string())
        .collect()
}

pub fn path_component_suffixes(path: &str) -> Vec<String> {
    let normalized = normalize_path(path);
    let bytes = normalized.as_bytes();
    let mut suffixes = Vec::new();
    let mut index = 0usize;
    while index < bytes.len() {
        while index < bytes.len() && (bytes[index] == b'/' || bytes[index] == b'\\') {
            index += 1;
        }
        if index >= bytes.len() {
            break;
        }
        let start = index;
        suffixes.push(normalized[start..].to_string());
        while index < bytes.len() && bytes[index] != b'/' && bytes[index] != b'\\' {
            index += 1;
        }
    }
    suffixes
}

pub fn join_components(components: &[String]) -> String {
    components.join(std::path::MAIN_SEPARATOR_STR)
}

pub fn parse_numeric_filename(filename: &str) -> Option<u64> {
    let bytes = filename.as_bytes();
    let mut pos = 0usize;
    while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
        pos += 1;
    }
    let mut negative = false;
    if pos < bytes.len() && (bytes[pos] == b'+' || bytes[pos] == b'-') {
        negative = bytes[pos] == b'-';
        pos += 1;
    }
    let digits_start = pos;
    let mut value: u64 = 0;
    let mut overflowed = false;
    while pos < bytes.len() && bytes[pos].is_ascii_digit() {
        let digit = u64::from(bytes[pos] - b'0');
        let (next, overflow) = value.overflowing_mul(10);
        let (next, overflow2) = next.overflowing_add(digit);
        if overflow || overflow2 {
            overflowed = true;
        }
        value = next;
        pos += 1;
    }
    if pos == digits_start || pos != bytes.len() {
        return None;
    }
    if overflowed {
        value = u64::MAX;
    }
    if negative {
        value = 0u64.wrapping_sub(value);
    }
    Some(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_round_trip() {
        for value in [0u64, 1, 127, 128, 300, u32::MAX as u64, u64::MAX] {
            let mut buf = Vec::new();
            put_varint(&mut buf, value);
            let mut reader = Reader::new(&buf);
            assert_eq!(reader.read_varint().unwrap(), value);
            assert_eq!(reader.position(), buf.len());
        }
    }

    #[test]
    fn signed_varint_round_trip() {
        for value in [0i64, 1, -1, 200, -200, i64::MAX, i64::MIN] {
            let mut buf = Vec::new();
            put_signed_varint(&mut buf, value);
            let mut reader = Reader::new(&buf);
            assert_eq!(reader.read_signed_varint().unwrap(), value);
        }
    }

    #[test]
    fn bit_packing_round_trip() {
        for table_size in [0usize, 1, 2, 3, 7, 8, 9, 100] {
            let capacity = 1u32 << bits_per_index(table_size).min(31);
            let indexes: Vec<u32> = (0..17).map(|v| (v * 3) as u32 % capacity).collect();
            let mut buf = Vec::new();
            write_bit_packed_indexes(&mut buf, &indexes, table_size);
            let mut pos = 0;
            let decoded =
                read_bit_packed_indexes(&buf, &mut pos, indexes.len(), table_size).unwrap();
            assert_eq!(decoded, indexes);
            assert_eq!(pos, buf.len());
        }
    }

    #[test]
    fn crc_matches_known_value() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn md5_matches_known_values() {
        assert_eq!(
            md5(b""),
            [
                0xd4, 0x1d, 0x8c, 0xd9, 0x8f, 0x00, 0xb2, 0x04, 0xe9, 0x80, 0x09, 0x98, 0xec, 0xf8,
                0x42, 0x7e
            ]
        );
        assert_eq!(
            md5(b"abc"),
            [
                0x90, 0x01, 0x50, 0x98, 0x3c, 0xd2, 0x4f, 0xb0, 0xd6, 0x96, 0x3f, 0x7d, 0x28, 0xe1,
                0x7f, 0x72
            ]
        );
    }

    #[test]
    fn numeric_filename_matches_c_style() {
        assert_eq!(parse_numeric_filename("123"), Some(123));
        assert_eq!(parse_numeric_filename("  +123"), Some(123));
        assert_eq!(parse_numeric_filename("-1"), Some(u64::MAX));
        assert_eq!(parse_numeric_filename("12x"), None);
        assert_eq!(parse_numeric_filename(""), None);
    }
}
