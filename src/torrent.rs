//! Extracts the `BitTorrent` v1 info hash from bounded `.torrent` metadata.

use anyhow::{Context, Result};
use data_encoding::HEXLOWER;
use sha1::{Digest, Sha1};

const MAX_BENCODE_DEPTH: usize = 64;

pub(crate) fn v1_info_hash(bytes: &[u8]) -> Result<String> {
    let mut cursor = Cursor::new(bytes);
    cursor
        .expect(b'd')
        .context("torrent root must be a dictionary")?;
    let mut info = None;
    while cursor.peek()? != b'e' {
        let key = cursor
            .read_bytes()
            .context("invalid torrent dictionary key")?;
        let value_start = cursor.position;
        cursor.skip_value(1)?;
        if key == b"info" {
            if info.is_some() {
                anyhow::bail!("torrent contains more than one info dictionary");
            }
            if bytes.get(value_start) != Some(&b'd') {
                anyhow::bail!("torrent info value must be a dictionary");
            }
            info = Some(&bytes[value_start..cursor.position]);
        }
    }
    cursor.expect(b'e')?;
    if cursor.position != bytes.len() {
        anyhow::bail!("torrent contains trailing data");
    }
    let info = info.context("torrent contains no info dictionary")?;
    Ok(HEXLOWER.encode(&Sha1::digest(info)))
}

struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn peek(&self) -> Result<u8> {
        self.bytes
            .get(self.position)
            .copied()
            .context("unexpected end of bencoded data")
    }

    fn expect(&mut self, expected: u8) -> Result<()> {
        if self.peek()? != expected {
            anyhow::bail!("unexpected bencode token");
        }
        self.position += 1;
        Ok(())
    }

    fn read_bytes(&mut self) -> Result<&'a [u8]> {
        let length_start = self.position;
        while self.peek()?.is_ascii_digit() {
            self.position += 1;
        }
        if self.position == length_start || self.peek()? != b':' {
            anyhow::bail!("invalid bencode byte string length");
        }
        if self.position - length_start > 1 && self.bytes[length_start] == b'0' {
            anyhow::bail!("non-canonical bencode byte string length");
        }
        let length = std::str::from_utf8(&self.bytes[length_start..self.position])?
            .parse::<usize>()
            .context("bencode byte string length overflow")?;
        self.position += 1;
        let end = self
            .position
            .checked_add(length)
            .context("bencode byte string length overflow")?;
        let value = self
            .bytes
            .get(self.position..end)
            .context("bencode byte string exceeds input")?;
        self.position = end;
        Ok(value)
    }

    fn skip_value(&mut self, depth: usize) -> Result<()> {
        if depth > MAX_BENCODE_DEPTH {
            anyhow::bail!("bencode nesting exceeds {MAX_BENCODE_DEPTH}");
        }
        match self.peek()? {
            b'i' => self.skip_integer(),
            b'l' => self.skip_list(depth),
            b'd' => self.skip_dictionary(depth),
            byte if byte.is_ascii_digit() => self.read_bytes().map(|_| ()),
            _ => anyhow::bail!("invalid bencode value"),
        }
    }

    fn skip_integer(&mut self) -> Result<()> {
        self.expect(b'i')?;
        let start = self.position;
        if self.peek()? == b'-' {
            self.position += 1;
        }
        let digits = self.position;
        while self.peek()?.is_ascii_digit() {
            self.position += 1;
        }
        if self.position == digits || self.peek()? != b'e' {
            anyhow::bail!("invalid bencode integer");
        }
        let encoded = &self.bytes[start..self.position];
        if encoded == b"-0"
            || (encoded.starts_with(b"0") && encoded.len() > 1)
            || (encoded.starts_with(b"-0") && encoded.len() > 2)
        {
            anyhow::bail!("non-canonical bencode integer");
        }
        self.position += 1;
        Ok(())
    }

    fn skip_list(&mut self, depth: usize) -> Result<()> {
        self.expect(b'l')?;
        while self.peek()? != b'e' {
            self.skip_value(depth + 1)?;
        }
        self.expect(b'e')
    }

    fn skip_dictionary(&mut self, depth: usize) -> Result<()> {
        self.expect(b'd')?;
        while self.peek()? != b'e' {
            self.read_bytes()?;
            self.skip_value(depth + 1)?;
        }
        self.expect(b'e')
    }
}

#[cfg(test)]
mod tests {
    use super::v1_info_hash;

    #[test]
    fn hashes_the_exact_encoded_info_dictionary() {
        let torrent = b"d8:announce14:http://tracker4:infod4:name4:testee";
        assert_eq!(
            v1_info_hash(torrent).expect("valid torrent"),
            "1ade8a1a581f338e4fce4ce784da3f7d03f81f3a"
        );
    }

    #[test]
    fn rejects_missing_duplicate_or_trailing_info_data() {
        assert!(v1_info_hash(b"d4:name4:teste").is_err());
        assert!(v1_info_hash(b"d4:infode4:infodee").is_err());
        assert!(v1_info_hash(b"d4:infodeejunk").is_err());
    }
}
