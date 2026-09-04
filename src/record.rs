//! Defines validated torrent observations shared by ingestion adapters.

use anyhow::{Context, Result};
use data_encoding::BASE32_NOPAD;
use url::Url;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TorrentRecord {
    pub(crate) source: String,
    pub(crate) info_hash: String,
    pub(crate) name: String,
    pub(crate) size: u64,
}

impl TorrentRecord {
    pub(crate) fn new(
        source: impl Into<String>,
        info_hash: impl AsRef<str>,
        name: impl Into<String>,
        size: u64,
    ) -> Result<Self> {
        let source = source.into().trim().to_owned();
        let name = name.into().trim().to_owned();
        if source.is_empty() {
            anyhow::bail!("torrent source cannot be empty");
        }
        if name.is_empty() {
            anyhow::bail!("torrent name cannot be empty");
        }
        Ok(Self {
            source,
            info_hash: normalize_info_hash(info_hash.as_ref())?,
            name,
            size,
        })
    }

    pub(crate) fn info_hash_bytes(&self) -> Result<[u8; 20]> {
        decode_hex(&self.info_hash)?
            .try_into()
            .map_err(|_bytes| anyhow::anyhow!("validated info hash is not 20 bytes"))
    }
}

pub(crate) fn info_hash_from_magnet(magnet: &Url) -> Result<String> {
    let value = magnet
        .query_pairs()
        .find_map(|(key, value)| {
            (key == "xt" && value.to_ascii_lowercase().starts_with("urn:btih:"))
                .then(|| value[9..].to_owned())
        })
        .context("magnet has no BitTorrent v1 info hash")?;
    normalize_info_hash(&value)
}

pub(crate) fn normalize_info_hash(value: &str) -> Result<String> {
    let value = value.trim();
    let bytes = if value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        decode_hex(value)?
    } else if value.len() == 32 {
        BASE32_NOPAD
            .decode(value.to_ascii_uppercase().as_bytes())
            .context("invalid base32 info hash")?
    } else {
        anyhow::bail!("BitTorrent v1 info hash must be 40 hex or 32 base32 characters");
    };
    if bytes.len() != 20 {
        anyhow::bail!("decoded BitTorrent v1 info hash is not 20 bytes");
    }
    let mut output = String::with_capacity(40);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(output, "{byte:02x}").context("failed to format info hash")?;
    }
    Ok(output)
}

fn decode_hex(value: &str) -> Result<Vec<u8>> {
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let pair = std::str::from_utf8(pair).context("hex info hash is not ASCII")?;
            u8::from_str_radix(pair, 16).context("invalid hexadecimal info hash")
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{info_hash_from_magnet, normalize_info_hash};
    use url::Url;

    #[test]
    fn normalizes_hex_and_base32_hashes() {
        let expected = "0123456789abcdef0123456789abcdef01234567";
        assert_eq!(
            normalize_info_hash(&expected.to_ascii_uppercase()).expect("hex"),
            expected
        );
        assert_eq!(
            normalize_info_hash("AERUKZ4JVPG66AJDIVTYTK6N54ASGRLH").expect("base32"),
            expected
        );
    }

    #[test]
    fn extracts_hash_from_magnet() {
        let magnet =
            Url::parse("magnet:?xt=urn:btih:AERUKZ4JVPG66AJDIVTYTK6N54ASGRLH").expect("URI");
        assert_eq!(
            info_hash_from_magnet(&magnet).expect("hash"),
            "0123456789abcdef0123456789abcdef01234567"
        );
    }
}
