//! Sends normalized torrent observations through Bitmagnet's supported import API.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use reqwest::Client;
use serde::Serialize;
use url::Url;

use crate::record::TorrentRecord;

const MAX_ERROR_BODY_BYTES: usize = 16 * 1024;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ImportRecord<'a> {
    source: &'a str,
    info_hash: &'a str,
    name: &'a str,
    size: u64,
}

pub(crate) async fn import_records(base_url: &Url, records: &[TorrentRecord]) -> Result<usize> {
    if records.is_empty() {
        return Ok(0);
    }
    let endpoint = base_url
        .join("import")
        .context("failed to construct Bitmagnet import URL")?;
    let payload = encode_json_lines(records)?;
    let import_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_millis();
    let response = Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .context("failed to create Bitmagnet client")?
        .post(endpoint)
        .header("content-type", "application/x-ndjson")
        .header("x-import-id", format!("crown-index-{import_id}"))
        .body(payload)
        .send()
        .await
        .context("Bitmagnet import request failed")?;
    let status = response.status();
    if !status.is_success() {
        let bytes = response
            .bytes()
            .await
            .context("failed to read Bitmagnet error response")?;
        let truncated = &bytes[..bytes.len().min(MAX_ERROR_BODY_BYTES)];
        let body = String::from_utf8_lossy(truncated);
        anyhow::bail!("Bitmagnet import returned {status}: {body}");
    }
    Ok(records.len())
}

fn encode_json_lines(records: &[TorrentRecord]) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    for record in records {
        serde_json::to_writer(
            &mut output,
            &ImportRecord {
                source: &record.source,
                info_hash: &record.info_hash,
                name: &record.name,
                size: record.size,
            },
        )
        .context("failed to serialize Bitmagnet import record")?;
        output.push(b'\n');
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::encode_json_lines;
    use crate::record::TorrentRecord;

    #[test]
    fn creates_bitmagnet_ndjson() {
        let payload = encode_json_lines(&[TorrentRecord {
            source: "ext".to_owned(),
            info_hash: "0123456789abcdef0123456789abcdef01234567".to_owned(),
            name: "Example Linux ISO".to_owned(),
            size: 42,
        }])
        .expect("payload should serialize");
        let line: serde_json::Value = serde_json::from_slice(&payload).expect("valid JSON line");
        assert_eq!(line["source"], "ext");
        assert_eq!(line["infoHash"], "0123456789abcdef0123456789abcdef01234567");
        assert_eq!(line["size"], 42);
    }
}
