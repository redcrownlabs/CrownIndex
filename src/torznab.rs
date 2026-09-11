//! Reads bounded recent-release feeds from explicitly configured Jackett indexers.

use std::collections::HashSet;
use std::time::Duration;

use anyhow::{Context, Result};
use quick_xml::Reader;
use quick_xml::XmlVersion;
use quick_xml::events::{BytesStart, Event};
use reqwest::Client;
use sha2::{Digest as _, Sha256};
use tokio::time::sleep;
use url::Url;

use crate::config::JackettConfig;
use crate::record::{TorrentRecord, info_hash_from_magnet, normalize_info_hash};
use crate::store::CatalogStore;
use crate::torrent::v1_info_hash;

// Torznab feeds are normally small RSS documents. This cap prevents a broken
// or hostile indexer response from consuming unbounded process memory.
const MAX_FEED_BYTES: usize = 16 * 1024 * 1024;
const MAX_TORRENT_BYTES: usize = 16 * 1024 * 1024;
// DonTorrent currently allows 60 metadata downloads per hour. Keeping ten
// requests in reserve avoids consuming the operator's entire shared quota.
const MAX_NEW_TORRENT_RESOLUTIONS_PER_HOUR: i32 = 50;
// Each indexer causes outbound traffic from Jackett. A small gap avoids
// launching all tracker requests at once while preserving a quick poll cycle.
const INDEXER_REQUEST_DELAY: Duration = Duration::from_secs(1);
const TORRENT_REQUEST_DELAY: Duration = Duration::from_millis(100);
/// Interactive searches must fail fast enough for actionable portal feedback.
///
/// Periodic and historical workers retain the operator-configured timeout;
/// only an explicitly requested search uses this lower latency boundary.
const TARGETED_SEARCH_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug)]
pub(crate) struct FetchBatch {
    pub(crate) records: Vec<TorrentRecord>,
    pub(crate) failures: Vec<IndexerFailure>,
    pub(crate) skipped: usize,
}

#[derive(Debug)]
pub(crate) struct FetchPage {
    pub(crate) records: Vec<TorrentRecord>,
    pub(crate) skipped: usize,
    pub(crate) deferred: usize,
    pub(crate) fingerprint: [u8; 32],
}

#[derive(Debug, Clone)]
enum FeedQuery {
    Recent,
    Year { year: i32, limit: u16 },
    Search { query: String, limit: u16 },
}

#[derive(Debug)]
pub(crate) struct IndexerFailure {
    pub(crate) indexer: String,
    pub(crate) message: String,
}

#[derive(Debug, Clone)]
pub(crate) struct Jackett {
    client: Client,
    config: JackettConfig,
    store: CatalogStore,
}

impl Jackett {
    pub(crate) fn new(config: JackettConfig, store: CatalogStore) -> Result<Self> {
        let client = Client::builder()
            .user_agent(concat!("CrownIndex/", env!("CARGO_PKG_VERSION")))
            .timeout(config.request_timeout)
            .build()
            .context("failed to create Jackett HTTP client")?;
        Ok(Self {
            client,
            config,
            store,
        })
    }

    pub(crate) async fn fetch_recent(&self) -> FetchBatch {
        let mut records = Vec::new();
        let mut failures = Vec::new();
        let mut skipped = 0;
        for (position, indexer) in self.config.indexers.iter().enumerate() {
            if position > 0 {
                sleep(INDEXER_REQUEST_DELAY).await;
            }
            match self.fetch_query(indexer, FeedQuery::Recent).await {
                Ok(feed) => {
                    records.extend(feed.records);
                    skipped += feed.skipped + feed.deferred;
                }
                Err(error) => failures.push(IndexerFailure {
                    indexer: indexer.clone(),
                    message: error.to_string(),
                }),
            }
        }
        let mut unique = HashSet::new();
        records.retain(|record| unique.insert((record.source.clone(), record.info_hash.clone())));
        FetchBatch {
            records,
            failures,
            skipped,
        }
    }

    pub(crate) async fn fetch_year(
        &self,
        indexer: &str,
        year: i32,
        limit: u16,
    ) -> Result<FetchPage> {
        self.fetch_query(indexer, FeedQuery::Year { year, limit })
            .await
    }

    /// Searches one configured indexer without changing its periodic poll state.
    pub(crate) async fn fetch_search(
        &self,
        indexer: &str,
        query: &str,
        limit: u16,
    ) -> Result<FetchPage> {
        self.fetch_query(
            indexer,
            FeedQuery::Search {
                query: query.to_owned(),
                limit,
            },
        )
        .await
    }

    async fn fetch_query(&self, indexer: &str, query: FeedQuery) -> Result<FetchPage> {
        let endpoint = self.query_endpoint(indexer, &query)?;
        let request = self.client.get(endpoint);
        let request = if matches!(&query, FeedQuery::Search { .. }) {
            request.timeout(TARGETED_SEARCH_TIMEOUT)
        } else {
            request
        };
        let response = request.send().await.map_err(|error| {
            anyhow::anyhow!(
                "Jackett transport failed (status available: {})",
                error.status().is_some()
            )
        })?;
        let status = response.status();
        if !status.is_success() {
            anyhow::bail!(
                "Jackett returned HTTP {status}; verify that this indexer is configured and healthy"
            );
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAX_FEED_BYTES as u64)
        {
            anyhow::bail!("Jackett feed exceeded {MAX_FEED_BYTES} bytes");
        }
        let bytes = response.bytes().await.map_err(|error| {
            anyhow::anyhow!(
                "failed to read Jackett feed (timeout: {})",
                error.is_timeout()
            )
        })?;
        if bytes.len() > MAX_FEED_BYTES {
            anyhow::bail!("Jackett feed exceeded {MAX_FEED_BYTES} bytes");
        }
        let mut feed = parse_feed(indexer, &bytes)?;
        let fingerprint = feed_fingerprint(&feed);
        let mut resolution_attempts = 0;
        let mut deferred = 0;
        for pending in feed.pending.drain(..) {
            validate_download_url(&self.config.base_url, &pending.url)?;
            let source = format!("jackett-{indexer}");
            let locator_hash = locator_fingerprint(&pending.url);
            if let Some(info_hash) = self
                .store
                .resolved_jackett_download(&source, &locator_hash)
                .await?
            {
                feed.records.push(TorrentRecord::new(
                    source,
                    info_hash,
                    pending.title,
                    pending.size,
                )?);
                continue;
            }
            if !self
                .store
                .reserve_jackett_resolution(&source, MAX_NEW_TORRENT_RESOLUTIONS_PER_HOUR)
                .await?
            {
                deferred += 1;
                continue;
            }
            if resolution_attempts > 0 {
                sleep(TORRENT_REQUEST_DELAY).await;
            }
            resolution_attempts += 1;
            match self.resolve_torrent(indexer, pending).await {
                Ok(record) => {
                    self.store
                        .remember_jackett_download(
                            &record.source,
                            &locator_hash,
                            &record.info_hash_bytes()?,
                        )
                        .await?;
                    feed.records.push(record);
                }
                Err(_) => deferred += 1,
            }
        }
        Ok(FetchPage {
            records: feed.records,
            skipped: feed.skipped,
            deferred,
            fingerprint,
        })
    }

    fn query_endpoint(&self, indexer: &str, query: &FeedQuery) -> Result<Url> {
        let mut endpoint = self
            .config
            .base_url
            .join(&format!("api/v2.0/indexers/{indexer}/results/torznab/api"))
            .context("failed to construct Jackett Torznab URL")?;
        endpoint
            .query_pairs_mut()
            .append_pair("apikey", self.config.api_key.expose())
            .append_pair("t", "search");
        match query {
            FeedQuery::Recent => {}
            FeedQuery::Year { year, limit } => {
                let year = year.to_string();
                let limit = limit.to_string();
                endpoint
                    .query_pairs_mut()
                    .append_pair("q", &year)
                    .append_pair("limit", &limit);
            }
            FeedQuery::Search { query, limit } => {
                let limit = limit.to_string();
                endpoint
                    .query_pairs_mut()
                    .append_pair("q", query)
                    .append_pair("limit", &limit);
            }
        }
        Ok(endpoint)
    }

    async fn resolve_torrent(
        &self,
        indexer: &str,
        pending: PendingTorrent,
    ) -> Result<TorrentRecord> {
        validate_download_url(&self.config.base_url, &pending.url)?;
        let response = self.client.get(pending.url).send().await.map_err(|error| {
            anyhow::anyhow!(
                "Jackett torrent download failed (timeout: {})",
                error.is_timeout()
            )
        })?;
        let status = response.status();
        if !status.is_success() {
            anyhow::bail!("Jackett torrent download returned HTTP {status}");
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAX_TORRENT_BYTES as u64)
        {
            anyhow::bail!("Jackett torrent download exceeded {MAX_TORRENT_BYTES} bytes");
        }
        let bytes = response
            .bytes()
            .await
            .context("failed to read Jackett torrent download")?;
        if bytes.len() > MAX_TORRENT_BYTES {
            anyhow::bail!("Jackett torrent download exceeded {MAX_TORRENT_BYTES} bytes");
        }
        TorrentRecord::new(
            format!("jackett-{indexer}"),
            v1_info_hash(&bytes)?,
            pending.title,
            pending.size,
        )
    }
}

fn feed_fingerprint(feed: &ParsedFeed) -> [u8; 32] {
    let mut identities = feed
        .records
        .iter()
        .map(|record| {
            let mut identity = Vec::with_capacity(41);
            identity.push(b'h');
            identity.extend_from_slice(record.info_hash.as_bytes());
            identity
        })
        .chain(feed.pending.iter().map(|pending| {
            let mut identity = Vec::with_capacity(33);
            identity.push(b'u');
            identity.extend_from_slice(&locator_fingerprint(&pending.url));
            identity
        }))
        .collect::<Vec<_>>();
    identities.sort_unstable();
    let mut digest = Sha256::new();
    for identity in identities {
        digest.update((identity.len() as u64).to_be_bytes());
        digest.update(identity);
    }
    digest.finalize().into()
}

fn validate_download_url(base_url: &Url, url: &Url) -> Result<()> {
    if url.origin() != base_url.origin() || !url.path().starts_with("/dl/") {
        anyhow::bail!("Torznab download URL is outside the configured Jackett service");
    }
    Ok(())
}

fn locator_fingerprint(url: &Url) -> [u8; 32] {
    let retained_query = url
        .query_pairs()
        .filter(|(key, _)| {
            !matches!(
                key.to_ascii_lowercase().as_ref(),
                "apikey" | "jackett_apikey"
            )
        })
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    let mut normalized = url.clone();
    normalized.set_query(None);
    if !retained_query.is_empty() {
        normalized.query_pairs_mut().extend_pairs(retained_query);
    }
    Sha256::digest(normalized.as_str()).into()
}

#[derive(Debug, Default)]
struct ParsedFeed {
    records: Vec<TorrentRecord>,
    pending: Vec<PendingTorrent>,
    skipped: usize,
}

#[derive(Debug)]
struct PendingTorrent {
    title: String,
    size: u64,
    url: Url,
}

enum FeedEntry {
    Record(TorrentRecord),
    Pending(PendingTorrent),
}

#[derive(Debug, Default)]
struct FeedItem {
    title: String,
    link: String,
    guid: String,
    info_hash: String,
    magnet: String,
    size: String,
}

#[derive(Debug, Clone, Copy)]
enum TextField {
    Title,
    Link,
    Guid,
    Size,
}

fn parse_feed(indexer: &str, xml: &[u8]) -> Result<ParsedFeed> {
    let mut reader = Reader::from_reader(xml);
    reader.config_mut().trim_text(true);
    let mut parsed = ParsedFeed::default();
    let mut item = None::<FeedItem>;
    let mut field = None::<TextField>;
    let mut open_elements = 0_usize;
    loop {
        match reader.read_event().context("invalid Torznab XML")? {
            Event::Start(element) => {
                open_elements = open_elements.saturating_add(1);
                match element.local_name().as_ref() {
                    b"item" => item = Some(FeedItem::default()),
                    b"title" if item.is_some() => field = Some(TextField::Title),
                    b"link" if item.is_some() => field = Some(TextField::Link),
                    b"guid" if item.is_some() => field = Some(TextField::Guid),
                    b"size" if item.is_some() => field = Some(TextField::Size),
                    b"attr" if item.is_some() => {
                        apply_torznab_attr(&reader, &element, item.as_mut())?;
                    }
                    b"enclosure" if item.is_some() => {
                        apply_enclosure(&reader, &element, item.as_mut())?;
                    }
                    _ => {}
                }
            }
            Event::Empty(element) => match element.local_name().as_ref() {
                b"attr" if item.is_some() => apply_torznab_attr(&reader, &element, item.as_mut())?,
                b"enclosure" if item.is_some() => {
                    apply_enclosure(&reader, &element, item.as_mut())?;
                }
                _ => {}
            },
            Event::Text(text) => {
                if let (Some(item), Some(field)) = (item.as_mut(), field) {
                    let value = text
                        .xml_content(XmlVersion::Implicit1_0)
                        .context("invalid Torznab text encoding")?;
                    field_value(item, field).push_str(&value);
                }
            }
            Event::CData(text) => {
                if let (Some(item), Some(field)) = (item.as_mut(), field) {
                    let value = text
                        .xml_content(XmlVersion::Implicit1_0)
                        .context("invalid Torznab CDATA encoding")?;
                    field_value(item, field).push_str(&value);
                }
            }
            Event::End(element) => {
                open_elements = open_elements
                    .checked_sub(1)
                    .context("Torznab XML contains an unmatched closing element")?;
                if element.local_name().as_ref() == b"item" {
                    if let Some(item) = item.take() {
                        match item.into_entry(indexer) {
                            Ok(FeedEntry::Record(record)) => parsed.records.push(record),
                            Ok(FeedEntry::Pending(pending)) => parsed.pending.push(pending),
                            Err(_) => parsed.skipped += 1,
                        }
                    }
                }
                field = None;
            }
            Event::Eof => {
                if open_elements != 0 {
                    anyhow::bail!("Torznab XML ended before all elements were closed");
                }
                break;
            }
            _ => {}
        }
    }
    Ok(parsed)
}

fn field_value(item: &mut FeedItem, field: TextField) -> &mut String {
    match field {
        TextField::Title => &mut item.title,
        TextField::Link => &mut item.link,
        TextField::Guid => &mut item.guid,
        TextField::Size => &mut item.size,
    }
}

fn apply_torznab_attr(
    reader: &Reader<&[u8]>,
    element: &BytesStart<'_>,
    item: Option<&mut FeedItem>,
) -> Result<()> {
    let Some(item) = item else { return Ok(()) };
    let mut name = String::new();
    let mut value = String::new();
    for attribute in element.attributes() {
        let attribute = attribute.context("invalid Torznab attribute")?;
        let decoded = attribute
            .decoded_and_normalized_value(XmlVersion::Implicit1_0, reader.decoder())
            .context("invalid Torznab attribute encoding")?;
        match attribute.key.as_ref() {
            b"name" => name = decoded.into_owned(),
            b"value" => value = decoded.into_owned(),
            _ => {}
        }
    }
    match name.to_ascii_lowercase().as_str() {
        "infohash" => item.info_hash = value,
        "magneturl" => item.magnet = value,
        "size" if item.size.is_empty() => item.size = value,
        _ => {}
    }
    Ok(())
}

fn apply_enclosure(
    reader: &Reader<&[u8]>,
    element: &BytesStart<'_>,
    item: Option<&mut FeedItem>,
) -> Result<()> {
    let Some(item) = item else { return Ok(()) };
    for attribute in element.attributes() {
        let attribute = attribute.context("invalid Torznab enclosure")?;
        if attribute.key.as_ref() == b"url" {
            item.link = attribute
                .decoded_and_normalized_value(XmlVersion::Implicit1_0, reader.decoder())
                .context("invalid Torznab enclosure URL")?
                .into_owned();
        }
    }
    Ok(())
}

impl FeedItem {
    fn into_entry(self, indexer: &str) -> Result<FeedEntry> {
        let title = self.title.trim().to_owned();
        if title.is_empty() {
            anyhow::bail!("Torznab item has no title");
        }
        let size = self.size.trim().parse::<u64>().unwrap_or_default();
        if !self.info_hash.trim().is_empty() {
            return TorrentRecord::new(
                format!("jackett-{indexer}"),
                normalize_info_hash(&self.info_hash)?,
                title,
                size,
            )
            .map(FeedEntry::Record);
        }
        if let Some(info_hash) =
            [&self.magnet, &self.link, &self.guid]
                .into_iter()
                .find_map(|candidate| {
                    let url = Url::parse(candidate.trim()).ok()?;
                    (url.scheme() == "magnet")
                        .then(|| info_hash_from_magnet(&url).ok())
                        .flatten()
                })
        {
            return TorrentRecord::new(format!("jackett-{indexer}"), info_hash, title, size)
                .map(FeedEntry::Record);
        }
        let url = Url::parse(self.link.trim())
            .context("Torznab item has no usable info hash or download URL")?;
        if !matches!(url.scheme(), "http" | "https") {
            anyhow::bail!("Torznab download URL must use HTTP or HTTPS");
        }
        Ok(FeedEntry::Pending(PendingTorrent { title, size, url }))
    }
}

#[cfg(test)]
mod tests {
    use super::{locator_fingerprint, parse_feed};
    use url::Url;

    const FEED: &[u8] = br#"<?xml version="1.0" encoding="UTF-8"?>
      <rss xmlns:torznab="http://torznab.com/schemas/2015/feed"><channel>
        <item><title>Example Linux ISO</title><size>2684354560</size>
          <torznab:attr name="infohash" value="0123456789ABCDEF0123456789ABCDEF01234567" />
        </item>
        <item><title><![CDATA[Second ISO]]></title>
          <torznab:attr name="magneturl" value="magnet:?xt=urn:btih:AERUKZ4JVPG66AJDIVTYTK6N54ASGRLH" />
          <torznab:attr name="size" value="42" />
        </item>
        <item><title>Missing hash</title><size>10</size></item>
        <item><title>Download metadata</title><link>http://jackett:9117/dl/id</link></item>
      </channel></rss>"#;

    #[test]
    fn parses_hash_and_magnet_items_and_counts_invalid_rows() {
        let parsed = parse_feed("yts", FEED).expect("valid feed");
        assert_eq!(parsed.records.len(), 2);
        assert_eq!(parsed.pending.len(), 1);
        assert_eq!(parsed.skipped, 1);
        assert_eq!(parsed.records[0].source, "jackett-yts");
        assert_eq!(parsed.records[0].size, 2_684_354_560);
        assert_eq!(parsed.records[1].name, "Second ISO");
        assert_eq!(parsed.records[1].size, 42);
    }

    #[test]
    fn rejects_malformed_xml() {
        assert!(parse_feed("yts", b"<rss><item>").is_err());
    }

    #[test]
    fn download_fingerprint_excludes_jackett_credentials() {
        let first =
            Url::parse("http://jackett:9117/dl/id?jackett_apikey=one&path=x").expect("valid URL");
        let second =
            Url::parse("http://jackett:9117/dl/id?jackett_apikey=two&path=x").expect("valid URL");
        assert_eq!(locator_fingerprint(&first), locator_fingerprint(&second));
    }
}
