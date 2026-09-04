//! Imports and merges complete Butter-compatible catalogs from every endpoint.

use std::collections::HashSet;

use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::time::sleep;
use tracing::{Level, event};
use url::Url;

use crate::config::ButterConfig;
use crate::importer;
use crate::record::{TorrentRecord, info_hash_from_magnet};
use crate::store::{ButterBackfillState, CatalogStore};

const CATALOG_PAGE_SIZE: u16 = 50;
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
/// Prevents a failing endpoint from becoming a tight retry loop.
///
/// The normal per-request timeout already handles slow failures. Five seconds
/// also bounds pressure from endpoints that reject requests immediately while
/// allowing the remaining union members to continue each round.
const ENDPOINT_FAILURE_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    Movie,
    Show,
}

impl Kind {
    pub(crate) const fn state_key(self) -> &'static str {
        match self {
            Self::Movie => "movies",
            Self::Show => "shows",
        }
    }

    pub(crate) const fn content_type(self) -> &'static str {
        match self {
            Self::Movie => "movie",
            Self::Show => "tv_show",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CatalogItem {
    pub(crate) id: String,
    pub(crate) tmdb_id: Option<String>,
    pub(crate) kind: Kind,
    pub(crate) title: String,
    pub(crate) year: Option<i32>,
    pub(crate) synopsis: Option<String>,
    pub(crate) rating: Option<f64>,
    pub(crate) poster: Option<String>,
    pub(crate) fanart: Option<String>,
    pub(crate) genres: Vec<String>,
    pub(crate) torrents: Vec<CatalogTorrent>,
}

#[derive(Debug, Clone)]
pub(crate) struct CatalogTorrent {
    pub(crate) record: TorrentRecord,
    pub(crate) quality: Option<String>,
    pub(crate) seeders: Option<i32>,
    pub(crate) leechers: Option<i32>,
    pub(crate) episode: Option<(u16, u16)>,
    pub(crate) file: Option<String>,
}

#[derive(Debug)]
struct Butter {
    client: Client,
    config: ButterConfig,
}

impl Butter {
    fn new(config: ButterConfig) -> Result<Self> {
        let client = Client::builder()
            .timeout(config.request_timeout)
            .build()
            .context("failed to create Butter catalog client")?;
        Ok(Self { client, config })
    }

    async fn page(&self, endpoint: &Url, kind: Kind, page: u32) -> Result<Vec<CatalogItem>> {
        let resource = kind.state_key();
        let path = format!(
            "{resource}/{page}?sort=last%20added&limit={CATALOG_PAGE_SIZE}&locale=en&contentLocale=en&showAll=1"
        );
        let value = self.get_json(endpoint, &path).await?;
        parse_page(value, kind, &self.import_source())
    }

    async fn show_details(
        &self,
        endpoint: &Url,
        item: &CatalogItem,
    ) -> Result<Vec<CatalogTorrent>> {
        let encoded_id: String = url::form_urlencoded::byte_serialize(item.id.as_bytes()).collect();
        let value = self
            .get_json(
                endpoint,
                &format!("show/{encoded_id}?locale=en&contentLocale=en&showAll=1"),
            )
            .await?;
        parse_show_torrents(&value, item, &self.import_source())
    }

    async fn get_json(&self, endpoint: &Url, path: &str) -> Result<Value> {
        let url = endpoint
            .join(path)
            .context("failed to construct Butter catalog URL")?;
        self.fetch_json(&url).await
    }

    async fn fetch_json(&self, url: &Url) -> Result<Value> {
        let response = self
            .client
            .get(url.clone())
            .send()
            .await
            .context("catalog request failed")?
            .error_for_status()
            .context("catalog returned an error status")?;
        if response
            .content_length()
            .is_some_and(|length| length > MAX_RESPONSE_BYTES as u64)
        {
            anyhow::bail!("catalog response exceeds 8 MiB");
        }
        let bytes = response
            .bytes()
            .await
            .context("failed to read catalog response")?;
        if bytes.len() > MAX_RESPONSE_BYTES {
            anyhow::bail!("catalog response exceeds 8 MiB");
        }
        serde_json::from_slice(&bytes).context("catalog returned invalid JSON")
    }

    fn import_source(&self) -> String {
        format!("butter-{}", self.config.source_id)
    }

    fn checkpoint_source_id(&self, endpoint: &Url) -> String {
        endpoint_checkpoint_id(&self.config.source_id, endpoint)
    }
}

pub(crate) async fn backfill(
    store: &CatalogStore,
    bitmagnet_url: &Url,
    config: ButterConfig,
    max_pages: Option<u32>,
) -> Result<()> {
    let source_id = config.source_id.clone();
    let request_delay = config.request_delay;
    let butter = Butter::new(config)?;
    let mut committed_pages = 0_u32;
    let mut failures = vec![None; butter.config.endpoints.len()];
    let mut active = vec![true; butter.config.endpoints.len()];
    while active.iter().any(|active| *active) {
        for (index, endpoint) in butter.config.endpoints.iter().enumerate() {
            if !active[index] {
                continue;
            }
            if max_pages.is_some_and(|limit| committed_pages >= limit) {
                return finish_backfill(&failures);
            }
            match backfill_endpoint_page(store, bitmagnet_url, &butter, endpoint, &source_id).await
            {
                Ok(BackfillStep::PageCommitted) => {
                    failures[index] = None;
                    committed_pages = committed_pages.saturating_add(1);
                    sleep(request_delay).await;
                }
                Ok(BackfillStep::CheckpointAdvanced) => failures[index] = None,
                Ok(BackfillStep::EndpointComplete) => {
                    failures[index] = None;
                    active[index] = false;
                }
                Err(error) => {
                    let host = endpoint.host_str().unwrap_or("unknown host");
                    event!(
                        name: "butter.endpoint.failed",
                        Level::WARN,
                        server.address = host,
                        error.message = %error,
                        "Butter catalog endpoint failed"
                    );
                    failures[index] = Some(format!("{host}: {error:#}"));
                    sleep(ENDPOINT_FAILURE_RETRY_DELAY).await;
                }
            }
        }
    }
    finish_backfill(&failures)
}

fn finish_backfill(failures: &[Option<String>]) -> Result<()> {
    let failures = failures
        .iter()
        .filter_map(Option::as_deref)
        .collect::<Vec<_>>();
    if failures.is_empty() {
        Ok(())
    } else {
        anyhow::bail!(
            "one or more Butter catalog endpoints failed: {}",
            failures.join("; ")
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BackfillStep {
    PageCommitted,
    CheckpointAdvanced,
    EndpointComplete,
}

async fn backfill_endpoint_page(
    store: &CatalogStore,
    bitmagnet_url: &Url,
    butter: &Butter,
    endpoint: &Url,
    source_id: &str,
) -> Result<BackfillStep> {
    let checkpoint_source_id = butter.checkpoint_source_id(endpoint);
    let movie = store
        .butter_backfill_state(&checkpoint_source_id, Kind::Movie)
        .await?;
    let show = store
        .butter_backfill_state(&checkpoint_source_id, Kind::Show)
        .await?;
    let Some((kind, state)) = least_advanced_partition(movie, show) else {
        return Ok(BackfillStep::EndpointComplete);
    };
    let page = u32::try_from(state.next_page)
        .context("stored Butter page is outside the supported range")?;
    let mut items = butter.page(endpoint, kind, page).await?;
    if items.is_empty() {
        store
            .advance_butter_backfill(&checkpoint_source_id, kind, state.next_page, true)
            .await?;
        event!(
            name: "butter.backfill.kind.completed",
            Level::INFO,
            butter.source.id = source_id,
            butter.endpoint.id = checkpoint_source_id,
            butter.catalog.kind = kind.state_key(),
            "Butter catalog partition completed"
        );
        return Ok(BackfillStep::CheckpointAdvanced);
    }
    if kind == Kind::Show {
        for item in &mut items {
            sleep(butter.config.request_delay).await;
            item.torrents = butter.show_details(endpoint, item).await?;
        }
    }
    let records = unique_records(&items);
    let unseen = store.unseen_records(&records).await?;
    importer::import_records(bitmagnet_url, &unseen).await?;
    store
        .persist_butter_items(source_id, &items)
        .await
        .context("failed to persist Butter catalog page")?;
    let reconciled = store.reconcile_pending_butter_links().await?;
    store.mark_ingested(&unseen).await?;
    store
        .advance_butter_backfill(&checkpoint_source_id, kind, state.next_page, false)
        .await?;
    event!(
        name: "butter.backfill.page.completed",
        Level::INFO,
        butter.source.id = source_id,
        butter.endpoint.id = checkpoint_source_id,
        butter.catalog.kind = kind.state_key(),
        butter.page = state.next_page,
        butter.items = items.len(),
        butter.torrents = records.len(),
        butter.torrents.imported = unseen.len(),
        butter.torrents.reconciled = reconciled,
        "Butter catalog page committed"
    );
    Ok(BackfillStep::PageCommitted)
}

fn least_advanced_partition(
    movie: ButterBackfillState,
    show: ButterBackfillState,
) -> Option<(Kind, ButterBackfillState)> {
    [(Kind::Movie, movie), (Kind::Show, show)]
        .into_iter()
        .filter(|(_, state)| !state.completed)
        .min_by_key(|(_, state)| state.next_page)
}

fn endpoint_checkpoint_id(source_id: &str, endpoint: &Url) -> String {
    let digest = Sha256::digest(endpoint.as_str().as_bytes());
    let fingerprint = data_encoding::HEXLOWER.encode(&digest[..8]);
    format!("{source_id}:{fingerprint}")
}

fn unique_records(items: &[CatalogItem]) -> Vec<TorrentRecord> {
    let mut seen = HashSet::new();
    items
        .iter()
        .flat_map(|item| &item.torrents)
        .filter_map(|torrent| {
            let key = (
                torrent.record.source.clone(),
                torrent.record.info_hash.clone(),
            );
            seen.insert(key).then(|| torrent.record.clone())
        })
        .collect()
}

fn parse_page(value: Value, kind: Kind, import_source: &str) -> Result<Vec<CatalogItem>> {
    let values = match value {
        Value::Array(items) => items,
        Value::Object(mut object) => object
            .remove("results")
            .or_else(|| object.remove(kind.state_key()))
            .and_then(|value| value.as_array().cloned())
            .unwrap_or_default(),
        _ => anyhow::bail!("catalog page must be an array or result object"),
    };
    Ok(values
        .into_iter()
        .filter_map(|value| parse_item(&value, kind, import_source))
        .collect())
}

fn parse_item(value: &Value, kind: Kind, import_source: &str) -> Option<CatalogItem> {
    let object = value.as_object()?;
    let title = string_field(object, &["title", "name"])
        .or_else(|| string_field(object, &["slug"]).map(|value| title_from_slug(&value)));
    let title = title.filter(|value| !value.is_empty())?;
    let id = string_field(object, &["imdb_id", "tvdb_id", "id", "_id"])
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| stable_item_id(&title, integer_field(object.get("year"))));
    let torrents = object.get("torrents").map_or_else(Vec::new, |value| {
        collect_torrents(value, &title, import_source, None)
    });
    let images = object.get("images").and_then(Value::as_object);
    Some(CatalogItem {
        id,
        tmdb_id: integer_field(object.get("tmdb_id"))
            .filter(|value| *value > 0)
            .map(|value| value.to_string()),
        kind,
        title,
        year: integer_field(object.get("year")).and_then(|year| i32::try_from(year).ok()),
        synopsis: string_field(object, &["synopsis", "description", "overview"]),
        rating: rating_field(object.get("rating")),
        poster: images.and_then(|images| string_field(images, &["poster"])),
        fanart: images.and_then(|images| string_field(images, &["fanart", "backdrop"])),
        genres: string_array(object.get("genres").or_else(|| object.get("genre"))),
        torrents,
    })
}

fn parse_show_torrents(
    value: &Value,
    item: &CatalogItem,
    source: &str,
) -> Result<Vec<CatalogTorrent>> {
    let Some(object) = value.as_object() else {
        anyhow::bail!("show detail must be an object");
    };
    let mut torrents = object.get("torrents").map_or_else(Vec::new, |value| {
        collect_torrents(value, &item.title, source, None)
    });
    if let Some(episodes) = object.get("episodes").and_then(Value::as_array) {
        for episode in episodes {
            let Some(episode_object) = episode.as_object() else {
                continue;
            };
            let Some(season) = integer_field(episode_object.get("season"))
                .and_then(|value| u16::try_from(value).ok())
            else {
                continue;
            };
            let Some(number) = integer_field(episode_object.get("episode"))
                .and_then(|value| u16::try_from(value).ok())
            else {
                continue;
            };
            if let Some(value) = episode_object.get("torrents") {
                torrents.extend(collect_torrents(
                    value,
                    &format!("{} S{season:02}E{number:02}", item.title),
                    source,
                    Some((season, number)),
                ));
            }
        }
    }
    Ok(torrents)
}

fn collect_torrents(
    value: &Value,
    fallback_name: &str,
    source: &str,
    episode: Option<(u16, u16)>,
) -> Vec<CatalogTorrent> {
    fn visit(
        value: &Value,
        key: Option<&str>,
        fallback_name: &str,
        source: &str,
        episode: Option<(u16, u16)>,
        output: &mut Vec<CatalogTorrent>,
    ) {
        let Some(object) = value.as_object() else {
            return;
        };
        let locator = string_field(object, &["url", "magnet"]);
        if let Some(locator) = locator
            && let Ok(magnet) = Url::parse(&locator.replace("&amp;", "&"))
            && magnet.scheme() == "magnet"
            && let Ok(info_hash) = info_hash_from_magnet(&magnet)
        {
            let name = string_field(object, &["name", "title", "filename", "file"])
                .unwrap_or_else(|| fallback_name.to_owned());
            if let Ok(record) = TorrentRecord::new(source, info_hash, name, size_field(object)) {
                output.push(CatalogTorrent {
                    record,
                    quality: string_field(object, &["quality"]).or_else(|| key.map(str::to_owned)),
                    seeders: signed_integer_field(object, &["seed", "seeds", "seeders"]),
                    leechers: signed_integer_field(object, &["peer", "peers", "leechers"]),
                    episode,
                    file: string_field(object, &["file", "filename"]),
                });
            }
            return;
        }
        for (child_key, child) in object {
            visit(
                child,
                Some(child_key),
                fallback_name,
                source,
                episode,
                output,
            );
        }
    }

    let mut output = Vec::new();
    visit(value, None, fallback_name, source, episode, &mut output);
    output
}

fn string_field(object: &serde_json::Map<String, Value>, names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| {
        object
            .get(*name)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
    })
}

fn integer_field(value: Option<&Value>) -> Option<i64> {
    value
        .and_then(Value::as_i64)
        .or_else(|| value?.as_str()?.trim().parse().ok())
}

fn signed_integer_field(object: &serde_json::Map<String, Value>, names: &[&str]) -> Option<i32> {
    names
        .iter()
        .find_map(|name| integer_field(object.get(*name)))
        .and_then(|value| i32::try_from(value.max(0)).ok())
}

fn rating_field(value: Option<&Value>) -> Option<f64> {
    let rating = value
        .and_then(Value::as_f64)
        .or_else(|| value?.as_str()?.trim().parse().ok())
        .or_else(|| value?.get("rating")?.as_f64())
        .or_else(|| value?.get("percentage")?.as_f64().map(|value| value / 10.0))?;
    rating.is_finite().then(|| rating.clamp(0.0, 10.0))
}

fn string_array(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn size_field(object: &serde_json::Map<String, Value>) -> u64 {
    ["size", "bytes"]
        .into_iter()
        .find_map(|name| {
            object
                .get(name)
                .and_then(Value::as_u64)
                .or_else(|| object.get(name)?.as_str()?.trim().parse().ok())
        })
        .unwrap_or_default()
}

fn stable_item_id(title: &str, year: Option<i64>) -> String {
    let digest = Sha256::digest(format!(
        "{}\0{}",
        title.to_lowercase(),
        year.unwrap_or_default()
    ));
    data_encoding::HEXLOWER.encode(&digest[..16])
}

fn title_from_slug(value: &str) -> String {
    value
        .split('-')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut characters = part.chars();
            characters.next().map_or_else(String::new, |first| {
                first.to_uppercase().chain(characters).collect()
            })
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::{
        Kind, endpoint_checkpoint_id, least_advanced_partition, parse_page, parse_show_torrents,
    };
    use crate::store::ButterBackfillState;
    use url::Url;

    #[test]
    fn gives_each_catalog_endpoint_an_independent_checkpoint() {
        let first = Url::parse("https://one.example/api/").expect("valid URL");
        let second = Url::parse("https://two.example/api/").expect("valid URL");
        let first_id = endpoint_checkpoint_id("catalog", &first);

        assert_eq!(first_id, endpoint_checkpoint_id("catalog", &first));
        assert_ne!(first_id, endpoint_checkpoint_id("catalog", &second));
        assert!(first_id.starts_with("catalog:"));
        assert!(!first_id.contains("one.example"));
    }

    #[test]
    fn schedules_the_catalog_partition_that_is_furthest_behind() {
        let movie = ButterBackfillState {
            next_page: 34,
            completed: false,
        };
        let show = ButterBackfillState {
            next_page: 1,
            completed: false,
        };

        let (kind, state) = least_advanced_partition(movie, show).expect("active partition");

        assert_eq!(kind, Kind::Show);
        assert_eq!(state.next_page, 1);
    }

    #[test]
    fn parses_nested_movie_torrents_and_metadata() {
        let payload = serde_json::json!([{
            "imdb_id": "tt1234567",
            "tmdb_id": 987_654,
            "title": "Example Film",
            "year": "2026",
            "rating": { "percentage": 74.0 },
            "genres": ["Drama"],
            "images": { "poster": "https://images.example/poster.jpg" },
            "torrents": { "en": { "1080p": {
                "url": "magnet:?xt=urn:btih:0123456789ABCDEF0123456789ABCDEF01234567",
                "size": 42,
                "seed": 12,
                "name": "Example.Film.2026.1080p"
            } } }
        }]);
        let items = parse_page(payload, Kind::Movie, "butter-test").expect("valid page");
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].id, "tt1234567");
        assert_eq!(items[0].tmdb_id.as_deref(), Some("987654"));
        assert_eq!(items[0].rating, Some(7.4));
        assert_eq!(items[0].torrents.len(), 1);
        assert_eq!(items[0].torrents[0].quality.as_deref(), Some("1080p"));
    }

    #[test]
    fn associates_show_torrents_with_exact_episodes() {
        let item = super::CatalogItem {
            id: "tt7654321".to_owned(),
            tmdb_id: Some("12345".to_owned()),
            kind: Kind::Show,
            title: "Example Show".to_owned(),
            year: Some(2025),
            synopsis: None,
            rating: None,
            poster: None,
            fanart: None,
            genres: Vec::new(),
            torrents: Vec::new(),
        };
        let detail = serde_json::json!({ "episodes": [{
            "season": 2,
            "episode": 3,
            "torrents": { "720p": {
                "url": "magnet:?xt=urn:btih:89ABCDEF0123456789ABCDEF0123456789ABCDEF",
                "size": 100
            } }
        }] });
        let torrents = parse_show_torrents(&detail, &item, "butter-test").expect("detail");
        assert_eq!(torrents.len(), 1);
        assert_eq!(torrents[0].episode, Some((2, 3)));
    }

    #[test]
    fn preserves_episode_file_separately_from_release_name() {
        let item = super::CatalogItem {
            id: "tt7654321".to_owned(),
            tmdb_id: Some("12345".to_owned()),
            kind: Kind::Show,
            title: "Example Show".to_owned(),
            year: Some(2025),
            synopsis: None,
            rating: None,
            poster: None,
            fanart: None,
            genres: Vec::new(),
            torrents: Vec::new(),
        };
        let detail = serde_json::json!({ "episodes": [{
            "season": 1,
            "episode": 4,
            "torrents": { "1080p": {
                "url": "magnet:?xt=urn:btih:89ABCDEF0123456789ABCDEF0123456789ABCDEF",
                "title": "Example Show Seasons 1 and 2 Complete",
                "file": "Season 1/Example Show - S01E04.mkv"
            } }
        }] });

        let torrents = parse_show_torrents(&detail, &item, "butter-test").expect("detail");

        assert_eq!(
            torrents[0].record.name,
            "Example Show Seasons 1 and 2 Complete"
        );
        assert_eq!(
            torrents[0].file.as_deref(),
            Some("Season 1/Example Show - S01E04.mkv")
        );
    }
}
