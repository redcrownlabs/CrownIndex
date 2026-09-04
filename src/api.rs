//! Serves grouped media through the Butter-compatible route contract.

use std::collections::{BTreeMap, HashMap};
use std::sync::OnceLock;

use anyhow::Result;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use regex::Regex;
use serde::Deserialize;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::trace::TraceLayer;
use tracing::{Level, event};

use crate::model::{CatalogItem, Episode, Images, LanguageTorrents, Show, Status, Torrent};
use crate::store::{Browse, CatalogRow, CatalogStore, EpisodeTorrentRow, MediaKind, Sort};

const TMDB_POSTER_BASE: &str = "https://image.tmdb.org/t/p/w500";
const TMDB_BACKDROP_BASE: &str = "https://image.tmdb.org/t/p/w1280";

#[derive(Debug, Clone)]
struct AppState {
    store: CatalogStore,
}

#[derive(Debug, Default, Deserialize)]
struct BrowseParams {
    sort: Option<String>,
    genre: Option<String>,
    keywords: Option<String>,
    anime: Option<String>,
}

#[derive(Debug)]
struct ApiError(anyhow::Error);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        event!(
            name: "api.request.failed",
            Level::ERROR,
            error.message = %self.0,
            "API request failed: {{error.message}}",
        );
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": "catalog request failed" })),
        )
            .into_response()
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(error: anyhow::Error) -> Self {
        Self(error)
    }
}

pub(crate) fn router(store: CatalogStore) -> Router {
    Router::new()
        .route("/", get(status))
        .route("/status", get(status))
        .route("/movies/{page}", get(movies))
        .route("/shows/{page}", get(shows))
        .route("/show/{id}", get(show))
        .layer(CatchPanicLayer::new())
        .layer(TraceLayer::new_for_http())
        .with_state(AppState { store })
}

async fn status(State(state): State<AppState>) -> Result<Json<Status>, ApiError> {
    let counts = state.store.stats().await?;
    Ok(Json(Status {
        status: "ok",
        version: env!("CARGO_PKG_VERSION"),
        total_movies: counts.movies,
        total_shows: counts.shows,
        total_torrents: counts.torrents,
    }))
}

async fn movies(
    State(state): State<AppState>,
    Path(page): Path<u32>,
    Query(params): Query<BrowseParams>,
) -> Result<Json<Vec<CatalogItem>>, ApiError> {
    browse(&state.store, page, MediaKind::Movie, params).await
}

async fn shows(
    State(state): State<AppState>,
    Path(page): Path<u32>,
    Query(params): Query<BrowseParams>,
) -> Result<Json<Vec<CatalogItem>>, ApiError> {
    let kind = if params
        .anime
        .as_deref()
        .is_some_and(|value| matches!(value, "1" | "true" | "yes"))
    {
        MediaKind::Anime
    } else {
        MediaKind::Series
    };
    browse(&state.store, page, kind, params).await
}

async fn browse(
    store: &CatalogStore,
    page: u32,
    kind: MediaKind,
    params: BrowseParams,
) -> Result<Json<Vec<CatalogItem>>, ApiError> {
    if page == 0 {
        return Ok(Json(Vec::new()));
    }
    let query = Browse {
        kind,
        page,
        sort: parse_sort(params.sort.as_deref()),
        keywords: cleaned(params.keywords),
        genre: cleaned(params.genre),
    };
    let rows = store.browse(&query).await?;
    Ok(Json(group_catalog(rows, kind)))
}

async fn show(State(state): State<AppState>, Path(id): Path<String>) -> Result<Response, ApiError> {
    let (source, content_id) = id
        .split_once('.')
        .map_or((None, id.as_str()), |(source, content_id)| {
            (Some(source), content_id)
        });
    let Some(resolved) = state.store.resolve_show(source, content_id).await? else {
        return Ok((
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "show not found" })),
        )
            .into_response());
    };
    let rows = state
        .store
        .episode_torrents(&resolved.source, &resolved.id)
        .await?;
    Ok(Json(Show {
        id: format!("{}.{}", resolved.source, resolved.id),
        title: resolved.title,
        episodes: group_episodes(rows),
    })
    .into_response())
}

fn group_catalog(rows: Vec<CatalogRow>, kind: MediaKind) -> Vec<CatalogItem> {
    let mut items = Vec::<CatalogItem>::new();
    let mut positions = HashMap::<(String, String), usize>::new();
    for row in rows {
        let key = (row.content_source.clone(), row.content_id.clone());
        let position = *positions.entry(key).or_insert_with(|| {
            let position = items.len();
            items.push(CatalogItem {
                id: format!("{}.{}", row.content_source, row.content_id),
                title: row.title.clone(),
                year: row.release_year,
                synopsis: row.overview.clone().unwrap_or_default(),
                rating: row.vote_average.unwrap_or_default().clamp(0.0, 10.0),
                images: Images {
                    poster: image_url(TMDB_POSTER_BASE, row.poster_path.as_deref()),
                    fanart: image_url(TMDB_BACKDROP_BASE, row.backdrop_path.as_deref()),
                },
                genres: row.genres.clone(),
                media_type: match kind {
                    MediaKind::Movie => "movie",
                    MediaKind::Series | MediaKind::Anime => "show",
                },
                torrents: LanguageTorrents::from([("en".to_owned(), BTreeMap::new())]),
            });
            position
        });
        let torrent = torrent_from_catalog(&row);
        if let Some(qualities) = items[position].torrents.get_mut("en") {
            insert_torrent(qualities, torrent);
        }
    }
    items
}

fn group_episodes(rows: Vec<EpisodeTorrentRow>) -> Vec<Episode> {
    let mut grouped = BTreeMap::<(u16, u16), BTreeMap<String, Torrent>>::new();
    for row in rows {
        let base = torrent_from_episode(&row);
        let mut exact_files = false;
        for file in &row.files {
            if let Some(key) = episode_from_path(file) {
                exact_files = true;
                let mut torrent = base.clone();
                torrent.file = Some(file.clone());
                insert_torrent(grouped.entry(key).or_default(), torrent);
            }
        }
        if !exact_files {
            for (season, episode, file) in
                merged_explicit_episodes(row.episodes.as_ref(), row.episode_files.as_ref())
            {
                let mut torrent = base.clone();
                torrent.file = file;
                insert_torrent(grouped.entry((season, episode)).or_default(), torrent);
            }
        }
    }
    grouped
        .into_iter()
        .map(|((season, episode), torrents)| Episode {
            season,
            episode,
            title: format!("Episode {episode}"),
            overview: String::new(),
            torrents,
        })
        .collect()
}

fn torrent_from_catalog(row: &CatalogRow) -> Torrent {
    torrent(
        &row.info_hash,
        &row.torrent_name,
        row.size,
        row.video_resolution.as_deref(),
        row.seeders,
        row.leechers,
        &row.provider,
    )
}

fn torrent_from_episode(row: &EpisodeTorrentRow) -> Torrent {
    torrent(
        &row.info_hash,
        &row.torrent_name,
        row.size,
        row.video_resolution.as_deref(),
        row.seeders,
        row.leechers,
        &row.provider,
    )
}

fn torrent(
    info_hash: &str,
    name: &str,
    size: i64,
    resolution: Option<&str>,
    seeders: Option<i32>,
    leechers: Option<i32>,
    provider: &str,
) -> Torrent {
    Torrent {
        provider: provider.to_owned(),
        quality: normalize_quality(resolution),
        seeds: non_negative_u32(seeders),
        peers: non_negative_u32(leechers),
        size: u64::try_from(size).unwrap_or_default(),
        url: format!("magnet:?xt=urn:btih:{}", info_hash.to_ascii_uppercase()),
        name: name.to_owned(),
        file: None,
    }
}

fn insert_torrent(torrents: &mut BTreeMap<String, Torrent>, torrent: Torrent) {
    let base = torrent.quality.clone();
    let mut key = base.clone();
    let mut suffix = 2_u32;
    while torrents.contains_key(&key) {
        key = format!("{base}-{suffix}");
        suffix = suffix.saturating_add(1);
    }
    torrents.insert(key, torrent);
}

fn explicit_episodes(value: Option<&serde_json::Value>) -> Vec<(u16, u16, Option<String>)> {
    let Some(seasons) = value.and_then(serde_json::Value::as_object) else {
        return Vec::new();
    };
    let mut result = Vec::new();
    for (season, episodes) in seasons {
        let Ok(season) = season.parse::<u16>() else {
            continue;
        };
        let Some(episodes) = episodes.as_object() else {
            continue;
        };
        result.extend(episodes.iter().filter_map(|(episode, metadata)| {
            let episode = episode.parse::<u16>().ok()?;
            let file = metadata.as_str().map(str::to_owned);
            Some((season, episode, file))
        }));
    }
    result
}

fn merged_explicit_episodes(
    compatibility: Option<&serde_json::Value>,
    exact_files: Option<&serde_json::Value>,
) -> Vec<(u16, u16, Option<String>)> {
    let mut episodes = BTreeMap::new();
    for (season, episode, file) in explicit_episodes(compatibility) {
        episodes.insert((season, episode), file);
    }
    for (season, episode, file) in explicit_episodes(exact_files) {
        episodes.insert((season, episode), file);
    }
    episodes
        .into_iter()
        .map(|((season, episode), file)| (season, episode, file))
        .collect()
}

fn episode_from_path(path: &str) -> Option<(u16, u16)> {
    static EPISODE_PATTERN: OnceLock<Regex> = OnceLock::new();
    let pattern = EPISODE_PATTERN.get_or_init(|| {
        Regex::new(r"(?i)(?:s(?<season>\d{1,3})[ ._-]*e(?<episode>\d{1,3})|(?<season_x>\d{1,3})x(?<episode_x>\d{1,3}))")
            .expect("episode regex must compile")
    });
    let captures = pattern.captures(path)?;
    let season = captures
        .name("season")
        .or_else(|| captures.name("season_x"))?
        .as_str()
        .parse()
        .ok()?;
    let episode = captures
        .name("episode")
        .or_else(|| captures.name("episode_x"))?
        .as_str()
        .parse()
        .ok()?;
    Some((season, episode))
}

fn parse_sort(value: Option<&str>) -> Sort {
    match value.map(str::to_ascii_lowercase).as_deref() {
        Some("popularity") => Sort::Popularity,
        Some("updated") => Sort::Updated,
        Some("last added") => Sort::LastAdded,
        Some("year") => Sort::Year,
        Some("title" | "name") => Sort::Title,
        Some("rating") => Sort::Rating,
        _ => Sort::Trending,
    }
}

fn cleaned(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn normalize_quality(resolution: Option<&str>) -> String {
    resolution
        .and_then(|value| value.strip_prefix('V'))
        .filter(|value| !value.is_empty())
        .unwrap_or("unknown")
        .to_ascii_lowercase()
}

fn image_url(base: &str, path: Option<&str>) -> Option<String> {
    let path = path?.trim();
    if path.is_empty() {
        None
    } else if path.starts_with("https://") || path.starts_with("http://") {
        Some(path.to_owned())
    } else {
        Some(format!("{base}/{}", path.trim_start_matches('/')))
    }
}

fn non_negative_u32(value: Option<i32>) -> u32 {
    value
        .and_then(|value| u32::try_from(value).ok())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::{episode_from_path, explicit_episodes, insert_torrent, merged_explicit_episodes};
    use crate::model::Torrent;
    use std::collections::BTreeMap;

    #[test]
    fn extracts_exact_episode_paths() {
        assert_eq!(episode_from_path("Show.S02E07.1080p.mkv"), Some((2, 7)));
        assert_eq!(episode_from_path("Show 3x11.mp4"), Some((3, 11)));
        assert_eq!(episode_from_path("Show Season 2.mkv"), None);
    }

    #[test]
    fn ignores_whole_seasons_without_exact_episode_metadata() {
        let value = serde_json::json!({"1": {}, "2": {"3": {}}});
        assert_eq!(explicit_episodes(Some(&value)), vec![(2, 3, None)]);
    }

    #[test]
    fn preserves_explicit_source_episode_file() {
        let value = serde_json::json!({
            "1": {"4": "Season 1/Example Show - S01E04.mkv"}
        });
        assert_eq!(
            explicit_episodes(Some(&value)),
            vec![(1, 4, Some("Season 1/Example Show - S01E04.mkv".to_owned()))]
        );
    }

    #[test]
    fn exact_episode_files_override_compact_compatibility_metadata() {
        let compatibility = serde_json::json!({"1": {"4": true, "5": true}});
        let exact = serde_json::json!({"1": {"4": "Season 1/Show.S01E04.mkv"}});

        assert_eq!(
            merged_explicit_episodes(Some(&compatibility), Some(&exact)),
            vec![
                (1, 4, Some("Season 1/Show.S01E04.mkv".to_owned())),
                (1, 5, None),
            ]
        );
    }

    #[test]
    fn retains_multiple_torrents_at_one_quality() {
        let mut torrents = BTreeMap::new();
        for name in ["first", "second"] {
            insert_torrent(
                &mut torrents,
                Torrent {
                    provider: "test".to_owned(),
                    quality: "1080p".to_owned(),
                    seeds: 1,
                    peers: 0,
                    size: 1,
                    url: format!("magnet:{name}"),
                    name: name.to_owned(),
                    file: None,
                },
            );
        }
        assert_eq!(torrents.len(), 2);
        assert!(torrents.contains_key("1080p"));
        assert!(torrents.contains_key("1080p-2"));
    }
}
