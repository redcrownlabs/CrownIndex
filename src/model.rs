//! Defines the Butter-compatible response model.

use std::collections::BTreeMap;

use serde::Serialize;

#[derive(Debug, Serialize)]
#[allow(
    clippy::struct_field_names,
    reason = "status is the required Butter API wire field"
)]
pub(crate) struct Status {
    pub(crate) status: &'static str,
    pub(crate) version: &'static str,
    #[serde(rename = "totalMovies")]
    pub(crate) total_movies: i64,
    #[serde(rename = "totalShows")]
    pub(crate) total_shows: i64,
    #[serde(rename = "totalTorrents")]
    pub(crate) total_torrents: i64,
}

#[derive(Debug, Serialize)]
pub(crate) struct Images {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) poster: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) fanart: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct Torrent {
    pub(crate) provider: String,
    pub(crate) quality: String,
    pub(crate) seeds: u32,
    pub(crate) peers: u32,
    pub(crate) size: u64,
    pub(crate) url: String,
    pub(crate) name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) file: Option<String>,
}

pub(crate) type LanguageTorrents = BTreeMap<String, BTreeMap<String, Torrent>>;

#[derive(Debug, Serialize)]
pub(crate) struct CatalogItem {
    #[serde(rename = "_id")]
    pub(crate) id: String,
    pub(crate) title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) year: Option<i32>,
    pub(crate) synopsis: String,
    pub(crate) rating: f64,
    pub(crate) images: Images,
    pub(crate) genres: Vec<String>,
    #[serde(rename = "type")]
    pub(crate) media_type: &'static str,
    pub(crate) torrents: LanguageTorrents,
}

#[derive(Debug, Serialize)]
pub(crate) struct Show {
    #[serde(rename = "_id")]
    pub(crate) id: String,
    pub(crate) title: String,
    pub(crate) episodes: Vec<Episode>,
}

#[derive(Debug, Serialize)]
#[allow(
    clippy::struct_field_names,
    reason = "episode is the required Butter API wire field"
)]
pub(crate) struct Episode {
    pub(crate) season: u16,
    pub(crate) episode: u16,
    pub(crate) title: String,
    pub(crate) overview: String,
    pub(crate) torrents: BTreeMap<String, Torrent>,
}
