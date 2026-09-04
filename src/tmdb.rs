//! Resolves conservative torrent-title matches through TMDB.

use anyhow::{Context, Result};
use reqwest::Client;
use serde::Deserialize;
use url::Url;

use crate::config::TmdbConfig;
use crate::media_match::{ParsedKind, ParsedMedia, normalized_title};

const TMDB_API_BASE: &str = "https://api.themoviedb.org/3/";

#[derive(Debug, Clone)]
pub(crate) struct Tmdb {
    client: Client,
    base_url: Url,
    config: TmdbConfig,
    auth: Auth,
}

#[derive(Debug, Clone)]
enum Auth {
    ApiKey(String),
    Bearer(String),
}

#[derive(Debug, Clone)]
pub(crate) struct TmdbContent {
    pub(crate) kind: ParsedKind,
    pub(crate) id: i32,
    pub(crate) title: String,
    pub(crate) original_title: Option<String>,
    pub(crate) release_date: Option<String>,
    pub(crate) release_year: Option<i32>,
    pub(crate) overview: Option<String>,
    pub(crate) runtime: Option<i32>,
    pub(crate) popularity: Option<f64>,
    pub(crate) vote_average: Option<f64>,
    pub(crate) vote_count: Option<i64>,
    pub(crate) poster_path: Option<String>,
    pub(crate) backdrop_path: Option<String>,
    pub(crate) original_language: Option<String>,
    pub(crate) adult: Option<bool>,
    pub(crate) genres: Vec<TmdbGenre>,
    pub(crate) imdb_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct TmdbGenre {
    pub(crate) id: i32,
    pub(crate) name: String,
}

impl Tmdb {
    pub(crate) fn new(config: TmdbConfig) -> Result<Self> {
        let client = Client::builder()
            .timeout(config.request_timeout)
            .build()
            .context("failed to build TMDB HTTP client")?;
        let base_url = Url::parse(TMDB_API_BASE).context("invalid TMDB API base URL")?;
        let key = config.api_key.expose().to_owned();
        let auth = if key.starts_with("eyJ") {
            Auth::Bearer(key)
        } else {
            Auth::ApiKey(key)
        };
        Ok(Self {
            client,
            base_url,
            config,
            auth,
        })
    }

    pub(crate) async fn match_media(&self, candidate: &ParsedMedia) -> Result<Option<TmdbContent>> {
        let results = self.search(candidate).await?;
        for result in results.results.into_iter().take(5) {
            if candidate_matches(candidate, &result) {
                return self.details(candidate.kind, result.id).await.map(Some);
            }
        }
        Ok(None)
    }

    /// Verifies that the configured TMDB endpoint and credentials are usable.
    ///
    /// Batch enrichment calls this once before selecting database work. A
    /// network-wide TLS, DNS, authentication, or service failure therefore
    /// cannot turn every item in the batch into a persisted item-level error.
    pub(crate) async fn check_availability(&self) -> Result<()> {
        let _: serde_json::Value = self
            .get("configuration", &[])
            .await
            .context("TMDB availability check failed")?;
        Ok(())
    }

    pub(crate) async fn match_imdb_id(
        &self,
        imdb_id: &str,
        hint: Option<&str>,
    ) -> Result<Option<TmdbContent>> {
        if !is_imdb_id(imdb_id) {
            return Ok(None);
        }
        let path = format!("find/{imdb_id}");
        let query = [
            ("external_source", "imdb_id".to_owned()),
            ("language", self.config.language.clone()),
        ];
        let result: FindResponse = self
            .get(&path, &query)
            .await
            .with_context(|| format!("TMDB external ID lookup failed for {imdb_id}"))?;
        let match_id = match hint {
            Some("movie") => result
                .movie_results
                .first()
                .map(|item| (ParsedKind::Movie, item.id)),
            Some("tv_show") => result
                .tv_results
                .first()
                .map(|item| (ParsedKind::Series, item.id)),
            _ => match (result.movie_results.first(), result.tv_results.first()) {
                (Some(movie), None) => Some((ParsedKind::Movie, movie.id)),
                (None, Some(series)) => Some((ParsedKind::Series, series.id)),
                _ => None,
            },
        };
        let Some((kind, id)) = match_id else {
            return Ok(None);
        };
        let mut content = self.details(kind, id).await?;
        content.imdb_id = Some(imdb_id.to_owned());
        Ok(Some(content))
    }

    async fn search(&self, candidate: &ParsedMedia) -> Result<SearchResponse> {
        let path = match candidate.kind {
            ParsedKind::Movie => "search/movie",
            ParsedKind::Series => "search/tv",
        };
        let mut query = vec![
            ("query", candidate.title.clone()),
            ("include_adult", "false".to_owned()),
            ("language", self.config.language.clone()),
            ("page", "1".to_owned()),
        ];
        if let Some(year) = candidate.year {
            let key = match candidate.kind {
                ParsedKind::Movie => "year",
                ParsedKind::Series => "first_air_date_year",
            };
            query.push((key, year.to_string()));
        }
        self.get(path, &query)
            .await
            .with_context(|| format!("TMDB search failed for {}", candidate.title))
    }

    async fn details(&self, kind: ParsedKind, id: i32) -> Result<TmdbContent> {
        let path = match kind {
            ParsedKind::Movie => format!("movie/{id}"),
            ParsedKind::Series => format!("tv/{id}"),
        };
        let query = [("language", self.config.language.clone())];
        let detail: DetailResponse = self
            .get(&path, &query)
            .await
            .with_context(|| format!("TMDB detail lookup failed for id {id}"))?;
        Ok(detail.into_content(kind))
    }

    async fn get<T>(&self, path: &str, query: &[(&str, String)]) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        let mut url = self
            .base_url
            .join(path)
            .with_context(|| format!("invalid TMDB path: {path}"))?;
        {
            let mut pairs = url.query_pairs_mut();
            for (key, value) in query {
                pairs.append_pair(key, value);
            }
            if let Auth::ApiKey(key) = &self.auth {
                pairs.append_pair("api_key", key);
            }
        }
        let mut request = self.client.get(url);
        match &self.auth {
            Auth::ApiKey(_) => {}
            Auth::Bearer(token) => {
                request = request.bearer_auth(token);
            }
        }
        let response = request.send().await.context("failed to call TMDB")?;
        let status = response.status();
        if !status.is_success() {
            anyhow::bail!("TMDB returned HTTP {status}");
        }
        response.json().await.context("failed to decode TMDB JSON")
    }
}

#[derive(Debug, Deserialize)]
struct SearchResponse {
    results: Vec<SearchResult>,
}

#[derive(Debug, Deserialize)]
struct FindResponse {
    #[serde(default)]
    movie_results: Vec<FindResult>,
    #[serde(default)]
    tv_results: Vec<FindResult>,
}

#[derive(Debug, Deserialize)]
struct FindResult {
    id: i32,
}

#[derive(Debug, Deserialize)]
struct SearchResult {
    id: i32,
    title: Option<String>,
    name: Option<String>,
    original_title: Option<String>,
    original_name: Option<String>,
    release_date: Option<String>,
    first_air_date: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DetailResponse {
    id: i32,
    title: Option<String>,
    name: Option<String>,
    original_title: Option<String>,
    original_name: Option<String>,
    release_date: Option<String>,
    first_air_date: Option<String>,
    overview: Option<String>,
    runtime: Option<i32>,
    episode_run_time: Option<Vec<i32>>,
    popularity: Option<f64>,
    vote_average: Option<f64>,
    vote_count: Option<i64>,
    poster_path: Option<String>,
    backdrop_path: Option<String>,
    original_language: Option<String>,
    adult: Option<bool>,
    genres: Option<Vec<TmdbGenre>>,
}

impl DetailResponse {
    fn into_content(self, kind: ParsedKind) -> TmdbContent {
        let title = self
            .title
            .or(self.name)
            .unwrap_or_else(|| self.id.to_string());
        let original_title = self.original_title.or(self.original_name);
        let release_date = self.release_date.or(self.first_air_date);
        let release_year = release_date.as_deref().and_then(year_from_date);
        let runtime = self.runtime.or_else(|| {
            self.episode_run_time
                .and_then(|values| values.into_iter().next())
        });
        TmdbContent {
            kind,
            id: self.id,
            title,
            original_title,
            release_date,
            release_year,
            overview: empty_to_none(self.overview),
            runtime,
            popularity: self.popularity,
            vote_average: self.vote_average,
            vote_count: self.vote_count,
            poster_path: self.poster_path,
            backdrop_path: self.backdrop_path,
            original_language: self.original_language,
            adult: self.adult,
            genres: self.genres.unwrap_or_default(),
            imdb_id: None,
        }
    }
}

fn is_imdb_id(value: &str) -> bool {
    value
        .strip_prefix("tt")
        .is_some_and(|digits| digits.len() >= 5 && digits.bytes().all(|byte| byte.is_ascii_digit()))
}

fn candidate_matches(candidate: &ParsedMedia, result: &SearchResult) -> bool {
    let candidate_title = normalized_title(&candidate.title);
    let result_titles = [
        result.title.as_deref(),
        result.name.as_deref(),
        result.original_title.as_deref(),
        result.original_name.as_deref(),
    ];
    let title_matches = result_titles.into_iter().flatten().any(|title| {
        let normalized = normalized_title(title);
        normalized == candidate_title
            || (candidate_title.chars().count() >= 8 && normalized.contains(&candidate_title))
            || (normalized.chars().count() >= 8 && candidate_title.contains(&normalized))
    });
    if !title_matches {
        return false;
    }
    match candidate.year {
        Some(expected) => result
            .release_date
            .as_deref()
            .or(result.first_air_date.as_deref())
            .and_then(year_from_date)
            .is_some_and(|actual| (actual - expected).abs() <= 1),
        None => true,
    }
}

fn year_from_date(value: &str) -> Option<i32> {
    value.get(..4)?.parse().ok()
}

fn empty_to_none(value: Option<String>) -> Option<String> {
    value.and_then(|text| {
        let trimmed = text.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_owned())
    })
}

#[cfg(test)]
mod tests {
    use super::{SearchResult, candidate_matches, is_imdb_id};
    use crate::media_match::{ParsedKind, ParsedMedia};

    #[test]
    fn requires_matching_year_when_present() {
        let candidate = ParsedMedia {
            kind: ParsedKind::Movie,
            title: "Tuner".to_owned(),
            year: Some(2025),
        };
        let result = SearchResult {
            id: 1,
            title: Some("Tuner".to_owned()),
            name: None,
            original_title: None,
            original_name: None,
            release_date: Some("2025-01-01".to_owned()),
            first_air_date: None,
        };
        assert!(candidate_matches(&candidate, &result));
    }

    #[test]
    fn rejects_different_titles() {
        let candidate = ParsedMedia {
            kind: ParsedKind::Movie,
            title: "Tuner".to_owned(),
            year: Some(2025),
        };
        let result = SearchResult {
            id: 1,
            title: Some("Something Else".to_owned()),
            name: None,
            original_title: None,
            original_name: None,
            release_date: Some("2025-01-01".to_owned()),
            first_air_date: None,
        };
        assert!(!candidate_matches(&candidate, &result));
    }

    #[test]
    fn validates_imdb_title_identifiers() {
        assert!(is_imdb_id("tt14688458"));
        assert!(!is_imdb_id("14688458"));
        assert!(!is_imdb_id("tt12x45"));
    }
}
