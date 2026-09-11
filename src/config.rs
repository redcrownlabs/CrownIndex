//! Parses process configuration and command-line intent.

use std::env;
use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use url::Url;

#[derive(Debug, Parser)]
#[command(version, about)]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub(crate) command: Option<Command>,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    /// Serve the Butter-compatible HTTP API.
    Serve,
    /// Import authorized EXT detail-page snapshots from a directory.
    ImportExtSnapshots {
        /// Directory containing saved HTML detail pages.
        directory: PathBuf,
    },
    /// Import a bounded number of live EXT browse pages.
    ImportExtLive {
        /// Maximum browse pages to request sequentially.
        #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u16).range(1..=100))]
        pages: u16,
    },
    /// Poll every configured Jackett indexer once and import new observations.
    ImportJackett,
    /// Import and merge every configured Butter-compatible catalog.
    BackfillButter {
        /// Stop after this many committed pages; primarily for controlled runs.
        #[arg(long, value_parser = clap::value_parser!(u32).range(1..))]
        max_pages: Option<u32>,
    },
    /// Import resumable Jackett history using year-partitioned searches.
    BackfillJackett {
        /// Restrict backfill to these configured indexer IDs.
        #[arg(
            long = "indexer",
            env = "CROWN_INDEX_BACKFILL_INDEXERS",
            value_delimiter = ','
        )]
        indexers: Vec<String>,
        /// Oldest release year to query, inclusive.
        #[arg(long, env = "CROWN_INDEX_BACKFILL_MIN_YEAR", default_value_t = 1900)]
        min_year: i32,
        /// Newest release year to query; defaults to the database's current year.
        #[arg(long, env = "CROWN_INDEX_BACKFILL_START_YEAR")]
        start_year: Option<i32>,
        /// Maximum results requested for one indexer/year partition.
        #[arg(long, env = "CROWN_INDEX_BACKFILL_RESULT_LIMIT", default_value_t = 1000, value_parser = clap::value_parser!(u16).range(1..=1000))]
        result_limit: u16,
        /// Delay between source queries.
        #[arg(long, env = "CROWN_INDEX_BACKFILL_DELAY_SECONDS", default_value_t = 60, value_parser = clap::value_parser!(u64).range(1..=86400))]
        delay_seconds: u64,
        /// Stop after this many attempted partitions; primarily for controlled runs.
        #[arg(long, value_parser = clap::value_parser!(u32).range(1..))]
        max_partitions: Option<u32>,
    },
    /// Enrich imported torrents with conservative TMDB movie/show matches.
    EnrichTmdb {
        /// Maximum torrents to inspect in this run.
        #[arg(long, env = "CROWN_INDEX_TMDB_BATCH_SIZE", default_value_t = 100, value_parser = clap::value_parser!(i64).range(1..=1000))]
        limit: i64,
        /// Enrich one exact hexadecimal info hash, including a prior attempt.
        #[arg(long)]
        info_hash: Option<String>,
    },
}

#[derive(Clone)]
pub(crate) struct Secret(String);

impl Secret {
    pub(crate) fn expose(&self) -> &str {
        &self.0
    }
}

impl Debug for Secret {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Secret([redacted])")
    }
}

#[derive(Debug, Clone)]
pub(crate) struct JackettConfig {
    pub(crate) base_url: Url,
    pub(crate) api_key: Secret,
    pub(crate) indexers: Vec<String>,
    pub(crate) poll_interval: Duration,
    pub(crate) request_timeout: Duration,
}

#[derive(Debug, Clone)]
pub(crate) struct TmdbConfig {
    pub(crate) api_key: Secret,
    pub(crate) enabled: bool,
    pub(crate) language: String,
    pub(crate) batch_size: i64,
    pub(crate) poll_interval: Duration,
    pub(crate) request_timeout: Duration,
}

#[derive(Debug, Clone)]
pub(crate) struct ButterConfig {
    pub(crate) source_id: String,
    pub(crate) endpoints: Vec<Url>,
    pub(crate) request_timeout: Duration,
    pub(crate) request_delay: Duration,
}

#[derive(Debug, Clone)]
pub(crate) struct Config {
    pub(crate) listen: SocketAddr,
    pub(crate) database_url: String,
    pub(crate) bitmagnet_url: Url,
    pub(crate) ext_url: Url,
    pub(crate) ext_user_agent: String,
    pub(crate) jackett: Option<JackettConfig>,
    pub(crate) butter: Option<ButterConfig>,
    pub(crate) tmdb: Option<TmdbConfig>,
    pub(crate) operator_token: Option<Secret>,
}

impl Config {
    pub(crate) fn from_env() -> Result<Self> {
        let listen = env_value("CROWN_INDEX_LISTEN", "127.0.0.1:8080")
            .parse()
            .context("CROWN_INDEX_LISTEN must be a socket address")?;
        let database_url = env_value(
            "CROWN_INDEX_DATABASE_URL",
            "postgres://postgres:crownindex-local@127.0.0.1:5432/bitmagnet",
        );
        let bitmagnet_url = parse_base_url(
            "CROWN_INDEX_BITMAGNET_URL",
            &env_value("CROWN_INDEX_BITMAGNET_URL", "http://127.0.0.1:3333/"),
        )?;
        let ext_url = parse_base_url(
            "CROWN_INDEX_EXT_URL",
            &env_value("CROWN_INDEX_EXT_URL", "https://ext.to/"),
        )?;
        let ext_user_agent = env_value(
            "CROWN_INDEX_EXT_USER_AGENT",
            "CrownIndex/0.1 (+local metadata index; operator must configure contact)",
        );
        let jackett = jackett_config()?;
        let butter = butter_config()?;
        let tmdb = tmdb_config()?;
        let operator_token = env::var("CROWN_INDEX_OPERATOR_TOKEN")
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .map(Secret);
        Ok(Self {
            listen,
            database_url,
            bitmagnet_url,
            ext_url,
            ext_user_agent,
            jackett,
            butter,
            tmdb,
            operator_token,
        })
    }
}

fn butter_config() -> Result<Option<ButterConfig>> {
    let Some(raw_urls) = env::var("CROWN_INDEX_BUTTER_URLS")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    let source_id = env_value("CROWN_INDEX_BUTTER_SOURCE_ID", "redcrown-fallbacks");
    validate_source_id(&source_id)?;
    let mut endpoints = Vec::new();
    for value in raw_urls
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let endpoint = parse_base_url("CROWN_INDEX_BUTTER_URLS", value)?;
        if endpoint.query().is_some() || endpoint.fragment().is_some() {
            anyhow::bail!("CROWN_INDEX_BUTTER_URLS entries cannot contain queries or fragments");
        }
        if !endpoints.contains(&endpoint) {
            endpoints.push(endpoint);
        }
    }
    if endpoints.is_empty() {
        anyhow::bail!("CROWN_INDEX_BUTTER_URLS must contain at least one URL");
    }
    let timeout_seconds = env_value("CROWN_INDEX_BUTTER_TIMEOUT_SECONDS", "60")
        .parse::<u64>()
        .context("CROWN_INDEX_BUTTER_TIMEOUT_SECONDS must be an integer")?;
    if !(10..=300).contains(&timeout_seconds) {
        anyhow::bail!("CROWN_INDEX_BUTTER_TIMEOUT_SECONDS must be between 10 and 300");
    }
    let delay_millis = env_value("CROWN_INDEX_BUTTER_DELAY_MILLIS", "500")
        .parse::<u64>()
        .context("CROWN_INDEX_BUTTER_DELAY_MILLIS must be an integer")?;
    if delay_millis > 60_000 {
        anyhow::bail!("CROWN_INDEX_BUTTER_DELAY_MILLIS must be at most 60000");
    }
    Ok(Some(ButterConfig {
        source_id,
        endpoints,
        request_timeout: Duration::from_secs(timeout_seconds),
        request_delay: Duration::from_millis(delay_millis),
    }))
}

fn validate_source_id(value: &str) -> Result<()> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        anyhow::bail!(
            "CROWN_INDEX_BUTTER_SOURCE_ID may contain only ASCII letters, numbers, and hyphens"
        );
    }
    Ok(())
}

fn tmdb_config() -> Result<Option<TmdbConfig>> {
    let enabled = parse_bool(&env_value("TMDB_ENABLED", "false"))
        .context("TMDB_ENABLED must be true or false")?;
    let Some(api_key) = env::var("TMDB_API_KEY")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    let language = env_value("CROWN_INDEX_TMDB_LANGUAGE", "en-US");
    if language.trim().is_empty() {
        anyhow::bail!("CROWN_INDEX_TMDB_LANGUAGE cannot be empty");
    }
    let batch_size = env_value("CROWN_INDEX_TMDB_BATCH_SIZE", "100")
        .parse::<i64>()
        .context("CROWN_INDEX_TMDB_BATCH_SIZE must be an integer")?;
    if !(1..=1_000).contains(&batch_size) {
        anyhow::bail!("CROWN_INDEX_TMDB_BATCH_SIZE must be between 1 and 1000");
    }
    let poll_seconds = env_value("CROWN_INDEX_TMDB_POLL_SECONDS", "900")
        .parse::<u64>()
        .context("CROWN_INDEX_TMDB_POLL_SECONDS must be an integer")?;
    if !(60..=86_400).contains(&poll_seconds) {
        anyhow::bail!("CROWN_INDEX_TMDB_POLL_SECONDS must be between 60 and 86400");
    }
    let timeout_seconds = env_value("CROWN_INDEX_TMDB_TIMEOUT_SECONDS", "30")
        .parse::<u64>()
        .context("CROWN_INDEX_TMDB_TIMEOUT_SECONDS must be an integer")?;
    if !(5..=120).contains(&timeout_seconds) {
        anyhow::bail!("CROWN_INDEX_TMDB_TIMEOUT_SECONDS must be between 5 and 120");
    }
    Ok(Some(TmdbConfig {
        api_key: Secret(api_key),
        enabled,
        language,
        batch_size,
        poll_interval: Duration::from_secs(poll_seconds),
        request_timeout: Duration::from_secs(timeout_seconds),
    }))
}

fn jackett_config() -> Result<Option<JackettConfig>> {
    let Some(api_key) = env::var("JACKETT_API_KEY")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    let base_url = parse_base_url(
        "CROWN_INDEX_JACKETT_URL",
        &env_value("CROWN_INDEX_JACKETT_URL", "http://127.0.0.1:9117/"),
    )?;
    let indexers = parse_indexers(&env_value(
        "CROWN_INDEX_JACKETT_INDEXERS",
        "1337x,audiobookbay,yts,thepiratebay,eztv,dontorrent,nyaasi,sktorrent,polskie-torrenty",
    ))?;
    let poll_seconds = env_value("CROWN_INDEX_JACKETT_POLL_SECONDS", "1800")
        .parse::<u64>()
        .context("CROWN_INDEX_JACKETT_POLL_SECONDS must be an integer")?;
    if !(60..=86_400).contains(&poll_seconds) {
        anyhow::bail!("CROWN_INDEX_JACKETT_POLL_SECONDS must be between 60 and 86400");
    }
    let timeout_seconds = env_value("CROWN_INDEX_JACKETT_TIMEOUT_SECONDS", "300")
        .parse::<u64>()
        .context("CROWN_INDEX_JACKETT_TIMEOUT_SECONDS must be an integer")?;
    if !(30..=600).contains(&timeout_seconds) {
        anyhow::bail!("CROWN_INDEX_JACKETT_TIMEOUT_SECONDS must be between 30 and 600");
    }
    Ok(Some(JackettConfig {
        base_url,
        api_key: Secret(api_key),
        indexers,
        poll_interval: Duration::from_secs(poll_seconds),
        request_timeout: Duration::from_secs(timeout_seconds),
    }))
}

fn parse_indexers(value: &str) -> Result<Vec<String>> {
    let mut indexers = Vec::new();
    for indexer in value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if !indexer
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            anyhow::bail!(
                "Jackett indexer IDs may contain only ASCII letters, numbers, and hyphens"
            );
        }
        if !indexers.iter().any(|existing| existing == indexer) {
            indexers.push(indexer.to_owned());
        }
    }
    if indexers.is_empty() {
        anyhow::bail!("CROWN_INDEX_JACKETT_INDEXERS must contain at least one indexer ID");
    }
    Ok(indexers)
}

fn env_value(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn parse_bool(value: &str) -> Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        _ => anyhow::bail!("invalid boolean value"),
    }
}

fn parse_base_url(name: &str, value: &str) -> Result<Url> {
    let url = Url::parse(value).with_context(|| format!("{name} must be an absolute URL"))?;
    if !matches!(url.scheme(), "http" | "https") || url.cannot_be_a_base() {
        anyhow::bail!("{name} must use HTTP or HTTPS and support relative paths");
    }
    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::{Secret, parse_bool, parse_indexers, validate_source_id};

    #[test]
    fn secret_debug_is_redacted() {
        let key = "do-not-print-this-key";
        let rendered = format!("{:?}", Secret(key.to_owned()));
        assert!(rendered.contains("redacted"));
        assert!(!rendered.contains(key));
    }

    #[test]
    fn indexer_ids_are_validated_and_deduplicated() {
        assert_eq!(
            parse_indexers("yts, nyaasi,yts").expect("valid"),
            vec!["yts", "nyaasi"]
        );
        assert!(parse_indexers("all/../../config").is_err());
    }

    #[test]
    fn booleans_accept_operator_friendly_values() {
        assert!(parse_bool("yes").expect("yes"));
        assert!(!parse_bool("off").expect("off"));
        assert!(parse_bool("maybe").is_err());
    }

    #[test]
    fn butter_source_ids_are_safe_database_identifiers() {
        assert!(validate_source_id("redcrown-fallbacks").is_ok());
        assert!(validate_source_id("../../other").is_err());
    }
}
