//! Exercises additive enrichment against a compatible `PostgreSQL` database.

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use sha1::{Digest, Sha1};

use super::{Browse, CatalogStore, MediaKind, Sort};
use crate::media_match::{ParsedKind, ParsedMedia};
use crate::tmdb::TmdbContent;

#[derive(Debug)]
struct AdditiveFixture {
    store: CatalogStore,
    source: String,
    exact_source: String,
    content_id: String,
    imdb_id: String,
    tmdb_id: i32,
    info_hash: Vec<u8>,
    info_hash_hex: String,
}

impl AdditiveFixture {
    async fn create(database_url: &str) -> Result<Self> {
        let store = CatalogStore::connect(database_url).await?;
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock predates Unix epoch")?
            .as_nanos();
        let source = format!("crown-index-test-{}-{nonce}", std::process::id());
        let exact_source = format!("{source}-exact");
        let content_id = format!("fallback-{nonce}");
        let imdb_id = format!("tt{:09}", nonce % 1_000_000_000);
        let tmdb_id = -i32::try_from((nonce % 1_000_000_000) + 1)
            .context("test TMDB identifier is out of range")?;
        let info_hash = Sha1::digest(format!("{source}:{content_id}")).to_vec();
        let info_hash_hex = hex_string(&info_hash);

        sqlx::query(
            "INSERT INTO metadata_sources (key, name, created_at, updated_at)
             VALUES ($1, $1, now(), now())",
        )
        .bind(&source)
        .execute(&store.pool)
        .await?;
        sqlx::query(
            "INSERT INTO metadata_sources (key, name, created_at, updated_at)
             VALUES ($1, $1, now(), now())",
        )
        .bind(&exact_source)
        .execute(&store.pool)
        .await?;
        sqlx::query(
            "INSERT INTO content (
                type, source, id, title, release_year, adult,
                created_at, updated_at, tsv
             ) VALUES (
                'movie', $1, $2, 'Fallback title', 2026, false,
                now(), now(), to_tsvector('simple', 'Fallback title')
             )",
        )
        .bind(&source)
        .bind(&content_id)
        .execute(&store.pool)
        .await?;
        sqlx::query(
            "INSERT INTO content (
                type, source, id, title, release_year, adult,
                created_at, updated_at, tsv
             ) VALUES (
                'movie', $1, $2, 'Exact ID alias', 2026, false,
                now(), now(), to_tsvector('simple', 'Exact ID alias')
             )",
        )
        .bind(&exact_source)
        .bind(&imdb_id)
        .execute(&store.pool)
        .await?;
        sqlx::query(
            "INSERT INTO torrents (
                info_hash, name, size, private, created_at, updated_at
             ) VALUES ($1, 'Fallback.Title.2026.1080p', 1000, false, now(), now())",
        )
        .bind(&info_hash)
        .execute(&store.pool)
        .await?;
        sqlx::query(
            "INSERT INTO crown_index.ingested_torrents (source, info_hash, imported_at)
             VALUES ($1, $2, now() + interval '1 day')",
        )
        .bind(&source)
        .bind(&info_hash)
        .execute(&store.pool)
        .await?;
        sqlx::query(
            "INSERT INTO torrent_contents (
                info_hash, content_type, content_source, content_id,
                video_resolution, created_at, updated_at, published_at, size
             ) VALUES ($1, 'movie', $2, $3, 'V1080p', now(), now(), now(), 1000)",
        )
        .bind(&info_hash)
        .bind(&source)
        .bind(&content_id)
        .execute(&store.pool)
        .await?;

        Ok(Self {
            store,
            source,
            exact_source,
            content_id,
            imdb_id,
            tmdb_id,
            info_hash,
            info_hash_hex,
        })
    }

    async fn verify_enrichment(&self) -> Result<(i64, i64, i64, i64, String)> {
        let parsed = ParsedMedia {
            kind: ParsedKind::Movie,
            title: "Canonical title".to_owned(),
            year: Some(2026),
        };
        let content = TmdbContent {
            kind: ParsedKind::Movie,
            id: self.tmdb_id,
            title: "Canonical title".to_owned(),
            original_title: None,
            release_date: Some("2026-01-01".to_owned()),
            release_year: Some(2026),
            overview: None,
            runtime: None,
            popularity: None,
            vote_average: None,
            vote_count: None,
            poster_path: None,
            backdrop_path: None,
            original_language: Some("en".to_owned()),
            adult: Some(false),
            genres: Vec::new(),
            imdb_id: Some(self.imdb_id.clone()),
        };
        self.store
            .persist_tmdb_match(
                &self.info_hash_hex,
                "Fallback.Title.2026.1080p",
                &parsed,
                &content,
            )
            .await?;

        sqlx::query_as(
            "SELECT
                COUNT(*) FILTER (WHERE content_source = $2)::bigint,
                COUNT(*) FILTER (
                    WHERE content_source = 'tmdb' AND content_id = $3
                )::bigint,
                (SELECT COUNT(*) FROM crown_index.content_correlations
                 WHERE content_type = 'movie' AND content_source = $2
                   AND content_id = $4 AND canonical_source = 'tmdb'
                   AND canonical_id = $3)::bigint,
                (SELECT COUNT(*) FROM crown_index.content_correlations
                 WHERE content_type = 'movie' AND content_source = $5
                   AND content_id = $6 AND canonical_source = 'tmdb'
                   AND canonical_id = $3
                   AND evidence_kind = 'exact_imdb_id')::bigint,
                (SELECT title FROM content
                 WHERE type = 'movie' AND source = $2 AND id = $4)
             FROM torrent_contents WHERE info_hash = $1",
        )
        .bind(&self.info_hash)
        .bind(&self.source)
        .bind(self.tmdb_id.to_string())
        .bind(&self.content_id)
        .bind(&self.exact_source)
        .bind(&self.imdb_id)
        .fetch_one(&self.store.pool)
        .await
        .context("failed to inspect additive enrichment result")
    }

    async fn cleanup(&self) -> Result<()> {
        sqlx::query("DELETE FROM crown_index.tmdb_enrichment_attempts WHERE info_hash = $1")
            .bind(&self.info_hash)
            .execute(&self.store.pool)
            .await?;
        sqlx::query("DELETE FROM crown_index.ingested_torrents WHERE info_hash = $1")
            .bind(&self.info_hash)
            .execute(&self.store.pool)
            .await?;
        sqlx::query("DELETE FROM torrents WHERE info_hash = $1")
            .bind(&self.info_hash)
            .execute(&self.store.pool)
            .await?;
        sqlx::query("DELETE FROM content WHERE type = 'movie' AND source = 'tmdb' AND id = $1")
            .bind(self.tmdb_id.to_string())
            .execute(&self.store.pool)
            .await?;
        sqlx::query("DELETE FROM metadata_sources WHERE key = $1")
            .bind(&self.source)
            .execute(&self.store.pool)
            .await?;
        sqlx::query("DELETE FROM metadata_sources WHERE key = $1")
            .bind(&self.exact_source)
            .execute(&self.store.pool)
            .await?;
        Ok(())
    }
}

#[derive(Debug)]
struct ShowUnionFixture {
    store: CatalogStore,
    source: String,
    source_id: String,
    imdb_id: String,
    canonical_id: String,
    canonical_hash: Vec<u8>,
    fallback_hash: Vec<u8>,
}

impl ShowUnionFixture {
    async fn create(database_url: &str) -> Result<Self> {
        let store = CatalogStore::connect(database_url).await?;
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock predates Unix epoch")?
            .as_nanos();
        let source = format!("crown-index-show-test-{}-{nonce}", std::process::id());
        let imdb_id = format!("tt{:09}", nonce % 1_000_000_000);
        let source_id = imdb_id.clone();
        let canonical_id = format!("test-{nonce}");
        let canonical_hash = Sha1::digest(format!("canonical:{nonce}")).to_vec();
        let fallback_hash = Sha1::digest(format!("fallback:{nonce}")).to_vec();

        sqlx::query(
            "INSERT INTO metadata_sources (key, name, created_at, updated_at)
             VALUES ($1, $1, now(), now())",
        )
        .bind(&source)
        .execute(&store.pool)
        .await?;
        for (content_source, content_id) in [
            ("tmdb", canonical_id.as_str()),
            (source.as_str(), source_id.as_str()),
        ] {
            sqlx::query(
                "INSERT INTO content (
                    type, source, id, title, adult, created_at, updated_at, tsv
                 ) VALUES (
                    'tv_show', $1, $2, 'Union test show', false,
                    now(), now(), to_tsvector('simple', 'Union test show')
                 )",
            )
            .bind(content_source)
            .bind(content_id)
            .execute(&store.pool)
            .await?;
        }
        for (hash, name) in [
            (&canonical_hash, "Union.Show.S01E01.1080p"),
            (&fallback_hash, "Union.Show.S01E02.1080p"),
        ] {
            sqlx::query(
                "INSERT INTO torrents (
                    info_hash, name, size, private, created_at, updated_at
                 ) VALUES ($1, $2, 1000, false, now(), now())",
            )
            .bind(hash)
            .bind(name)
            .execute(&store.pool)
            .await?;
        }
        let fixture = Self {
            store,
            source,
            source_id,
            imdb_id,
            canonical_id,
            canonical_hash,
            fallback_hash,
        };
        fixture.insert_associations().await?;
        Ok(fixture)
    }

    async fn insert_associations(&self) -> Result<()> {
        sqlx::query(
            "INSERT INTO content_attributes (
                content_type, content_source, content_id,
                source, key, value, created_at, updated_at
             ) VALUES (
                'tv_show', 'tmdb', $1, 'imdb', 'id', $2, now(), now()
             )",
        )
        .bind(&self.canonical_id)
        .bind(&self.imdb_id)
        .execute(&self.store.pool)
        .await?;
        sqlx::query(
            "INSERT INTO torrent_contents (
                info_hash, content_type, content_source, content_id, episodes,
                created_at, updated_at, published_at, size
             ) VALUES
                ($1, 'tv_show', 'tmdb', $3, '{\"1\": {\"1\": true}}',
                 now(), now(), now(), 1000),
                ($2, 'tv_show', $4, $5, '{\"1\": {\"2\": true}}',
                 now(), now(), now(), 1000)",
        )
        .bind(&self.canonical_hash)
        .bind(&self.fallback_hash)
        .bind(&self.canonical_id)
        .bind(&self.source)
        .bind(&self.source_id)
        .execute(&self.store.pool)
        .await?;
        sqlx::query(
            "INSERT INTO crown_index.content_correlations (
                content_type, content_source, content_id,
                canonical_type, canonical_source, canonical_id,
                evidence_info_hash, evidence_kind
             ) VALUES (
                'tv_show', $1, $2, 'tv_show', 'tmdb', $3, $4, 'exact_imdb_id'
             )
             ON CONFLICT (content_type, content_source, content_id) DO NOTHING",
        )
        .bind(&self.source)
        .bind(&self.source_id)
        .bind(&self.canonical_id)
        .bind(&self.fallback_hash)
        .execute(&self.store.pool)
        .await?;
        Ok(())
    }

    async fn cleanup(&self) -> Result<()> {
        for hash in [&self.canonical_hash, &self.fallback_hash] {
            sqlx::query("DELETE FROM torrents WHERE info_hash = $1")
                .bind(hash)
                .execute(&self.store.pool)
                .await?;
        }
        sqlx::query(
            "DELETE FROM content
             WHERE type = 'tv_show' AND source = 'tmdb' AND id = $1",
        )
        .bind(&self.canonical_id)
        .execute(&self.store.pool)
        .await?;
        sqlx::query("DELETE FROM metadata_sources WHERE key = $1")
            .bind(&self.source)
            .execute(&self.store.pool)
            .await?;
        Ok(())
    }
}

fn hex_string(bytes: &[u8]) -> String {
    data_encoding::HEXLOWER.encode(bytes)
}

#[tokio::test]
#[ignore = "requires CROWN_INDEX_TEST_DATABASE_URL pointing at Bitmagnet v0.10.0 PostgreSQL"]
async fn tmdb_enrichment_preserves_source_association_and_adds_correlation() -> Result<()> {
    let database_url = std::env::var("CROWN_INDEX_TEST_DATABASE_URL")
        .context("CROWN_INDEX_TEST_DATABASE_URL is required")?;
    let fixture = AdditiveFixture::create(&database_url).await?;
    let outcome = fixture.verify_enrichment().await;
    let cleanup = fixture.cleanup().await;
    let (source_links, tmdb_links, correlations, exact_correlations, source_title) = outcome?;
    cleanup?;

    assert_eq!(source_links, 1);
    assert_eq!(tmdb_links, 1);
    assert_eq!(correlations, 1);
    assert_eq!(exact_correlations, 1);
    assert_eq!(source_title, "Fallback title");
    Ok(())
}

#[tokio::test]
#[ignore = "requires CROWN_INDEX_TEST_DATABASE_URL pointing at Bitmagnet v0.10.0 PostgreSQL"]
async fn tmdb_transport_errors_retry_after_delay_but_rejections_remain_terminal() -> Result<()> {
    let database_url = std::env::var("CROWN_INDEX_TEST_DATABASE_URL")
        .context("CROWN_INDEX_TEST_DATABASE_URL is required")?;
    let fixture = AdditiveFixture::create(&database_url).await?;
    fixture
        .store
        .record_tmdb_enrichment_failure(
            &fixture.info_hash_hex,
            "Fallback.Title.2026.1080p",
            "error",
            "temporary transport failure",
        )
        .await?;

    let immediate = fixture.store.pending_tmdb_enrichment(100_000).await?;
    let immediate_excludes_recent_error = immediate
        .iter()
        .all(|item| item.info_hash != fixture.info_hash_hex);

    sqlx::query(
        "UPDATE crown_index.tmdb_enrichment_attempts
         SET attempted_at = now() - interval '7 hours'
         WHERE info_hash = $1",
    )
    .bind(&fixture.info_hash)
    .execute(&fixture.store.pool)
    .await?;
    let retryable = fixture.store.pending_tmdb_enrichment(100_000).await?;
    let retryable_contains_old_error = retryable
        .iter()
        .any(|item| item.info_hash == fixture.info_hash_hex);

    sqlx::query(
        "UPDATE crown_index.tmdb_enrichment_attempts
         SET status = 'no_match'
         WHERE info_hash = $1",
    )
    .bind(&fixture.info_hash)
    .execute(&fixture.store.pool)
    .await?;
    let rejected = fixture.store.pending_tmdb_enrichment(100_000).await?;
    let rejected_remains_terminal = rejected
        .iter()
        .all(|item| item.info_hash != fixture.info_hash_hex);
    fixture.cleanup().await?;

    assert!(immediate_excludes_recent_error);
    assert!(retryable_contains_old_error);
    assert!(rejected_remains_terminal);
    Ok(())
}

#[tokio::test]
#[ignore = "requires CROWN_INDEX_TEST_DATABASE_URL pointing at Bitmagnet v0.10.0 PostgreSQL"]
async fn show_details_union_canonical_and_correlated_torrents() -> Result<()> {
    let database_url = std::env::var("CROWN_INDEX_TEST_DATABASE_URL")
        .context("CROWN_INDEX_TEST_DATABASE_URL is required")?;
    let fixture = ShowUnionFixture::create(&database_url).await?;
    let outcome = fixture
        .store
        .episode_torrents("tmdb", &fixture.canonical_id)
        .await;
    let cleanup = fixture.cleanup().await;
    let rows = outcome?;
    cleanup?;
    assert_eq!(rows.len(), 2);
    assert!(
        rows.iter()
            .any(|row| row.info_hash == hex_string(&fixture.canonical_hash))
    );
    assert!(
        rows.iter()
            .any(|row| row.info_hash == hex_string(&fixture.fallback_hash))
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires CROWN_INDEX_TEST_DATABASE_URL pointing at Bitmagnet v0.10.0 PostgreSQL"]
async fn catalog_snapshot_publishes_additive_torrent_union() -> Result<()> {
    let database_url = std::env::var("CROWN_INDEX_TEST_DATABASE_URL")
        .context("CROWN_INDEX_TEST_DATABASE_URL is required")?;
    let fixture = ShowUnionFixture::create(&database_url).await?;
    let outcome = async {
        fixture.store.refresh_catalog_snapshot().await?;
        fixture
            .store
            .browse(&Browse {
                kind: MediaKind::Series,
                page: 1,
                sort: Sort::Title,
                keywords: Some("Union test show".to_owned()),
                genre: None,
            })
            .await
    }
    .await;
    let cleanup = fixture.cleanup().await;
    cleanup?;
    fixture.store.refresh_catalog_snapshot().await?;
    let rows = outcome?
        .into_iter()
        .filter(|row| row.content_source == "tmdb" && row.content_id == fixture.canonical_id)
        .collect::<Vec<_>>();

    assert_eq!(rows.len(), 2);
    assert!(
        rows.iter()
            .any(|row| row.info_hash == hex_string(&fixture.canonical_hash))
    );
    assert!(
        rows.iter()
            .any(|row| row.info_hash == hex_string(&fixture.fallback_hash))
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires CROWN_INDEX_TEST_DATABASE_URL pointing at Bitmagnet v0.10.0 PostgreSQL"]
async fn show_identity_resolves_qualified_alias_and_bare_imdb_id() -> Result<()> {
    let database_url = std::env::var("CROWN_INDEX_TEST_DATABASE_URL")
        .context("CROWN_INDEX_TEST_DATABASE_URL is required")?;
    let fixture = ShowUnionFixture::create(&database_url).await?;
    let qualified = fixture
        .store
        .resolve_show(Some(&fixture.source), &fixture.source_id)
        .await;
    let external = fixture.store.resolve_show(None, &fixture.imdb_id).await;
    let cleanup = fixture.cleanup().await;
    let qualified = qualified?.context("qualified alias should resolve")?;
    let external = external?.context("bare IMDb ID should resolve")?;
    cleanup?;

    assert_eq!(qualified.source, "tmdb");
    assert_eq!(qualified.id, fixture.canonical_id);
    assert_eq!(external.source, "tmdb");
    assert_eq!(external.id, fixture.canonical_id);
    Ok(())
}

#[tokio::test]
#[ignore = "requires CROWN_INDEX_TEST_DATABASE_URL pointing at Bitmagnet v0.10.0 PostgreSQL"]
#[expect(
    clippy::too_many_lines,
    reason = "the integration scenario keeps setup, migration, assertions, and cleanup together"
)]
async fn pending_butter_association_preserves_large_episode_file_maps() -> Result<()> {
    let database_url = std::env::var("CROWN_INDEX_TEST_DATABASE_URL")
        .context("CROWN_INDEX_TEST_DATABASE_URL is required")?;
    let store = CatalogStore::connect(&database_url).await?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock predates Unix epoch")?
        .as_nanos();
    let source = format!("crown-index-pending-test-{}-{nonce}", std::process::id());
    let content_id = format!("pending-{nonce}");
    let info_hash = Sha1::digest(format!("{source}:{content_id}")).to_vec();
    sqlx::query(
        "INSERT INTO metadata_sources (key, name, created_at, updated_at)
         VALUES ($1, $1, now(), now())",
    )
    .bind(&source)
    .execute(&store.pool)
    .await?;
    sqlx::query(
        "INSERT INTO content (
            type, source, id, title, adult, created_at, updated_at, tsv
         ) VALUES (
            'tv_show', $1, $2, 'Pending show', false,
            now(), now(), to_tsvector('simple', 'Pending show')
         )",
    )
    .bind(&source)
    .bind(&content_id)
    .execute(&store.pool)
    .await?;
    let mut compact_episodes = serde_json::Map::new();
    for episode in 1..=52 {
        compact_episodes.insert(episode.to_string(), serde_json::Value::Bool(true));
    }
    let compact_episodes = serde_json::json!({"1": compact_episodes});
    sqlx::query(
        "INSERT INTO crown_index.pending_butter_links (
            info_hash, content_type, content_source, content_id,
            episodes, video_resolution
         ) VALUES ($1, 'tv_show', $2, $3, $4, 'V1080p')",
    )
    .bind(&info_hash)
    .bind(&source)
    .bind(&content_id)
    .bind(compact_episodes)
    .execute(&store.pool)
    .await?;
    for episode in 1..=52_i32 {
        let file_path = format!(
            "Season 1/An intentionally descriptive release path for episode {episode:02} - S01E{episode:02}.mkv"
        );
        sqlx::query(
            "INSERT INTO crown_index.butter_episode_files (
                info_hash, content_type, content_source, content_id,
                season, episode, file_path
             ) VALUES ($1, 'tv_show', $2, $3, 1, $4, $5)",
        )
        .bind(&info_hash)
        .bind(&source)
        .bind(&content_id)
        .bind(episode)
        .bind(file_path)
        .execute(&store.pool)
        .await?;
    }

    let _reconciled_existing = store.reconcile_pending_butter_links().await?;
    let pending_before: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM crown_index.pending_butter_links
         WHERE info_hash = $1 AND content_source = $2 AND content_id = $3",
    )
    .bind(&info_hash)
    .bind(&source)
    .bind(&content_id)
    .fetch_one(&store.pool)
    .await?;
    sqlx::query(
        "INSERT INTO torrents (
            info_hash, name, size, private, created_at, updated_at
         ) VALUES ($1, 'Pending.Show.S01E01.1080p', 1000, false, now(), now())",
    )
    .bind(&info_hash)
    .execute(&store.pool)
    .await?;
    let after_materialization = store.reconcile_pending_butter_links().await?;
    let episode_rows = store.episode_torrents(&source, &content_id).await?;
    let (links, pending): (i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT count(*) FROM torrent_contents
             WHERE info_hash = $1 AND content_source = $2 AND content_id = $3),
            (SELECT count(*) FROM crown_index.pending_butter_links
             WHERE info_hash = $1 AND content_source = $2 AND content_id = $3)",
    )
    .bind(&info_hash)
    .bind(&source)
    .bind(&content_id)
    .fetch_one(&store.pool)
    .await?;
    sqlx::query("DELETE FROM torrents WHERE info_hash = $1")
        .bind(&info_hash)
        .execute(&store.pool)
        .await?;
    sqlx::query("DELETE FROM metadata_sources WHERE key = $1")
        .bind(&source)
        .execute(&store.pool)
        .await?;

    assert_eq!(pending_before, 1);
    assert!(after_materialization >= 1);
    assert_eq!(links, 1);
    assert_eq!(pending, 0);
    assert_eq!(episode_rows.len(), 1);
    let exact_files = episode_rows[0]
        .episode_files
        .as_ref()
        .context("exact episode files should be restored")?;
    assert_eq!(
        exact_files["1"].as_object().map(serde_json::Map::len),
        Some(52)
    );
    assert_eq!(
        episode_rows[0]
            .episodes
            .as_ref()
            .and_then(|value| value["1"]["1"].as_bool()),
        Some(true)
    );
    Ok(())
}
