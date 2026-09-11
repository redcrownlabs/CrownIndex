//! Reads the pinned Bitmagnet compatibility view.

use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;
use sqlx::{FromRow, PgPool, Postgres, QueryBuilder, Transaction};

use crate::butter::{CatalogItem as ButterItem, Kind as ButterKind};
use crate::media_match::{ParsedKind, ParsedMedia};
use crate::record::TorrentRecord;
use crate::tmdb::TmdbContent;

mod snapshot;

const PAGE_SIZE: i64 = 50;
const ADDITIVE_CORRELATION_REPAIR: &str =
    "2026-07-26-additive-correlations-v2-bitmagnet-external-ids";
/// Delay before retrying an item after a transient TMDB transport failure.
///
/// A multi-hour delay prevents an unavailable upstream from consuming the
/// complete worker cycle while still allowing automatic recovery. Permanent
/// parsing and matching rejections remain terminal.
const TMDB_FAILURE_RETRY_SECONDS: i32 = 6 * 60 * 60;

#[derive(Debug, Clone, Copy)]
pub(crate) enum MediaKind {
    Movie,
    Series,
    Anime,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum Sort {
    Trending,
    Popularity,
    Updated,
    LastAdded,
    Year,
    Title,
    Rating,
}

#[derive(Debug)]
pub(crate) struct Browse {
    pub(crate) kind: MediaKind,
    pub(crate) page: u32,
    pub(crate) sort: Sort,
    pub(crate) keywords: Option<String>,
    pub(crate) genre: Option<String>,
}

#[derive(Debug, Clone, FromRow)]
pub(crate) struct CatalogRow {
    pub(crate) content_source: String,
    pub(crate) content_id: String,
    pub(crate) title: String,
    pub(crate) release_year: Option<i32>,
    pub(crate) overview: Option<String>,
    pub(crate) vote_average: Option<f64>,
    pub(crate) poster_path: Option<String>,
    pub(crate) backdrop_path: Option<String>,
    pub(crate) genres: Vec<String>,
    pub(crate) info_hash: String,
    pub(crate) torrent_name: String,
    pub(crate) size: i64,
    pub(crate) video_resolution: Option<String>,
    pub(crate) seeders: Option<i32>,
    pub(crate) leechers: Option<i32>,
    pub(crate) provider: String,
}

#[derive(Debug, Clone, FromRow)]
pub(crate) struct EpisodeTorrentRow {
    pub(crate) info_hash: String,
    pub(crate) torrent_name: String,
    pub(crate) size: i64,
    pub(crate) video_resolution: Option<String>,
    pub(crate) seeders: Option<i32>,
    pub(crate) leechers: Option<i32>,
    pub(crate) provider: String,
    pub(crate) episodes: Option<Value>,
    pub(crate) episode_files: Option<Value>,
    pub(crate) files: Vec<String>,
}

#[derive(Debug, Clone, FromRow)]
pub(crate) struct ResolvedShow {
    pub(crate) source: String,
    pub(crate) id: String,
    pub(crate) title: String,
}

#[derive(Debug, Clone, Copy, FromRow)]
pub(crate) struct IndexStats {
    pub(crate) movies: i64,
    pub(crate) shows: i64,
    pub(crate) torrents: i64,
}

#[derive(Debug, Clone, FromRow)]
pub(crate) struct BackfillState {
    pub(crate) next_year: i32,
    pub(crate) last_fingerprint: Option<Vec<u8>>,
    pub(crate) repeated_pages: i32,
    pub(crate) completed: bool,
}

#[derive(Debug, Clone, Copy, FromRow)]
pub(crate) struct ButterBackfillState {
    pub(crate) next_page: i32,
    pub(crate) completed: bool,
}

#[derive(Debug, Clone, FromRow)]
pub(crate) struct PendingEnrichment {
    pub(crate) info_hash: String,
    pub(crate) name: String,
    pub(crate) content_type: Option<String>,
    pub(crate) external_id: Option<String>,
}

#[derive(Debug, Clone, FromRow, serde::Serialize)]
pub(crate) struct OperatorSyncJob {
    pub(crate) id: i64,
    pub(crate) query: String,
    pub(crate) requested_indexers: Vec<String>,
    pub(crate) status: String,
    pub(crate) phase: String,
    pub(crate) fetched: i32,
    pub(crate) imported: i32,
    pub(crate) skipped: i32,
    pub(crate) deferred: i32,
    pub(crate) saturated_sources: i32,
    pub(crate) matched: i32,
    pub(crate) rejected: i32,
    pub(crate) enrichment_pending: i32,
    pub(crate) failed: i32,
    pub(crate) error: Option<String>,
    pub(crate) created_at: i64,
    pub(crate) started_at: Option<i64>,
    pub(crate) finished_at: Option<i64>,
}

#[derive(Debug, Clone, FromRow, serde::Serialize)]
pub(crate) struct OperatorSyncSource {
    pub(crate) indexer: String,
    pub(crate) status: String,
    pub(crate) fetched: i32,
    pub(crate) imported: i32,
    pub(crate) skipped: i32,
    pub(crate) deferred: i32,
    pub(crate) saturated: bool,
    pub(crate) error: Option<String>,
    pub(crate) started_at: Option<i64>,
    pub(crate) finished_at: Option<i64>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct OperatorSyncJobDetail {
    #[serde(flatten)]
    pub(crate) job: OperatorSyncJob,
    pub(crate) sources: Vec<OperatorSyncSource>,
}

#[derive(Debug, Clone)]
pub(crate) struct CatalogStore {
    pool: PgPool,
}

async fn ensure_content_correlation_evidence_constraint(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<()> {
    sqlx::query(
        "ALTER TABLE crown_index.content_correlations
         ADD COLUMN IF NOT EXISTS evidence_kind text NOT NULL
             DEFAULT 'shared_info_hash' CHECK (
                evidence_kind IN (
                    'shared_info_hash', 'exact_imdb_id', 'exact_tmdb_id'
                )
             )",
    )
    .execute(&mut **transaction)
    .await
    .context("failed to add content correlation evidence kind")?;
    sqlx::query("LOCK TABLE crown_index.content_correlations IN ACCESS EXCLUSIVE MODE")
        .execute(&mut **transaction)
        .await
        .context("failed to lock content correlations for constraint migration")?;
    sqlx::query(
        "ALTER TABLE crown_index.content_correlations
         DROP CONSTRAINT IF EXISTS content_correlations_evidence_kind_check",
    )
    .execute(&mut **transaction)
    .await
    .context("failed to replace content correlation evidence constraint")?;
    sqlx::query(
        "ALTER TABLE crown_index.content_correlations
         ADD CONSTRAINT content_correlations_evidence_kind_check CHECK (
            evidence_kind IN (
                'shared_info_hash', 'exact_imdb_id', 'exact_tmdb_id'
            )
         )",
    )
    .execute(&mut **transaction)
    .await
    .context("failed to constrain content correlation evidence")?;
    Ok(())
}

async fn ensure_content_correlation_schema(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS crown_index.content_correlations (
            content_type text NOT NULL,
            content_source text NOT NULL,
            content_id text NOT NULL,
            canonical_type text NOT NULL,
            canonical_source text NOT NULL,
            canonical_id text NOT NULL,
            evidence_info_hash bytea NOT NULL CHECK (
                octet_length(evidence_info_hash) = 20
            ),
            evidence_kind text NOT NULL DEFAULT 'shared_info_hash' CHECK (
                evidence_kind IN (
                    'shared_info_hash', 'exact_imdb_id', 'exact_tmdb_id'
                )
            ),
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (content_type, content_source, content_id),
            FOREIGN KEY (content_type, content_source, content_id)
                REFERENCES content(type, source, id) ON DELETE CASCADE,
            FOREIGN KEY (canonical_type, canonical_source, canonical_id)
                REFERENCES content(type, source, id) ON DELETE CASCADE,
            CHECK (content_type = canonical_type)
         )",
    )
    .execute(&mut **transaction)
    .await
    .context("failed to create content correlation table")?;
    ensure_content_correlation_evidence_constraint(transaction).await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS content_correlations_canonical_idx
         ON crown_index.content_correlations (
            canonical_type, canonical_source, canonical_id
         )",
    )
    .execute(&mut **transaction)
    .await
    .context("failed to index canonical content correlations")?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS crown_index_torrent_contents_identity_idx
         ON torrent_contents (
            content_type, content_source, content_id, info_hash
         )",
    )
    .execute(&mut **transaction)
    .await
    .context("failed to index torrent content identities")?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS crown_index.schema_migrations (
            key text PRIMARY KEY,
            applied_at timestamptz NOT NULL DEFAULT now()
         )",
    )
    .execute(&mut **transaction)
    .await
    .context("failed to create CrownIndex migration ledger")?;
    let claimed = sqlx::query_scalar::<_, String>(
        "INSERT INTO crown_index.schema_migrations (key)
         VALUES ($1)
         ON CONFLICT (key) DO NOTHING
         RETURNING key",
    )
    .bind(ADDITIVE_CORRELATION_REPAIR)
    .fetch_optional(&mut **transaction)
    .await
    .context("failed to claim additive correlation repair")?;
    if claimed.is_some() {
        repair_shared_content_correlations(transaction).await?;
        repair_exact_content_correlations(transaction).await?;
    }
    Ok(())
}

async fn repair_shared_content_correlations(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<()> {
    sqlx::query(
        "WITH candidates AS (
            SELECT alias.content_type, alias.content_source, alias.content_id,
                   canonical.content_type AS canonical_type,
                   canonical.content_source AS canonical_source,
                   canonical.content_id AS canonical_id,
                   (array_agg(alias.info_hash ORDER BY alias.info_hash))[1]
                       AS evidence_info_hash
            FROM torrent_contents alias
            JOIN torrent_contents canonical
              ON canonical.info_hash = alias.info_hash
             AND canonical.content_type = alias.content_type
             AND canonical.content_source = 'tmdb'
            WHERE alias.content_source IS NOT NULL
              AND alias.content_source <> 'tmdb'
              AND alias.content_id IS NOT NULL
            GROUP BY alias.content_type, alias.content_source, alias.content_id,
                     canonical.content_type, canonical.content_source,
                     canonical.content_id
         ), unambiguous AS (
            SELECT content_type, content_source, content_id,
                   min(canonical_type) AS canonical_type,
                   min(canonical_source) AS canonical_source,
                   min(canonical_id) AS canonical_id,
                   (array_agg(evidence_info_hash ORDER BY evidence_info_hash))[1]
                       AS evidence_info_hash
            FROM candidates
            GROUP BY content_type, content_source, content_id
            HAVING count(*) = 1
         )
         INSERT INTO crown_index.content_correlations (
            content_type, content_source, content_id,
            canonical_type, canonical_source, canonical_id,
            evidence_info_hash
         )
         SELECT content_type, content_source, content_id,
                canonical_type, canonical_source, canonical_id,
                evidence_info_hash
         FROM unambiguous
         ON CONFLICT (content_type, content_source, content_id) DO NOTHING",
    )
    .execute(&mut **transaction)
    .await
    .context("failed to recover content correlations")?;
    Ok(())
}

#[expect(
    clippy::too_many_lines,
    reason = "the exact-correlation SQL must remain one auditable statement"
)]
async fn repair_exact_content_correlations(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<()> {
    sqlx::query(
        "WITH candidates AS (
            SELECT alias.type AS content_type,
                   alias.source AS content_source,
                   alias.id AS content_id,
                   canonical.content_type AS canonical_type,
                   canonical.content_source AS canonical_source,
                   canonical.content_id AS canonical_id,
                   evidence.info_hash AS evidence_info_hash,
                   'exact_imdb_id'::text AS evidence_kind
            FROM content alias
            JOIN LATERAL (
                SELECT tc.info_hash
                FROM torrent_contents tc
                WHERE tc.content_type = alias.type
                  AND tc.content_source = alias.source
                  AND tc.content_id = alias.id
                ORDER BY tc.updated_at DESC
                LIMIT 1
            ) evidence ON true
            JOIN content_attributes canonical
              ON canonical.content_type = alias.type
             AND canonical.content_source = 'tmdb'
             AND (
                    (canonical.source = 'tmdb' AND canonical.key = 'imdb_id')
                 OR (canonical.source = 'imdb' AND canonical.key = 'id')
             )
             AND (
                    canonical.value = alias.id
                 OR EXISTS (
                    SELECT 1
                    FROM content_attributes alias_external
                    WHERE alias_external.content_type = alias.type
                      AND alias_external.content_source = alias.source
                      AND alias_external.content_id = alias.id
                      AND (
                            alias_external.key = 'imdb_id'
                         OR (
                                alias_external.source = 'imdb'
                            AND alias_external.key = 'id'
                         )
                      )
                      AND alias_external.value = canonical.value
                 )
             )
            WHERE alias.source <> 'tmdb'
            UNION ALL
            SELECT alias.type, alias.source, alias.id,
                   canonical.type, canonical.source, canonical.id,
                   evidence.info_hash, 'exact_tmdb_id'::text
            FROM content alias
            JOIN LATERAL (
                SELECT tc.info_hash
                FROM torrent_contents tc
                WHERE tc.content_type = alias.type
                  AND tc.content_source = alias.source
                  AND tc.content_id = alias.id
                ORDER BY tc.updated_at DESC
                LIMIT 1
            ) evidence ON true
            JOIN content_attributes alias_external
              ON alias_external.content_type = alias.type
             AND alias_external.content_source = alias.source
             AND alias_external.content_id = alias.id
             AND alias_external.key = 'tmdb_id'
            JOIN content canonical
              ON canonical.type = alias.type
             AND canonical.source = 'tmdb'
             AND canonical.id = alias_external.value
            WHERE alias.source <> 'tmdb'
         ), unambiguous AS (
            SELECT content_type, content_source, content_id,
                   min(canonical_type) AS canonical_type,
                   min(canonical_source) AS canonical_source,
                   min(canonical_id) AS canonical_id,
                   (array_agg(evidence_info_hash ORDER BY evidence_info_hash))[1]
                       AS evidence_info_hash,
                   CASE
                       WHEN bool_or(evidence_kind = 'exact_tmdb_id')
                           THEN 'exact_tmdb_id'
                       ELSE 'exact_imdb_id'
                   END AS evidence_kind
            FROM candidates
            GROUP BY content_type, content_source, content_id
            HAVING count(DISTINCT (
                canonical_type, canonical_source, canonical_id
            )) = 1
         )
         INSERT INTO crown_index.content_correlations (
            content_type, content_source, content_id,
            canonical_type, canonical_source, canonical_id,
            evidence_info_hash, evidence_kind
         )
         SELECT content_type, content_source, content_id,
                canonical_type, canonical_source, canonical_id,
                evidence_info_hash, evidence_kind
         FROM unambiguous
         ON CONFLICT (content_type, content_source, content_id) DO UPDATE SET
            evidence_info_hash = EXCLUDED.evidence_info_hash,
            evidence_kind = CASE
                WHEN EXCLUDED.evidence_kind = 'exact_tmdb_id'
                    THEN EXCLUDED.evidence_kind
                WHEN crown_index.content_correlations.evidence_kind
                     = 'shared_info_hash'
                    THEN EXCLUDED.evidence_kind
                ELSE crown_index.content_correlations.evidence_kind
            END,
            updated_at = now()
         WHERE crown_index.content_correlations.canonical_type
                   = EXCLUDED.canonical_type
           AND crown_index.content_correlations.canonical_source
                   = EXCLUDED.canonical_source
           AND crown_index.content_correlations.canonical_id
                   = EXCLUDED.canonical_id",
    )
    .execute(&mut **transaction)
    .await
    .context("failed to repair exact content correlations")?;
    Ok(())
}

#[expect(
    clippy::too_many_lines,
    reason = "the pending-correlation SQL must remain one auditable statement"
)]
async fn correlate_pending_butter_links(transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
    sqlx::query(
        "WITH aliases AS (
            SELECT DISTINCT pending.content_type, pending.content_source,
                            pending.content_id
            FROM crown_index.pending_butter_links pending
            JOIN torrents torrent ON torrent.info_hash = pending.info_hash
         ), raw_candidates AS (
            SELECT alias.content_type, alias.content_source, alias.content_id,
                   canonical.content_type AS canonical_type,
                   canonical.content_source AS canonical_source,
                   canonical.content_id AS canonical_id,
                   source_link.info_hash AS evidence_info_hash,
                   'shared_info_hash'::text AS evidence_kind
            FROM aliases alias
            JOIN torrent_contents source_link
              ON source_link.content_type = alias.content_type
             AND source_link.content_source = alias.content_source
             AND source_link.content_id = alias.content_id
            JOIN torrent_contents canonical
              ON canonical.info_hash = source_link.info_hash
             AND canonical.content_type = source_link.content_type
             AND canonical.content_source = 'tmdb'
            UNION ALL
            SELECT alias.content_type, alias.content_source, alias.content_id,
                   canonical.type, canonical.source, canonical.id,
                   source_link.info_hash, 'exact_tmdb_id'::text
            FROM aliases alias
            JOIN torrent_contents source_link
              ON source_link.content_type = alias.content_type
             AND source_link.content_source = alias.content_source
             AND source_link.content_id = alias.content_id
            JOIN content_attributes external
              ON external.content_type = alias.content_type
             AND external.content_source = alias.content_source
             AND external.content_id = alias.content_id
             AND external.key = 'tmdb_id'
            JOIN content canonical
              ON canonical.type = alias.content_type
             AND canonical.source = 'tmdb'
             AND canonical.id = external.value
            UNION ALL
            SELECT alias.content_type, alias.content_source, alias.content_id,
                   canonical.content_type, canonical.content_source,
                   canonical.content_id, source_link.info_hash,
                   'exact_imdb_id'::text
            FROM aliases alias
            JOIN torrent_contents source_link
              ON source_link.content_type = alias.content_type
             AND source_link.content_source = alias.content_source
             AND source_link.content_id = alias.content_id
            JOIN content_attributes canonical
              ON canonical.content_type = alias.content_type
             AND canonical.content_source = 'tmdb'
             AND (
                    (canonical.source = 'tmdb' AND canonical.key = 'imdb_id')
                 OR (canonical.source = 'imdb' AND canonical.key = 'id')
             )
             AND (
                    canonical.value = alias.content_id
                 OR EXISTS (
                    SELECT 1
                    FROM content_attributes external
                    WHERE external.content_type = alias.content_type
                      AND external.content_source = alias.content_source
                      AND external.content_id = alias.content_id
                      AND (
                            external.key = 'imdb_id'
                         OR (
                                external.source = 'imdb'
                            AND external.key = 'id'
                         )
                      )
                      AND external.value = canonical.value
                 )
             )
         ), candidates AS (
            SELECT content_type, content_source, content_id,
                   canonical_type, canonical_source, canonical_id,
                   (array_agg(evidence_info_hash ORDER BY evidence_info_hash))[1]
                       AS evidence_info_hash,
                   CASE
                       WHEN bool_or(evidence_kind = 'exact_tmdb_id')
                           THEN 'exact_tmdb_id'
                       WHEN bool_or(evidence_kind = 'exact_imdb_id')
                           THEN 'exact_imdb_id'
                       ELSE 'shared_info_hash'
                   END AS evidence_kind
            FROM raw_candidates
            GROUP BY content_type, content_source, content_id,
                     canonical_type, canonical_source, canonical_id
         ), unambiguous AS (
            SELECT content_type, content_source, content_id,
                   min(canonical_type) AS canonical_type,
                   min(canonical_source) AS canonical_source,
                   min(canonical_id) AS canonical_id,
                   (array_agg(evidence_info_hash ORDER BY evidence_info_hash))[1]
                       AS evidence_info_hash,
                   CASE
                       WHEN bool_or(evidence_kind = 'exact_tmdb_id')
                           THEN 'exact_tmdb_id'
                       WHEN bool_or(evidence_kind = 'exact_imdb_id')
                           THEN 'exact_imdb_id'
                       ELSE 'shared_info_hash'
                   END AS evidence_kind
            FROM candidates
            GROUP BY content_type, content_source, content_id
            HAVING count(*) = 1
         )
         INSERT INTO crown_index.content_correlations (
            content_type, content_source, content_id,
            canonical_type, canonical_source, canonical_id,
            evidence_info_hash, evidence_kind
         )
         SELECT content_type, content_source, content_id,
                canonical_type, canonical_source, canonical_id,
                evidence_info_hash, evidence_kind
         FROM unambiguous
         ON CONFLICT (content_type, content_source, content_id) DO UPDATE SET
            evidence_info_hash = EXCLUDED.evidence_info_hash,
            evidence_kind = EXCLUDED.evidence_kind,
            updated_at = now()
         WHERE crown_index.content_correlations.canonical_type
                   = EXCLUDED.canonical_type
           AND crown_index.content_correlations.canonical_source
                   = EXCLUDED.canonical_source
           AND crown_index.content_correlations.canonical_id
                   = EXCLUDED.canonical_id",
    )
    .execute(&mut **transaction)
    .await
    .context("failed to correlate pending Butter associations")?;
    Ok(())
}

async fn ensure_pending_butter_schema(transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
    create_pending_butter_table(transaction).await?;
    create_butter_episode_file_table(transaction).await?;
    migrate_pending_butter_episode_files(transaction).await
}

async fn create_pending_butter_table(transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS crown_index.pending_butter_links (
            info_hash bytea NOT NULL CHECK (octet_length(info_hash) = 20),
            content_type text NOT NULL,
            content_source text NOT NULL,
            content_id text NOT NULL,
            episodes jsonb NULL,
            video_resolution text NULL,
            seeders integer NULL,
            leechers integer NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (
                info_hash, content_type, content_source, content_id
            ),
            FOREIGN KEY (content_type, content_source, content_id)
                REFERENCES content(type, source, id) ON DELETE CASCADE
         )",
    )
    .execute(&mut **transaction)
    .await
    .context("failed to create pending Butter association table")?;
    Ok(())
}

async fn create_butter_episode_file_table(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS crown_index.butter_episode_files (
            info_hash bytea NOT NULL CHECK (octet_length(info_hash) = 20),
            content_type text NOT NULL,
            content_source text NOT NULL,
            content_id text NOT NULL,
            season integer NOT NULL CHECK (season >= 0),
            episode integer NOT NULL CHECK (episode > 0),
            file_path text NOT NULL CHECK (file_path <> ''),
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (
                info_hash, content_type, content_source, content_id,
                season, episode
            ),
            FOREIGN KEY (content_type, content_source, content_id)
                REFERENCES content(type, source, id) ON DELETE CASCADE
         )",
    )
    .execute(&mut **transaction)
    .await
    .context("failed to create Butter episode file table")?;
    Ok(())
}

async fn migrate_pending_butter_episode_files(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO crown_index.butter_episode_files (
            info_hash, content_type, content_source, content_id,
            season, episode, file_path
         )
         SELECT pending.info_hash, pending.content_type,
                pending.content_source, pending.content_id,
                season.key::integer, episode.key::integer,
                episode.value #>> '{}'
         FROM crown_index.pending_butter_links pending
         CROSS JOIN LATERAL jsonb_each(
            CASE WHEN jsonb_typeof(pending.episodes) = 'object'
                 THEN pending.episodes ELSE '{}'::jsonb END
         ) season
         CROSS JOIN LATERAL jsonb_each(
            CASE WHEN jsonb_typeof(season.value) = 'object'
                 THEN season.value ELSE '{}'::jsonb END
         ) episode
         WHERE jsonb_typeof(pending.episodes) = 'object'
           AND jsonb_typeof(season.value) = 'object'
           AND jsonb_typeof(episode.value) = 'string'
           AND season.key ~ '^[0-9]+$'
           AND episode.key ~ '^[0-9]+$'
           AND season.key::bigint BETWEEN 0 AND 2147483647
           AND episode.key::bigint BETWEEN 1 AND 2147483647
           AND (episode.value #>> '{}') <> ''
         ON CONFLICT (
            info_hash, content_type, content_source, content_id,
            season, episode
         ) DO UPDATE SET
            file_path = EXCLUDED.file_path,
            updated_at = now()",
    )
    .execute(&mut **transaction)
    .await
    .context("failed to migrate pending Butter episode files")?;
    sqlx::query(
        "WITH episode_seasons AS (
            SELECT pending.info_hash, pending.content_type,
                   pending.content_source, pending.content_id,
                   season.key AS season,
                   jsonb_object_agg(episode.key, true) AS episodes
            FROM crown_index.pending_butter_links pending
            CROSS JOIN LATERAL jsonb_each(
                CASE WHEN jsonb_typeof(pending.episodes) = 'object'
                     THEN pending.episodes ELSE '{}'::jsonb END
            ) season
            CROSS JOIN LATERAL jsonb_each(
                CASE WHEN jsonb_typeof(season.value) = 'object'
                     THEN season.value ELSE '{}'::jsonb END
            ) episode
            WHERE jsonb_typeof(pending.episodes) = 'object'
              AND jsonb_typeof(season.value) = 'object'
            GROUP BY pending.info_hash, pending.content_type,
                     pending.content_source, pending.content_id, season.key
         ), compacted AS (
            SELECT info_hash, content_type, content_source, content_id,
                   jsonb_object_agg(season, episodes) AS episodes
            FROM episode_seasons
            GROUP BY info_hash, content_type, content_source, content_id
         )
         UPDATE crown_index.pending_butter_links pending
         SET episodes = compacted.episodes, updated_at = now()
         FROM compacted
         WHERE pending.info_hash = compacted.info_hash
           AND pending.content_type = compacted.content_type
           AND pending.content_source = compacted.content_source
           AND pending.content_id = compacted.content_id
           AND pending.episodes IS DISTINCT FROM compacted.episodes",
    )
    .execute(&mut **transaction)
    .await
    .context("failed to compact pending Butter episode metadata")?;
    Ok(())
}

async fn ensure_tmdb_enrichment_schema(transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS crown_index.tmdb_enrichment_attempts (
            info_hash bytea PRIMARY KEY CHECK (octet_length(info_hash) = 20),
            torrent_name text NOT NULL,
            parsed_title text NULL,
            parsed_kind text NULL,
            parsed_year integer NULL,
            status text NOT NULL,
            error text NULL,
            content_type text NULL,
            content_source text NULL,
            content_id text NULL,
            attempted_at timestamptz NOT NULL DEFAULT now(),
            matched_at timestamptz NULL
         )",
    )
    .execute(&mut **transaction)
    .await
    .context("failed to create TMDB enrichment attempts table")?;
    Ok(())
}

async fn ensure_operator_phase_constraint(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<()> {
    sqlx::query(
        "DO $migration$
         BEGIN
           IF NOT EXISTS (
             SELECT 1 FROM pg_constraint
             WHERE conname = 'operator_sync_jobs_phase_check'
               AND conrelid = 'crown_index.operator_sync_jobs'::regclass
           ) THEN
             ALTER TABLE crown_index.operator_sync_jobs
             ADD CONSTRAINT operator_sync_jobs_phase_check CHECK (
               phase IN ('queued', 'searching', 'enriching', 'publishing', 'completed')
             );
           END IF;
         END
         $migration$",
    )
    .execute(&mut **transaction)
    .await
    .context("failed to constrain operator sync phases")?;
    Ok(())
}

async fn ensure_operator_sync_schema(transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS crown_index.operator_sync_jobs (
            id bigserial PRIMARY KEY,
            query text NOT NULL CHECK (char_length(query) BETWEEN 3 AND 200),
            requested_indexers text[] NOT NULL CHECK (
                cardinality(requested_indexers) BETWEEN 1 AND 100
            ),
            status text NOT NULL DEFAULT 'queued' CHECK (
                status IN ('queued', 'running', 'completed', 'partial', 'failed')
            ),
            phase text NOT NULL DEFAULT 'queued' CHECK (
                phase IN ('queued', 'searching', 'enriching', 'publishing', 'completed')
            ),
            fetched integer NOT NULL DEFAULT 0 CHECK (fetched >= 0),
            imported integer NOT NULL DEFAULT 0 CHECK (imported >= 0),
            skipped integer NOT NULL DEFAULT 0 CHECK (skipped >= 0),
            deferred integer NOT NULL DEFAULT 0 CHECK (deferred >= 0),
            saturated_sources integer NOT NULL DEFAULT 0 CHECK (saturated_sources >= 0),
            matched integer NOT NULL DEFAULT 0 CHECK (matched >= 0),
            rejected integer NOT NULL DEFAULT 0 CHECK (rejected >= 0),
            enrichment_pending integer NOT NULL DEFAULT 0 CHECK (enrichment_pending >= 0),
            failed integer NOT NULL DEFAULT 0 CHECK (failed >= 0),
            error text NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            started_at timestamptz NULL,
            finished_at timestamptz NULL
         )",
    )
    .execute(&mut **transaction)
    .await
    .context("failed to create operator sync job table")?;
    for statement in [
        "ALTER TABLE crown_index.operator_sync_jobs
         ADD COLUMN IF NOT EXISTS phase text NOT NULL DEFAULT 'queued'",
        "ALTER TABLE crown_index.operator_sync_jobs
         ADD COLUMN IF NOT EXISTS enrichment_pending integer NOT NULL DEFAULT 0
             CHECK (enrichment_pending >= 0)",
        "ALTER TABLE crown_index.operator_sync_jobs
         ADD COLUMN IF NOT EXISTS saturated_sources integer NOT NULL DEFAULT 0
             CHECK (saturated_sources >= 0)",
    ] {
        sqlx::query(statement)
            .execute(&mut **transaction)
            .await
            .context("failed to migrate operator sync job progress")?;
    }
    sqlx::query(
        "UPDATE crown_index.operator_sync_jobs
         SET phase = CASE
             WHEN status IN ('completed', 'partial', 'failed') THEN 'completed'
             WHEN status = 'running' THEN 'searching'
             ELSE 'queued'
         END
         WHERE phase = 'queued' AND status <> 'queued'",
    )
    .execute(&mut **transaction)
    .await
    .context("failed to backfill operator sync phases")?;
    ensure_operator_phase_constraint(transaction).await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS operator_sync_jobs_queue_idx
         ON crown_index.operator_sync_jobs (id)
         WHERE status = 'queued'",
    )
    .execute(&mut **transaction)
    .await
    .context("failed to index queued operator sync jobs")?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS crown_index.operator_sync_sources (
            job_id bigint NOT NULL REFERENCES crown_index.operator_sync_jobs(id)
                ON DELETE CASCADE,
            indexer text NOT NULL,
            status text NOT NULL DEFAULT 'queued' CHECK (
                status IN ('queued', 'searching', 'importing', 'completed', 'failed')
            ),
            fetched integer NOT NULL DEFAULT 0 CHECK (fetched >= 0),
            imported integer NOT NULL DEFAULT 0 CHECK (imported >= 0),
            skipped integer NOT NULL DEFAULT 0 CHECK (skipped >= 0),
            deferred integer NOT NULL DEFAULT 0 CHECK (deferred >= 0),
            saturated boolean NOT NULL DEFAULT false,
            error text NULL,
            started_at timestamptz NULL,
            finished_at timestamptz NULL,
            PRIMARY KEY (job_id, indexer)
         )",
    )
    .execute(&mut **transaction)
    .await
    .context("failed to create operator sync source table")?;
    sqlx::query(
        "ALTER TABLE crown_index.operator_sync_sources
         ADD COLUMN IF NOT EXISTS saturated boolean NOT NULL DEFAULT false",
    )
    .execute(&mut **transaction)
    .await
    .context("failed to migrate operator sync source saturation state")?;
    Ok(())
}

impl CatalogStore {
    pub(crate) async fn connect(database_url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(database_url)
            .await
            .context("failed to connect to Bitmagnet PostgreSQL")?;
        let store = Self { pool };
        store.verify_schema().await?;
        store.ensure_ingestion_schema().await?;
        store.ensure_catalog_snapshot().await?;
        Ok(store)
    }

    async fn ensure_ingestion_schema(&self) -> Result<()> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .context("failed to begin ingestion schema transaction")?;
        sqlx::query("CREATE SCHEMA IF NOT EXISTS crown_index")
            .execute(&mut *transaction)
            .await
            .context("failed to create CrownIndex schema")?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS crown_index.ingested_torrents (
                source text NOT NULL,
                info_hash bytea NOT NULL CHECK (octet_length(info_hash) = 20),
                imported_at timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (source, info_hash)
             )",
        )
        .execute(&mut *transaction)
        .await
        .context("failed to create CrownIndex ingestion state table")?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS crown_index.jackett_downloads (
                source text NOT NULL,
                locator_hash bytea NOT NULL CHECK (octet_length(locator_hash) = 32),
                info_hash bytea NOT NULL CHECK (octet_length(info_hash) = 20),
                resolved_at timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (source, locator_hash)
             )",
        )
        .execute(&mut *transaction)
        .await
        .context("failed to create Jackett download resolution table")?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS crown_index.jackett_resolution_quota (
                source text PRIMARY KEY,
                window_started_at timestamptz NOT NULL,
                attempts integer NOT NULL CHECK (attempts >= 0)
             )",
        )
        .execute(&mut *transaction)
        .await
        .context("failed to create Jackett resolution quota table")?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS crown_index.jackett_backfill_state (
                indexer text PRIMARY KEY,
                next_year integer NOT NULL,
                target_min_year integer NOT NULL,
                last_fingerprint bytea NULL CHECK (
                    last_fingerprint IS NULL OR octet_length(last_fingerprint) = 32
                ),
                repeated_pages integer NOT NULL DEFAULT 0 CHECK (repeated_pages >= 0),
                completed boolean NOT NULL DEFAULT false,
                updated_at timestamptz NOT NULL DEFAULT now()
             )",
        )
        .execute(&mut *transaction)
        .await
        .context("failed to create Jackett backfill state table")?;
        ensure_tmdb_enrichment_schema(&mut transaction).await?;
        ensure_operator_sync_schema(&mut transaction).await?;
        ensure_content_correlation_schema(&mut transaction).await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS crown_index.butter_backfill_state (
                source_id text NOT NULL,
                kind text NOT NULL,
                next_page integer NOT NULL DEFAULT 1 CHECK (next_page >= 1),
                completed boolean NOT NULL DEFAULT false,
                updated_at timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (source_id, kind)
             )",
        )
        .execute(&mut *transaction)
        .await
        .context("failed to create Butter backfill state table")?;
        ensure_pending_butter_schema(&mut transaction).await?;
        snapshot::ensure_schema(&mut transaction).await?;
        transaction
            .commit()
            .await
            .context("failed to commit ingestion schema transaction")?;
        Ok(())
    }

    pub(crate) async fn unseen_records(
        &self,
        records: &[TorrentRecord],
    ) -> Result<Vec<TorrentRecord>> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        let keys = records
            .iter()
            .map(|record| Ok((record.source.clone(), record.info_hash_bytes()?.to_vec())))
            .collect::<Result<Vec<_>>>()?;
        let mut query = QueryBuilder::<Postgres>::new(
            "SELECT source, encode(info_hash, 'hex') FROM crown_index.ingested_torrents WHERE false",
        );
        for (source, info_hash) in &keys {
            query
                .push(" OR (source = ")
                .push_bind(source)
                .push(" AND info_hash = ")
                .push_bind(info_hash)
                .push(")");
        }
        let existing = query
            .build_query_as::<(String, String)>()
            .fetch_all(&self.pool)
            .await
            .context("failed to read ingestion state")?
            .into_iter()
            .collect::<HashSet<_>>();
        Ok(records
            .iter()
            .filter(|record| !existing.contains(&(record.source.clone(), record.info_hash.clone())))
            .cloned()
            .collect())
    }

    pub(crate) async fn enqueue_operator_sync(
        &self,
        query: &str,
        indexers: &[String],
    ) -> Result<OperatorSyncJob> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .context("failed to begin operator sync transaction")?;
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO crown_index.operator_sync_jobs (query, requested_indexers)
             VALUES ($1, $2)
             RETURNING id",
        )
        .bind(query)
        .bind(indexers)
        .fetch_one(&mut *transaction)
        .await
        .context("failed to enqueue operator sync")?;
        let mut sources = QueryBuilder::<Postgres>::new(
            "INSERT INTO crown_index.operator_sync_sources (job_id, indexer) ",
        );
        sources.push_values(indexers, |mut row, indexer| {
            row.push_bind(id).push_bind(indexer);
        });
        sources
            .build()
            .execute(&mut *transaction)
            .await
            .context("failed to initialize operator sync sources")?;
        transaction
            .commit()
            .await
            .context("failed to commit operator sync transaction")?;
        self.operator_sync_job(id)
            .await?
            .map(|detail| detail.job)
            .context("new operator sync job disappeared")
    }

    pub(crate) async fn requeue_interrupted_operator_syncs(&self) -> Result<u64> {
        let result = sqlx::query(
            "WITH interrupted AS (
                UPDATE crown_index.operator_sync_jobs
                SET status = 'queued', phase = 'queued', started_at = NULL,
                    error = 'resumed after CrownIndex restarted'
                WHERE status = 'running'
                RETURNING id
             )
             UPDATE crown_index.operator_sync_sources source
             SET status = 'queued', started_at = NULL, finished_at = NULL,
                 error = NULL
             FROM interrupted
             WHERE source.job_id = interrupted.id
               AND source.status IN ('searching', 'importing')",
        )
        .execute(&self.pool)
        .await
        .context("failed to requeue interrupted operator syncs")?;
        Ok(result.rows_affected())
    }

    pub(crate) async fn claim_operator_sync(&self) -> Result<Option<OperatorSyncJob>> {
        sqlx::query_as(
            "WITH next_job AS (
                SELECT id
                FROM crown_index.operator_sync_jobs
                WHERE status = 'queued'
                ORDER BY id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
             ), claimed AS (
                UPDATE crown_index.operator_sync_jobs job
                SET status = 'running', phase = 'searching',
                    started_at = now(), finished_at = NULL,
                    error = NULL
                FROM next_job
                WHERE job.id = next_job.id
                RETURNING job.*
             )
             SELECT id, query, requested_indexers, status, phase,
                    fetched, imported, skipped, deferred, saturated_sources, matched, rejected,
                    enrichment_pending, failed,
                    error,
                    EXTRACT(EPOCH FROM created_at)::bigint AS created_at,
                    EXTRACT(EPOCH FROM started_at)::bigint AS started_at,
                    EXTRACT(EPOCH FROM finished_at)::bigint AS finished_at
             FROM claimed",
        )
        .fetch_optional(&self.pool)
        .await
        .context("failed to claim operator sync")
    }

    pub(crate) async fn operator_sync_source_started(
        &self,
        job_id: i64,
        indexer: &str,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE crown_index.operator_sync_sources
             SET status = 'searching', started_at = now(), finished_at = NULL,
                 error = NULL
             WHERE job_id = $1 AND indexer = $2",
        )
        .bind(job_id)
        .bind(indexer)
        .execute(&self.pool)
        .await
        .context("failed to start operator sync source")?;
        Ok(())
    }

    pub(crate) async fn set_operator_sync_phase(&self, id: i64, phase: &str) -> Result<()> {
        sqlx::query(
            "UPDATE crown_index.operator_sync_jobs
             SET phase = $2
             WHERE id = $1 AND status = 'running'",
        )
        .bind(id)
        .bind(phase)
        .execute(&self.pool)
        .await
        .context("failed to update operator sync phase")?;
        Ok(())
    }

    pub(crate) async fn operator_sync_source_importing(
        &self,
        job_id: i64,
        indexer: &str,
        fetched: i32,
        skipped: i32,
        deferred: i32,
        saturated: bool,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE crown_index.operator_sync_sources
             SET status = 'importing', fetched = $3, skipped = $4, deferred = $5,
                 saturated = $6
             WHERE job_id = $1 AND indexer = $2",
        )
        .bind(job_id)
        .bind(indexer)
        .bind(fetched)
        .bind(skipped)
        .bind(deferred)
        .bind(saturated)
        .execute(&self.pool)
        .await
        .context("failed to update operator sync source")?;
        Ok(())
    }

    pub(crate) async fn finish_operator_sync_source(
        &self,
        job_id: i64,
        indexer: &str,
        imported: i32,
        error: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE crown_index.operator_sync_sources
             SET status = CASE WHEN $4::text IS NULL THEN 'completed' ELSE 'failed' END,
                 imported = $3, error = $4, finished_at = now()
             WHERE job_id = $1 AND indexer = $2",
        )
        .bind(job_id)
        .bind(indexer)
        .bind(imported)
        .bind(error.map(truncate_operator_error))
        .execute(&self.pool)
        .await
        .context("failed to finish operator sync source")?;
        Ok(())
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "job totals form one persisted progress snapshot"
    )]
    pub(crate) async fn finish_operator_sync(
        &self,
        id: i64,
        status: &str,
        fetched: i32,
        imported: i32,
        skipped: i32,
        deferred: i32,
        saturated_sources: i32,
        matched: i32,
        rejected: i32,
        enrichment_pending: i32,
        failed: i32,
        error: Option<&str>,
    ) -> Result<()> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .context("failed to begin operator sync completion")?;
        sqlx::query(
            "UPDATE crown_index.operator_sync_sources
             SET status = 'failed', error = COALESCE(error, $2), finished_at = now()
             WHERE job_id = $1 AND status IN ('queued', 'searching', 'importing')",
        )
        .bind(id)
        .bind(error.map(truncate_operator_error))
        .execute(&mut *transaction)
        .await
        .context("failed to close unfinished operator sync sources")?;
        sqlx::query(
            "UPDATE crown_index.operator_sync_jobs
             SET status = $2, phase = 'completed',
                 fetched = $3, imported = $4, skipped = $5,
                 deferred = $6, saturated_sources = $7, matched = $8, rejected = $9,
                 enrichment_pending = $10, failed = $11,
                 error = $12, finished_at = now()
             WHERE id = $1 AND status = 'running'",
        )
        .bind(id)
        .bind(status)
        .bind(fetched)
        .bind(imported)
        .bind(skipped)
        .bind(deferred)
        .bind(saturated_sources)
        .bind(matched)
        .bind(rejected)
        .bind(enrichment_pending)
        .bind(failed)
        .bind(error.map(truncate_operator_error))
        .execute(&mut *transaction)
        .await
        .context("failed to finish operator sync")?;
        transaction
            .commit()
            .await
            .context("failed to commit operator sync completion")?;
        Ok(())
    }

    pub(crate) async fn operator_sync_jobs(&self, limit: i64) -> Result<Vec<OperatorSyncJob>> {
        sqlx::query_as(&operator_sync_job_query("ORDER BY id DESC LIMIT $1"))
            .bind(limit)
            .fetch_all(&self.pool)
            .await
            .context("failed to list operator sync jobs")
    }

    pub(crate) async fn operator_sync_job(&self, id: i64) -> Result<Option<OperatorSyncJobDetail>> {
        let job = sqlx::query_as(&operator_sync_job_query("WHERE id = $1"))
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .context("failed to read operator sync job")?;
        let Some(job) = job else {
            return Ok(None);
        };
        let sources = sqlx::query_as(
            "SELECT indexer, status, fetched, imported, skipped, deferred, saturated, error,
                    EXTRACT(EPOCH FROM started_at)::bigint AS started_at,
                    EXTRACT(EPOCH FROM finished_at)::bigint AS finished_at
             FROM crown_index.operator_sync_sources
             WHERE job_id = $1
             ORDER BY indexer",
        )
        .bind(id)
        .fetch_all(&self.pool)
        .await
        .context("failed to read operator sync sources")?;
        Ok(Some(OperatorSyncJobDetail { job, sources }))
    }

    pub(crate) async fn mark_ingested(&self, records: &[TorrentRecord]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let keys = records
            .iter()
            .map(|record| Ok((record.source.clone(), record.info_hash_bytes()?.to_vec())))
            .collect::<Result<Vec<_>>>()?;
        let mut query = QueryBuilder::<Postgres>::new(
            "INSERT INTO crown_index.ingested_torrents (source, info_hash) ",
        );
        query.push_values(&keys, |mut row, (source, info_hash)| {
            row.push_bind(source).push_bind(info_hash);
        });
        query.push(" ON CONFLICT (source, info_hash) DO NOTHING");
        query
            .build()
            .execute(&self.pool)
            .await
            .context("failed to persist ingestion state")?;
        Ok(())
    }

    pub(crate) async fn resolved_jackett_download(
        &self,
        source: &str,
        locator_hash: &[u8; 32],
    ) -> Result<Option<String>> {
        sqlx::query_scalar(
            "SELECT encode(info_hash, 'hex')
             FROM crown_index.jackett_downloads
             WHERE source = $1 AND locator_hash = $2",
        )
        .bind(source)
        .bind(locator_hash.as_slice())
        .fetch_optional(&self.pool)
        .await
        .context("failed to read Jackett download resolution")
    }

    pub(crate) async fn remember_jackett_download(
        &self,
        source: &str,
        locator_hash: &[u8; 32],
        info_hash: &[u8; 20],
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO crown_index.jackett_downloads (source, locator_hash, info_hash)
             VALUES ($1, $2, $3)
             ON CONFLICT (source, locator_hash) DO UPDATE
             SET info_hash = EXCLUDED.info_hash, resolved_at = now()",
        )
        .bind(source)
        .bind(locator_hash.as_slice())
        .bind(info_hash.as_slice())
        .execute(&self.pool)
        .await
        .context("failed to persist Jackett download resolution")?;
        Ok(())
    }

    pub(crate) async fn reserve_jackett_resolution(
        &self,
        source: &str,
        hourly_limit: i32,
    ) -> Result<bool> {
        let reserved = sqlx::query_scalar::<_, bool>(
            "INSERT INTO crown_index.jackett_resolution_quota (
                source, window_started_at, attempts
             ) VALUES ($1, date_trunc('hour', now()), 1)
             ON CONFLICT (source) DO UPDATE SET
                window_started_at = CASE
                    WHEN crown_index.jackett_resolution_quota.window_started_at
                         < date_trunc('hour', now())
                    THEN date_trunc('hour', now())
                    ELSE crown_index.jackett_resolution_quota.window_started_at
                END,
                attempts = CASE
                    WHEN crown_index.jackett_resolution_quota.window_started_at
                         < date_trunc('hour', now())
                    THEN 1
                    ELSE crown_index.jackett_resolution_quota.attempts + 1
                END
             WHERE crown_index.jackett_resolution_quota.window_started_at
                       < date_trunc('hour', now())
                OR crown_index.jackett_resolution_quota.attempts < $2
             RETURNING true",
        )
        .bind(source)
        .bind(hourly_limit)
        .fetch_optional(&self.pool)
        .await
        .context("failed to reserve Jackett metadata resolution quota")?;
        Ok(reserved.unwrap_or(false))
    }

    pub(crate) async fn current_year(&self) -> Result<i32> {
        sqlx::query_scalar("SELECT EXTRACT(YEAR FROM CURRENT_DATE)::integer")
            .fetch_one(&self.pool)
            .await
            .context("failed to read the database current year")
    }

    pub(crate) async fn butter_backfill_state(
        &self,
        source_id: &str,
        kind: ButterKind,
    ) -> Result<ButterBackfillState> {
        sqlx::query(
            "INSERT INTO crown_index.butter_backfill_state (source_id, kind)
             VALUES ($1, $2)
             ON CONFLICT (source_id, kind) DO NOTHING",
        )
        .bind(source_id)
        .bind(kind.state_key())
        .execute(&self.pool)
        .await
        .context("failed to initialize Butter backfill state")?;
        sqlx::query_as(
            "SELECT next_page, completed
             FROM crown_index.butter_backfill_state
             WHERE source_id = $1 AND kind = $2",
        )
        .bind(source_id)
        .bind(kind.state_key())
        .fetch_one(&self.pool)
        .await
        .context("failed to read Butter backfill state")
    }

    pub(crate) async fn advance_butter_backfill(
        &self,
        source_id: &str,
        kind: ButterKind,
        expected_page: i32,
        completed: bool,
    ) -> Result<()> {
        let result = sqlx::query(
            "UPDATE crown_index.butter_backfill_state
             SET next_page = CASE WHEN $4 THEN next_page ELSE next_page + 1 END,
                 completed = $4,
                 updated_at = now()
             WHERE source_id = $1 AND kind = $2
               AND next_page = $3 AND completed = false",
        )
        .bind(source_id)
        .bind(kind.state_key())
        .bind(expected_page)
        .bind(completed)
        .execute(&self.pool)
        .await
        .context("failed to advance Butter backfill state")?;
        if result.rows_affected() != 1 {
            anyhow::bail!("Butter backfill state changed concurrently");
        }
        Ok(())
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the normalized Bitmagnet writes must remain transactional and auditable together"
    )]
    pub(crate) async fn persist_butter_items(
        &self,
        source_id: &str,
        items: &[ButterItem],
    ) -> Result<()> {
        let source = format!("butter-{source_id}");
        let mut transaction = self
            .pool
            .begin()
            .await
            .context("failed to begin Butter catalog transaction")?;
        sqlx::query(
            "INSERT INTO metadata_sources (key, name, created_at, updated_at)
             VALUES ($1, $2, now(), now())
             ON CONFLICT (key) DO UPDATE SET name = EXCLUDED.name, updated_at = now()",
        )
        .bind(&source)
        .bind(format!("Butter catalog ({source_id})"))
        .execute(&mut *transaction)
        .await
        .context("failed to register Butter metadata source")?;

        for item in items {
            let content_type = item.kind.content_type();
            sqlx::query(
                "INSERT INTO content (
                    type, source, id, title, release_year, adult,
                    original_title, overview, vote_average,
                    created_at, updated_at, tsv
                 ) VALUES (
                    $1, $2, $3, $4, $5, false,
                    $4, $6, $7, now(), now(), to_tsvector('simple', $4)
                 )
                 ON CONFLICT (type, source, id) DO UPDATE SET
                    title = CASE
                        WHEN content.title = '' THEN EXCLUDED.title
                        ELSE content.title
                    END,
                    release_year = COALESCE(content.release_year, EXCLUDED.release_year),
                    original_title = COALESCE(content.original_title, EXCLUDED.original_title),
                    overview = CASE
                        WHEN COALESCE(content.overview, '') = '' THEN EXCLUDED.overview
                        ELSE content.overview
                    END,
                    vote_average = COALESCE(content.vote_average, EXCLUDED.vote_average),
                    updated_at = now(),
                    tsv = COALESCE(content.tsv, EXCLUDED.tsv)",
            )
            .bind(content_type)
            .bind(&source)
            .bind(&item.id)
            .bind(&item.title)
            .bind(item.year)
            .bind(item.synopsis.as_deref())
            .bind(item.rating)
            .execute(&mut *transaction)
            .await
            .context("failed to upsert Butter content")?;

            for (key, value) in [
                ("poster_path", item.poster.as_deref()),
                ("backdrop_path", item.fanart.as_deref()),
                ("tmdb_id", item.tmdb_id.as_deref()),
            ] {
                if let Some(value) = value {
                    sqlx::query(
                        "INSERT INTO content_attributes (
                            content_type, content_source, content_id,
                            source, key, value, created_at, updated_at
                         ) VALUES ($1, $2, $3, $2, $4, $5, now(), now())
                         ON CONFLICT (
                            content_type, content_source, content_id, source, key
                         ) DO UPDATE SET
                            value = CASE
                                WHEN content_attributes.value = '' THEN EXCLUDED.value
                                ELSE content_attributes.value
                            END,
                            updated_at = now()",
                    )
                    .bind(content_type)
                    .bind(&source)
                    .bind(&item.id)
                    .bind(key)
                    .bind(value)
                    .execute(&mut *transaction)
                    .await
                    .context("failed to upsert Butter image attribute")?;
                }
            }

            for genre in &item.genres {
                let genre_id = normalized_genre_id(genre);
                if genre_id.is_empty() {
                    continue;
                }
                sqlx::query(
                    "INSERT INTO content_collections (
                        type, source, id, name, created_at, updated_at
                     ) VALUES ('genre', $1, $2, $3, now(), now())
                     ON CONFLICT (type, source, id) DO UPDATE SET
                        name = EXCLUDED.name, updated_at = now()",
                )
                .bind(&source)
                .bind(&genre_id)
                .bind(genre)
                .execute(&mut *transaction)
                .await
                .context("failed to upsert Butter genre")?;
                sqlx::query(
                    "INSERT INTO content_collections_content (
                        content_type, content_source, content_id,
                        content_collection_type, content_collection_source,
                        content_collection_id
                     ) VALUES ($1, $2, $3, 'genre', $2, $4)
                     ON CONFLICT DO NOTHING",
                )
                .bind(content_type)
                .bind(&source)
                .bind(&item.id)
                .bind(&genre_id)
                .execute(&mut *transaction)
                .await
                .context("failed to attach Butter genre")?;
            }

            let links = grouped_butter_links(item)?;
            for link in links {
                sqlx::query(
                    "INSERT INTO crown_index.pending_butter_links (
                        info_hash, content_type, content_source, content_id,
                        episodes, video_resolution, seeders, leechers
                     ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                     ON CONFLICT (
                        info_hash, content_type, content_source, content_id
                     ) DO UPDATE SET
                        episodes = CASE
                            WHEN crown_index.pending_butter_links.episodes IS NULL
                                THEN EXCLUDED.episodes
                            WHEN EXCLUDED.episodes IS NULL
                                THEN crown_index.pending_butter_links.episodes
                            ELSE (
                                SELECT jsonb_object_agg(
                                    season,
                                    COALESCE(
                                        crown_index.pending_butter_links.episodes
                                            -> season,
                                        '{}'::jsonb
                                    )
                                    || COALESCE(EXCLUDED.episodes -> season, '{}'::jsonb)
                                )
                                FROM jsonb_object_keys(
                                    crown_index.pending_butter_links.episodes
                                        || EXCLUDED.episodes
                                ) AS keys(season)
                            )
                        END,
                        video_resolution = COALESCE(
                            crown_index.pending_butter_links.video_resolution,
                            EXCLUDED.video_resolution
                        ),
                        seeders = GREATEST(
                            crown_index.pending_butter_links.seeders,
                            EXCLUDED.seeders
                        ),
                        leechers = GREATEST(
                            crown_index.pending_butter_links.leechers,
                            EXCLUDED.leechers
                        ),
                        updated_at = now()",
                )
                .bind(&link.info_hash)
                .bind(content_type)
                .bind(&source)
                .bind(&item.id)
                .bind(link.episodes)
                .bind(link.video_resolution)
                .bind(link.seeders)
                .bind(link.leechers)
                .execute(&mut *transaction)
                .await
                .context("failed to stage Butter torrent association")?;
                for episode_file in link.episode_files {
                    sqlx::query(
                        "INSERT INTO crown_index.butter_episode_files (
                            info_hash, content_type, content_source, content_id,
                            season, episode, file_path
                         ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                         ON CONFLICT (
                            info_hash, content_type, content_source, content_id,
                            season, episode
                         ) DO UPDATE SET
                            file_path = EXCLUDED.file_path,
                            updated_at = now()",
                    )
                    .bind(&link.info_hash)
                    .bind(content_type)
                    .bind(&source)
                    .bind(&item.id)
                    .bind(i32::from(episode_file.season))
                    .bind(i32::from(episode_file.episode))
                    .bind(episode_file.file_path)
                    .execute(&mut *transaction)
                    .await
                    .context("failed to preserve Butter episode file")?;
                }
            }

            // A fallback item becomes an alias only when an exact external ID
            // or shared hash identifies one canonical title. This handles
            // either ingestion order without replacing source-owned data.
            sqlx::query(
                "WITH raw_candidates AS (
                    SELECT canonical.content_type AS canonical_type,
                           canonical.content_source AS canonical_source,
                           canonical.content_id AS canonical_id,
                           source_link.info_hash AS evidence_info_hash,
                           'shared_info_hash'::text AS evidence_kind
                    FROM torrent_contents source_link
                    JOIN torrent_contents canonical
                      ON canonical.info_hash = source_link.info_hash
                     AND canonical.content_type = source_link.content_type
                     AND canonical.content_source = 'tmdb'
                    WHERE source_link.content_type = $1
                      AND source_link.content_source = $2
                      AND source_link.content_id = $3
                    UNION ALL
                    SELECT canonical.type, canonical.source, canonical.id,
                           source_link.info_hash, 'exact_tmdb_id'::text
                    FROM content canonical
                    JOIN torrent_contents source_link
                      ON source_link.content_type = $1
                     AND source_link.content_source = $2
                     AND source_link.content_id = $3
                    WHERE canonical.type = $1
                      AND canonical.source = 'tmdb'
                      AND $4::text IS NOT NULL
                      AND canonical.id = $4
                    UNION ALL
                    SELECT attribute.content_type, attribute.content_source,
                           attribute.content_id, source_link.info_hash,
                           'exact_imdb_id'::text
                    FROM content_attributes attribute
                    JOIN torrent_contents source_link
                      ON source_link.content_type = $1
                     AND source_link.content_source = $2
                     AND source_link.content_id = $3
                    WHERE attribute.content_type = $1
                      AND attribute.content_source = 'tmdb'
                       AND (
                              (attribute.source = 'tmdb'
                               AND attribute.key = 'imdb_id')
                           OR (attribute.source = 'imdb'
                               AND attribute.key = 'id')
                       )
                      AND attribute.value = $3
                 ), candidates AS (
                    SELECT canonical_type, canonical_source, canonical_id,
                           (array_agg(evidence_info_hash ORDER BY evidence_info_hash))[1]
                               AS evidence_info_hash,
                           CASE
                               WHEN bool_or(evidence_kind = 'exact_tmdb_id')
                                   THEN 'exact_tmdb_id'
                               WHEN bool_or(evidence_kind = 'exact_imdb_id')
                                   THEN 'exact_imdb_id'
                               ELSE 'shared_info_hash'
                           END
                               AS evidence_kind
                    FROM raw_candidates
                    GROUP BY canonical_type, canonical_source, canonical_id
                 )
                 INSERT INTO crown_index.content_correlations (
                    content_type, content_source, content_id,
                    canonical_type, canonical_source, canonical_id,
                    evidence_info_hash, evidence_kind
                 )
                 SELECT $1, $2, $3, canonical_type, canonical_source,
                        canonical_id, evidence_info_hash, evidence_kind
                 FROM candidates
                 WHERE (SELECT count(*) FROM candidates) = 1
                 ON CONFLICT (content_type, content_source, content_id) DO NOTHING",
            )
            .bind(content_type)
            .bind(&source)
            .bind(&item.id)
            .bind(item.tmdb_id.as_deref())
            .execute(&mut *transaction)
            .await
            .context("failed to correlate Butter content with TMDB")?;
        }
        transaction
            .commit()
            .await
            .context("failed to commit Butter catalog transaction")?;
        Ok(())
    }

    #[expect(
        clippy::too_many_lines,
        reason = "the Bitmagnet association upsert must remain one auditable statement"
    )]
    pub(crate) async fn reconcile_pending_butter_links(&self) -> Result<u64> {
        let materialized: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1
                FROM crown_index.pending_butter_links pending
                JOIN torrents torrent ON torrent.info_hash = pending.info_hash
             )",
        )
        .fetch_one(&self.pool)
        .await
        .context("failed to inspect pending Butter associations")?;
        if !materialized {
            return Ok(0);
        }
        let mut transaction = self
            .pool
            .begin()
            .await
            .context("failed to begin Butter reconciliation transaction")?;
        let result = sqlx::query(
            "INSERT INTO torrent_contents (
                info_hash, content_type, content_source, content_id,
                languages, episodes, video_resolution,
                created_at, updated_at, seeders, leechers, published_at, size
             )
             SELECT pending.info_hash, pending.content_type,
                    pending.content_source, pending.content_id,
                    jsonb_build_object('en', true), pending.episodes,
                    pending.video_resolution, now(), now(),
                    pending.seeders, pending.leechers,
                    COALESCE(existing.published_at, '1999-01-01'::timestamptz),
                    torrent.size
             FROM crown_index.pending_butter_links pending
             JOIN torrents torrent ON torrent.info_hash = pending.info_hash
             LEFT JOIN LATERAL (
                SELECT tc.published_at
                FROM torrent_contents tc
                WHERE tc.info_hash = pending.info_hash
                ORDER BY tc.updated_at DESC
                LIMIT 1
             ) existing ON true
             ON CONFLICT (
                info_hash, content_type, content_source, content_id
             ) DO UPDATE SET
                episodes = CASE
                    WHEN torrent_contents.episodes IS NULL THEN EXCLUDED.episodes
                    WHEN EXCLUDED.episodes IS NULL THEN torrent_contents.episodes
                    ELSE (
                        SELECT jsonb_object_agg(
                            season,
                            COALESCE(
                                torrent_contents.episodes -> season,
                                '{}'::jsonb
                            ) || COALESCE(
                                EXCLUDED.episodes -> season,
                                '{}'::jsonb
                            )
                        )
                        FROM jsonb_object_keys(
                            torrent_contents.episodes || EXCLUDED.episodes
                        ) AS keys(season)
                    )
                END,
                video_resolution = COALESCE(
                    torrent_contents.video_resolution,
                    EXCLUDED.video_resolution
                ),
                seeders = GREATEST(
                    torrent_contents.seeders,
                    EXCLUDED.seeders
                ),
                leechers = GREATEST(
                    torrent_contents.leechers,
                    EXCLUDED.leechers
                ),
                updated_at = now()",
        )
        .execute(&mut *transaction)
        .await
        .context("failed to materialize pending Butter associations")?;
        correlate_pending_butter_links(&mut transaction).await?;
        sqlx::query(
            "DELETE FROM crown_index.pending_butter_links pending
             USING torrents torrent
             WHERE torrent.info_hash = pending.info_hash
               AND EXISTS (
                    SELECT 1
                    FROM torrent_contents linked
                    WHERE linked.info_hash = pending.info_hash
                      AND linked.content_type = pending.content_type
                      AND linked.content_source = pending.content_source
                      AND linked.content_id = pending.content_id
               )",
        )
        .execute(&mut *transaction)
        .await
        .context("failed to clear materialized Butter associations")?;
        transaction
            .commit()
            .await
            .context("failed to commit Butter reconciliation transaction")?;
        Ok(result.rows_affected())
    }

    pub(crate) async fn backfill_state(
        &self,
        indexer: &str,
        start_year: i32,
        min_year: i32,
    ) -> Result<BackfillState> {
        sqlx::query(
            "INSERT INTO crown_index.jackett_backfill_state (
                indexer, next_year, target_min_year
             ) VALUES ($1, $2, $3)
             ON CONFLICT (indexer) DO UPDATE SET
                target_min_year = LEAST(
                    crown_index.jackett_backfill_state.target_min_year,
                    EXCLUDED.target_min_year
                ),
                completed = CASE
                    WHEN EXCLUDED.target_min_year
                         < crown_index.jackett_backfill_state.target_min_year
                    THEN false
                    ELSE crown_index.jackett_backfill_state.completed
                END",
        )
        .bind(indexer)
        .bind(start_year)
        .bind(min_year)
        .execute(&self.pool)
        .await
        .context("failed to initialize Jackett backfill state")?;
        sqlx::query_as(
            "SELECT next_year, last_fingerprint, repeated_pages, completed
             FROM crown_index.jackett_backfill_state
             WHERE indexer = $1",
        )
        .bind(indexer)
        .fetch_one(&self.pool)
        .await
        .context("failed to read Jackett backfill state")
    }

    pub(crate) async fn advance_backfill(
        &self,
        indexer: &str,
        expected_year: i32,
        next_year: i32,
        fingerprint: Option<&[u8; 32]>,
        repeated_pages: i32,
        completed: bool,
    ) -> Result<()> {
        let result = sqlx::query(
            "UPDATE crown_index.jackett_backfill_state
             SET next_year = $3,
                 last_fingerprint = $4,
                 repeated_pages = $5,
                 completed = $6,
                 updated_at = now()
             WHERE indexer = $1 AND next_year = $2 AND completed = false",
        )
        .bind(indexer)
        .bind(expected_year)
        .bind(next_year)
        .bind(fingerprint.map(<[u8; 32]>::as_slice))
        .bind(repeated_pages)
        .bind(completed)
        .execute(&self.pool)
        .await
        .context("failed to advance Jackett backfill state")?;
        if result.rows_affected() != 1 {
            anyhow::bail!("Jackett backfill state changed concurrently");
        }
        Ok(())
    }

    pub(crate) async fn verify_schema(&self) -> Result<()> {
        let compatible: bool = sqlx::query_scalar(
            "SELECT to_regclass('public.torrent_contents') IS NOT NULL
                AND to_regclass('public.content') IS NOT NULL
                AND EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = 'torrent_contents'
                      AND column_name = 'video_resolution'
                )",
        )
        .fetch_one(&self.pool)
        .await
        .context("failed to inspect Bitmagnet schema")?;
        if !compatible {
            anyhow::bail!("database is not compatible with pinned Bitmagnet v0.10.0 schema");
        }
        Ok(())
    }

    pub(crate) async fn stats(&self) -> Result<IndexStats> {
        sqlx::query_as::<_, IndexStats>(
            "SELECT movie_count AS movies, show_count AS shows,
                    torrent_count AS torrents
             FROM crown_index.catalog_snapshot_generations
             WHERE active",
        )
        .fetch_one(&self.pool)
        .await
        .context("failed to read index statistics")
    }

    pub(crate) async fn pending_tmdb_enrichment(
        &self,
        limit: i64,
    ) -> Result<Vec<PendingEnrichment>> {
        sqlx::query_as::<_, PendingEnrichment>(
            "SELECT encode(t.info_hash, 'hex') AS info_hash,
                    t.name,
                    classified.content_type,
                    classified.external_id
             FROM crown_index.ingested_torrents i
             JOIN torrents t ON t.info_hash = i.info_hash
             LEFT JOIN LATERAL (
                SELECT tc.content_type,
                       CASE WHEN tc.content_id ~ '^tt[0-9]{5,}$'
                            THEN tc.content_id END AS external_id
                FROM torrent_contents tc
                WHERE tc.info_hash = i.info_hash
                ORDER BY (tc.content_id ~ '^tt[0-9]{5,}$') DESC,
                         (tc.content_type IN ('movie', 'tv_show')) DESC,
                         tc.updated_at DESC
                LIMIT 1
             ) classified ON true
             LEFT JOIN crown_index.tmdb_enrichment_attempts tea
                    ON tea.info_hash = i.info_hash
             WHERE (
                    tea.info_hash IS NULL
                 OR (
                        tea.status = 'error'
                    AND tea.attempted_at <= now() - make_interval(secs => $2)
                 )
               )
               AND t.size > 0
               AND (
                    classified.content_type IN ('movie', 'tv_show')
                 OR EXISTS (
                    SELECT 1 FROM torrent_contents classified_torrent
                    WHERE classified_torrent.info_hash = i.info_hash
                      AND classified_torrent.video_resolution IS NOT NULL
                 )
                 OR t.name ~* '\\m(2160p|1080p|720p|480p|web[-._ ]?dl|webrip|bluray|brrip|hdtv|s[0-9]{1,2}e[0-9]{1,3})\\M'
               )
             ORDER BY CASE
                    WHEN classified.external_id IS NOT NULL THEN 0
                    WHEN classified.content_type IN ('movie', 'tv_show') THEN 1
                    WHEN EXISTS (
                        SELECT 1 FROM torrent_contents classified_torrent
                        WHERE classified_torrent.info_hash = i.info_hash
                          AND classified_torrent.video_resolution IS NOT NULL
                    ) THEN 2
                    ELSE 2
                 END,
                 i.imported_at DESC
             LIMIT $1",
        )
        .bind(limit)
        .bind(TMDB_FAILURE_RETRY_SECONDS)
        .fetch_all(&self.pool)
        .await
        .context("failed to load pending TMDB enrichment items")
    }

    pub(crate) async fn tmdb_enrichment_by_hash(
        &self,
        info_hash: &str,
    ) -> Result<Option<PendingEnrichment>> {
        let info_hash = hex_to_bytes(info_hash)?;
        sqlx::query_as::<_, PendingEnrichment>(
            "SELECT encode(t.info_hash, 'hex') AS info_hash,
                    t.name,
                    classified.content_type,
                    classified.external_id
             FROM torrents t
             LEFT JOIN LATERAL (
                SELECT tc.content_type,
                       CASE WHEN tc.content_id ~ '^tt[0-9]{5,}$'
                            THEN tc.content_id END AS external_id
                FROM torrent_contents tc
                WHERE tc.info_hash = t.info_hash
                ORDER BY (tc.content_id ~ '^tt[0-9]{5,}$') DESC,
                         (tc.content_type IN ('movie', 'tv_show')) DESC,
                         tc.updated_at DESC
                LIMIT 1
             ) classified ON true
             WHERE t.info_hash = $1",
        )
        .bind(info_hash)
        .fetch_optional(&self.pool)
        .await
        .context("failed to load targeted TMDB enrichment item")
    }

    pub(crate) async fn materialized_info_hashes(
        &self,
        records: &[TorrentRecord],
    ) -> Result<HashSet<String>> {
        if records.is_empty() {
            return Ok(HashSet::new());
        }
        let hashes = records
            .iter()
            .map(|record| record.info_hash_bytes().map(|hash| hash.to_vec()))
            .collect::<Result<Vec<_>>>()?;
        let mut query = QueryBuilder::<Postgres>::new(
            "SELECT encode(info_hash, 'hex') FROM torrents WHERE false",
        );
        for hash in hashes {
            query.push(" OR info_hash = ").push_bind(hash);
        }
        query
            .build_query_scalar()
            .fetch_all(&self.pool)
            .await
            .map(|values| values.into_iter().collect())
            .context("failed to inspect Bitmagnet import materialization")
    }

    pub(crate) async fn record_tmdb_enrichment_failure(
        &self,
        info_hash: &str,
        torrent_name: &str,
        status: &str,
        error: &str,
    ) -> Result<()> {
        let info_hash = hex_to_bytes(info_hash)?;
        sqlx::query(
            "INSERT INTO crown_index.tmdb_enrichment_attempts (
                info_hash, torrent_name, status, error, attempted_at
             ) VALUES ($1, $2, $3, $4, now())
             ON CONFLICT (info_hash) DO UPDATE SET
                torrent_name = EXCLUDED.torrent_name,
                status = EXCLUDED.status,
                error = EXCLUDED.error,
                attempted_at = now()",
        )
        .bind(info_hash)
        .bind(torrent_name)
        .bind(status)
        .bind(error.chars().take(1_000).collect::<String>())
        .execute(&self.pool)
        .await
        .context("failed to persist TMDB enrichment failure")?;
        Ok(())
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the writes target Bitmagnet's normalized content schema and must remain auditable together"
    )]
    pub(crate) async fn persist_tmdb_match(
        &self,
        info_hash: &str,
        torrent_name: &str,
        parsed: &ParsedMedia,
        content: &TmdbContent,
    ) -> Result<()> {
        let info_hash_bytes = hex_to_bytes(info_hash)?;
        let content_type = match content.kind {
            ParsedKind::Movie => "movie",
            ParsedKind::Series => "tv_show",
        };
        let content_id = content.id.to_string();
        let mut transaction = self
            .pool
            .begin()
            .await
            .context("failed to begin TMDB enrichment transaction")?;
        sqlx::query(
            "INSERT INTO content (
                type, source, id, title, release_date, release_year, adult,
                original_language, original_title, overview, runtime, popularity,
                vote_average, vote_count, created_at, updated_at, tsv
             ) VALUES (
                $1, 'tmdb', $2, $3, $4::date, $5, $6,
                $7, $8, $9, $10, $11, $12, $13, now(), now(),
                to_tsvector('simple', $3)
             )
             ON CONFLICT (type, source, id) DO UPDATE SET
                title = EXCLUDED.title,
                release_date = EXCLUDED.release_date,
                release_year = EXCLUDED.release_year,
                adult = EXCLUDED.adult,
                original_language = EXCLUDED.original_language,
                original_title = EXCLUDED.original_title,
                overview = EXCLUDED.overview,
                runtime = EXCLUDED.runtime,
                popularity = EXCLUDED.popularity,
                vote_average = EXCLUDED.vote_average,
                vote_count = EXCLUDED.vote_count,
                updated_at = now(),
                tsv = EXCLUDED.tsv",
        )
        .bind(content_type)
        .bind(&content_id)
        .bind(&content.title)
        .bind(content.release_date.as_deref())
        .bind(content.release_year)
        .bind(content.adult)
        .bind(content.original_language.as_deref())
        .bind(content.original_title.as_deref())
        .bind(content.overview.as_deref())
        .bind(content.runtime)
        .bind(content.popularity)
        .bind(content.vote_average)
        .bind(content.vote_count)
        .execute(&mut *transaction)
        .await
        .context("failed to upsert TMDB content")?;

        for (key, value) in [
            ("poster_path", content.poster_path.as_deref()),
            ("backdrop_path", content.backdrop_path.as_deref()),
            ("imdb_id", content.imdb_id.as_deref()),
        ] {
            if let Some(value) = value {
                sqlx::query(
                    "INSERT INTO content_attributes (
                        content_type, content_source, content_id,
                        source, key, value, created_at, updated_at
                     ) VALUES ($1, 'tmdb', $2, 'tmdb', $3, $4, now(), now())
                     ON CONFLICT (
                        content_type, content_source, content_id, source, key
                     ) DO UPDATE SET value = EXCLUDED.value, updated_at = now()",
                )
                .bind(content_type)
                .bind(&content_id)
                .bind(key)
                .bind(value)
                .execute(&mut *transaction)
                .await
                .context("failed to upsert TMDB content attribute")?;
            }
        }

        for genre in &content.genres {
            let genre_id = genre.id.to_string();
            sqlx::query(
                "INSERT INTO content_collections (
                    type, source, id, name, created_at, updated_at
                 ) VALUES ('genre', 'tmdb', $1, $2, now(), now())
                 ON CONFLICT (type, source, id) DO UPDATE SET
                    name = EXCLUDED.name,
                    updated_at = now()",
            )
            .bind(&genre_id)
            .bind(&genre.name)
            .execute(&mut *transaction)
            .await
            .context("failed to upsert TMDB genre")?;
            sqlx::query(
                "INSERT INTO content_collections_content (
                    content_type, content_source, content_id,
                    content_collection_type, content_collection_source,
                    content_collection_id
                 ) VALUES ($1, 'tmdb', $2, 'genre', 'tmdb', $3)
                 ON CONFLICT DO NOTHING",
            )
            .bind(content_type)
            .bind(&content_id)
            .bind(&genre_id)
            .execute(&mut *transaction)
            .await
            .context("failed to attach TMDB genre")?;
        }

        let conflicting_alias: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                SELECT 1
                FROM content alias
                JOIN crown_index.content_correlations correlation
                  ON correlation.content_type = alias.type
                 AND correlation.content_source = alias.source
                 AND correlation.content_id = alias.id
                WHERE alias.type = $2
                  AND alias.source <> 'tmdb'
                  AND (
                    EXISTS (
                        SELECT 1 FROM torrent_contents tc
                        WHERE tc.info_hash = $1
                          AND tc.content_type = alias.type
                          AND tc.content_source = alias.source
                          AND tc.content_id = alias.id
                    )
                    OR ($4::text IS NOT NULL AND alias.id = $4)
                    OR EXISTS (
                        SELECT 1 FROM content_attributes attribute
                        WHERE attribute.content_type = alias.type
                          AND attribute.content_source = alias.source
                          AND attribute.content_id = alias.id
                          AND attribute.source = alias.source
                          AND attribute.key = 'tmdb_id'
                          AND attribute.value = $3
                    )
                  )
                  AND (
                       correlation.canonical_type <> $2
                    OR correlation.canonical_source <> 'tmdb'
                    OR correlation.canonical_id <> $3
                  )
             )",
        )
        .bind(&info_hash_bytes)
        .bind(content_type)
        .bind(&content_id)
        .bind(content.imdb_id.as_deref())
        .fetch_one(&mut *transaction)
        .await
        .context("failed to validate existing content correlations")?;
        if conflicting_alias {
            anyhow::bail!(
                "torrent metadata conflicts with an existing canonical content correlation"
            );
        }

        // Enrichment is additive: a TMDB association is copied from the best
        // existing classification without moving or deleting any source-owned
        // association. Repeated passes fill missing fields and retain maxima for
        // volatile swarm counts.
        sqlx::query(
            "INSERT INTO torrent_contents (
                info_hash, content_type, content_source, content_id,
                languages, episodes, video_resolution, video_source,
                video_codec, video_3d, video_modifier, release_group,
                created_at, updated_at, tsv, seeders, leechers,
                published_at, size, files_count
             )
             SELECT existing.info_hash, $2, 'tmdb', $3,
                    existing.languages, existing.episodes,
                    existing.video_resolution, existing.video_source,
                    existing.video_codec, existing.video_3d,
                    existing.video_modifier, existing.release_group,
                    now(), now(), existing.tsv, existing.seeders,
                    existing.leechers, existing.published_at,
                    existing.size, existing.files_count
             FROM torrent_contents existing
             WHERE existing.info_hash = $1
             ORDER BY (existing.content_source = 'tmdb') DESC,
                      (existing.content_source IS NOT NULL) DESC,
                      existing.updated_at DESC
             LIMIT 1
             ON CONFLICT (
                info_hash, content_type, content_source, content_id
             ) DO UPDATE SET
                languages = COALESCE(torrent_contents.languages, EXCLUDED.languages),
                episodes = COALESCE(torrent_contents.episodes, EXCLUDED.episodes),
                video_resolution = COALESCE(
                    torrent_contents.video_resolution,
                    EXCLUDED.video_resolution
                ),
                video_source = COALESCE(torrent_contents.video_source, EXCLUDED.video_source),
                video_codec = COALESCE(torrent_contents.video_codec, EXCLUDED.video_codec),
                video_3d = COALESCE(torrent_contents.video_3d, EXCLUDED.video_3d),
                video_modifier = COALESCE(
                    torrent_contents.video_modifier,
                    EXCLUDED.video_modifier
                ),
                release_group = COALESCE(
                    torrent_contents.release_group,
                    EXCLUDED.release_group
                ),
                tsv = COALESCE(torrent_contents.tsv, EXCLUDED.tsv),
                seeders = GREATEST(torrent_contents.seeders, EXCLUDED.seeders),
                leechers = GREATEST(torrent_contents.leechers, EXCLUDED.leechers),
                files_count = GREATEST(
                    torrent_contents.files_count,
                    EXCLUDED.files_count
                ),
                size = GREATEST(torrent_contents.size, EXCLUDED.size),
                updated_at = now()",
        )
        .bind(&info_hash_bytes)
        .bind(content_type)
        .bind(&content_id)
        .execute(&mut *transaction)
        .await
        .context("failed to attach torrent to TMDB content")?;
        sqlx::query(
            "INSERT INTO crown_index.content_correlations (
                content_type, content_source, content_id,
                canonical_type, canonical_source, canonical_id,
                evidence_info_hash, evidence_kind
             )
             SELECT alias.type, alias.source, alias.id,
                    $2, 'tmdb', $3, $1,
                    CASE
                        WHEN EXISTS (
                            SELECT 1 FROM content_attributes attribute
                            WHERE attribute.content_type = alias.type
                              AND attribute.content_source = alias.source
                              AND attribute.content_id = alias.id
                              AND attribute.source = alias.source
                              AND attribute.key = 'tmdb_id'
                              AND attribute.value = $3
                        ) THEN 'exact_tmdb_id'
                        WHEN $4::text IS NOT NULL AND alias.id = $4
                            THEN 'exact_imdb_id'
                        ELSE 'shared_info_hash'
                    END
             FROM content alias
             WHERE alias.type = $2
               AND alias.source <> 'tmdb'
               AND (
                    EXISTS (
                        SELECT 1 FROM torrent_contents tc
                        WHERE tc.info_hash = $1
                          AND tc.content_type = alias.type
                          AND tc.content_source = alias.source
                          AND tc.content_id = alias.id
                    )
                    OR ($4::text IS NOT NULL AND alias.id = $4)
                    OR EXISTS (
                        SELECT 1 FROM content_attributes attribute
                        WHERE attribute.content_type = alias.type
                          AND attribute.content_source = alias.source
                          AND attribute.content_id = alias.id
                          AND attribute.source = alias.source
                          AND attribute.key = 'tmdb_id'
                          AND attribute.value = $3
                    )
               )
             ON CONFLICT (content_type, content_source, content_id) DO UPDATE SET
                evidence_info_hash = EXCLUDED.evidence_info_hash,
                evidence_kind = EXCLUDED.evidence_kind,
                updated_at = now()
             WHERE crown_index.content_correlations.canonical_type = $2
               AND crown_index.content_correlations.canonical_source = 'tmdb'
               AND crown_index.content_correlations.canonical_id = $3",
        )
        .bind(&info_hash_bytes)
        .bind(content_type)
        .bind(&content_id)
        .bind(content.imdb_id.as_deref())
        .execute(&mut *transaction)
        .await
        .context("failed to correlate source content with TMDB")?;
        sqlx::query(
            "INSERT INTO crown_index.tmdb_enrichment_attempts (
                info_hash, torrent_name, parsed_title, parsed_kind, parsed_year,
                status, content_type, content_source, content_id,
                attempted_at, matched_at
             ) VALUES (
                $1, $2, $3, $4, $5, 'matched', $6, 'tmdb', $7, now(), now()
             )
             ON CONFLICT (info_hash) DO UPDATE SET
                torrent_name = EXCLUDED.torrent_name,
                parsed_title = EXCLUDED.parsed_title,
                parsed_kind = EXCLUDED.parsed_kind,
                parsed_year = EXCLUDED.parsed_year,
                status = 'matched',
                error = NULL,
                content_type = EXCLUDED.content_type,
                content_source = 'tmdb',
                content_id = EXCLUDED.content_id,
                attempted_at = now(),
                matched_at = now()",
        )
        .bind(&info_hash_bytes)
        .bind(torrent_name)
        .bind(&parsed.title)
        .bind(match parsed.kind {
            ParsedKind::Movie => "movie",
            ParsedKind::Series => "series",
        })
        .bind(parsed.year)
        .bind(content_type)
        .bind(&content_id)
        .execute(&mut *transaction)
        .await
        .context("failed to persist TMDB enrichment attempt")?;
        transaction
            .commit()
            .await
            .context("failed to commit TMDB enrichment transaction")?;
        Ok(())
    }

    pub(crate) async fn browse(&self, browse: &Browse) -> Result<Vec<CatalogRow>> {
        let content_type = match browse.kind {
            MediaKind::Movie => "movie",
            MediaKind::Series | MediaKind::Anime => "tv_show",
        };
        let mut query = QueryBuilder::<Postgres>::new(
            "WITH active AS (
                SELECT id
                FROM crown_index.catalog_snapshot_generations
                WHERE active
             ), selected AS (
                SELECT item.*, row_number() OVER (ORDER BY ",
        );
        query.push(order_clause(browse.sort));
        query.push(
            ") AS page_rank
                 FROM crown_index.catalog_snapshot_items item
                 JOIN active ON active.id = item.generation_id
                 WHERE item.content_type = ",
        );
        query.push_bind(content_type);
        match browse.kind {
            MediaKind::Anime => {
                query.push(
                    " AND item.original_language = 'ja'
                      AND 'animation' = ANY(item.normalized_genres)",
                );
            }
            MediaKind::Series => {
                query.push(
                    " AND NOT (
                        item.original_language = 'ja'
                        AND 'animation' = ANY(item.normalized_genres)
                      )",
                );
            }
            MediaKind::Movie => {}
        }
        if let Some(keywords) = browse.keywords.as_deref() {
            query.push(" AND item.tsv @@ websearch_to_tsquery('simple', ");
            query.push_bind(keywords);
            query.push(")");
        }
        if let Some(genre) = browse.genre.as_deref() {
            query.push(" AND lower(");
            query.push_bind(genre);
            query.push(") = ANY(item.normalized_genres)");
        }
        query.push(" ORDER BY ");
        query.push(order_clause(browse.sort));
        query.push(" LIMIT ");
        query.push_bind(PAGE_SIZE);
        query.push(" OFFSET ");
        query.push_bind(i64::from(browse.page.saturating_sub(1)) * PAGE_SIZE);
        query.push(
            ")
             SELECT selected.content_source, selected.content_id,
                    selected.title, selected.release_year, selected.overview,
                    selected.vote_average, selected.poster_path,
                    selected.backdrop_path, selected.genres,
                    encode(torrent.info_hash, 'hex') AS info_hash,
                    torrent.torrent_name, torrent.size,
                    torrent.video_resolution, torrent.seeders,
                    torrent.leechers, torrent.provider
             FROM selected
             JOIN crown_index.catalog_snapshot_torrents torrent
               ON torrent.generation_id = selected.generation_id
              AND torrent.content_type = selected.content_type
              AND torrent.content_source = selected.content_source
              AND torrent.content_id = selected.content_id
             ORDER BY selected.page_rank, torrent.seeders DESC NULLS LAST,
                      torrent.torrent_name",
        );
        query
            .build_query_as::<CatalogRow>()
            .fetch_all(&self.pool)
            .await
            .context("failed to browse indexed media")
    }

    pub(crate) async fn resolve_show(
        &self,
        source: Option<&str>,
        id: &str,
    ) -> Result<Option<ResolvedShow>> {
        sqlx::query_as::<_, ResolvedShow>(
            "WITH candidates AS (
                SELECT content.type, content.source, content.id, 0 AS priority
                FROM content
                WHERE content.type = 'tv_show'
                  AND $1::text IS NOT NULL
                  AND content.source = $1
                  AND content.id = $2
                UNION ALL
                SELECT content.type, content.source, content.id, 0 AS priority
                FROM content_attributes external
                JOIN content
                  ON content.type = external.content_type
                 AND content.source = external.content_source
                 AND content.id = external.content_id
                WHERE $1::text IS NULL
                  AND content.type = 'tv_show'
                  AND (
                         (external.source = 'tmdb'
                          AND external.key = 'imdb_id')
                      OR (external.source = 'imdb'
                          AND external.key = 'id')
                  )
                  AND external.value = $2
                UNION ALL
                SELECT content.type, content.source, content.id, 1 AS priority
                FROM content
                WHERE $1::text IS NULL
                  AND content.type = 'tv_show'
                  AND content.id = $2
             ), resolved AS (
                SELECT COALESCE(
                           correlation.canonical_source,
                           candidate.source
                       ) AS source,
                       COALESCE(
                           correlation.canonical_id,
                           candidate.id
                       ) AS id,
                       candidate.priority
                FROM candidates candidate
                LEFT JOIN crown_index.content_correlations correlation
                  ON correlation.content_type = candidate.type
                 AND correlation.content_source = candidate.source
                 AND correlation.content_id = candidate.id
             )
             SELECT resolved.source, resolved.id, content.title
             FROM resolved
             JOIN content
               ON content.type = 'tv_show'
              AND content.source = resolved.source
              AND content.id = resolved.id
             GROUP BY resolved.source, resolved.id, content.title
             ORDER BY min(resolved.priority),
                      (resolved.source = 'tmdb') DESC,
                      resolved.source, resolved.id
             LIMIT 1",
        )
        .bind(source)
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .context("failed to resolve show identity")
    }

    #[expect(
        clippy::too_many_lines,
        reason = "the correlated show-detail read remains one auditable SQL statement"
    )]
    pub(crate) async fn episode_torrents(
        &self,
        source: &str,
        id: &str,
    ) -> Result<Vec<EpisodeTorrentRow>> {
        sqlx::query_as::<_, EpisodeTorrentRow>(
            "WITH resolved_torrent_contents AS (
                SELECT tc.info_hash,
                       COALESCE(correlation.canonical_type, tc.content_type)
                           AS content_type,
                       COALESCE(correlation.canonical_source, tc.content_source)
                           AS content_source,
                       COALESCE(correlation.canonical_id, tc.content_id)
                           AS content_id,
                       (array_agg(tc.episodes ORDER BY
                            (tc.episodes IS NOT NULL) DESC, tc.updated_at DESC
                        ) FILTER (WHERE tc.episodes IS NOT NULL))[1] AS episodes,
                       (array_agg(tc.video_resolution ORDER BY
                            (tc.video_resolution IS NOT NULL) DESC, tc.updated_at DESC
                        ) FILTER (WHERE tc.video_resolution IS NOT NULL))[1]
                           AS video_resolution,
                       MAX(tc.seeders) AS seeders,
                       MAX(tc.leechers) AS leechers
                FROM torrent_contents tc
                LEFT JOIN crown_index.content_correlations correlation
                  ON correlation.content_type = tc.content_type
                 AND correlation.content_source = tc.content_source
                 AND correlation.content_id = tc.content_id
                 WHERE tc.content_type = 'tv_show'
                   AND tc.content_source IS NOT NULL
                   AND tc.content_id IS NOT NULL
                   AND (
                        (tc.content_source = $1 AND tc.content_id = $2)
                     OR (
                            correlation.canonical_type = 'tv_show'
                        AND correlation.canonical_source = $1
                        AND correlation.canonical_id = $2
                     )
                   )
                GROUP BY tc.info_hash,
                         COALESCE(correlation.canonical_type, tc.content_type),
                         COALESCE(correlation.canonical_source, tc.content_source),
                         COALESCE(correlation.canonical_id, tc.content_id)
             )
             , episode_file_seasons AS (
                SELECT file.info_hash,
                       COALESCE(correlation.canonical_type, file.content_type)
                           AS content_type,
                       COALESCE(correlation.canonical_source, file.content_source)
                           AS content_source,
                       COALESCE(correlation.canonical_id, file.content_id)
                           AS content_id,
                       file.season,
                       jsonb_object_agg(
                           file.episode::text, file.file_path
                           ORDER BY file.updated_at
                       ) AS episodes
                FROM crown_index.butter_episode_files file
                LEFT JOIN crown_index.content_correlations correlation
                  ON correlation.content_type = file.content_type
                 AND correlation.content_source = file.content_source
                 AND correlation.content_id = file.content_id
                GROUP BY file.info_hash,
                         COALESCE(correlation.canonical_type, file.content_type),
                         COALESCE(correlation.canonical_source, file.content_source),
                         COALESCE(correlation.canonical_id, file.content_id),
                         file.season
             ), episode_file_maps AS (
                SELECT info_hash, content_type, content_source, content_id,
                       jsonb_object_agg(season::text, episodes) AS episodes
                FROM episode_file_seasons
                GROUP BY info_hash, content_type, content_source, content_id
             )
             SELECT encode(tc.info_hash, 'hex') AS info_hash,
                     t.name AS torrent_name, t.size, tc.video_resolution,
                     tc.seeders, tc.leechers, tc.episodes,
                     episode_file_maps.episodes AS episode_files,
                    COALESCE((SELECT string_agg(DISTINCT ts.name, ', ' ORDER BY ts.name)
                     FROM torrents_torrent_sources tts
                     JOIN torrent_sources ts ON ts.key = tts.source
                     WHERE tts.info_hash = tc.info_hash), 'DHT') AS provider,
                    COALESCE(array_agg(tf.path ORDER BY tf.index)
                      FILTER (WHERE tf.path IS NOT NULL), ARRAY[]::text[]) AS files
             FROM content c
             JOIN resolved_torrent_contents tc
               ON tc.content_type = c.type
              AND tc.content_source = c.source
              AND tc.content_id = c.id
             JOIN torrents t ON t.info_hash = tc.info_hash
              LEFT JOIN torrent_files tf ON tf.info_hash = tc.info_hash
              LEFT JOIN episode_file_maps
                ON episode_file_maps.info_hash = tc.info_hash
               AND episode_file_maps.content_type = tc.content_type
               AND episode_file_maps.content_source = tc.content_source
               AND episode_file_maps.content_id = tc.content_id
             WHERE c.type = 'tv_show' AND c.source = $1 AND c.id = $2
             GROUP BY tc.info_hash, t.name, t.size, tc.video_resolution,
                       tc.seeders, tc.leechers, tc.episodes,
                       episode_file_maps.episodes
             ORDER BY tc.seeders DESC NULLS LAST, t.name",
        )
        .bind(source)
        .bind(id)
        .fetch_all(&self.pool)
        .await
        .context("failed to load show torrents")
    }
}

#[derive(Debug)]
struct ButterLink {
    info_hash: Vec<u8>,
    video_resolution: Option<String>,
    seeders: Option<i32>,
    leechers: Option<i32>,
    episodes: Option<Value>,
    episode_files: Vec<ButterEpisodeFile>,
}

#[derive(Debug)]
struct ButterEpisodeFile {
    season: u16,
    episode: u16,
    file_path: String,
}

fn grouped_butter_links(item: &ButterItem) -> Result<Vec<ButterLink>> {
    let mut links = HashMap::<String, ButterLink>::new();
    for torrent in &item.torrents {
        if !links.contains_key(&torrent.record.info_hash) {
            links.insert(
                torrent.record.info_hash.clone(),
                ButterLink {
                    info_hash: torrent.record.info_hash_bytes()?.to_vec(),
                    video_resolution: torrent.quality.as_deref().and_then(video_resolution),
                    seeders: torrent.seeders,
                    leechers: torrent.leechers,
                    episodes: None,
                    episode_files: Vec::new(),
                },
            );
        }
        let entry = links
            .get_mut(&torrent.record.info_hash)
            .context("Butter torrent grouping lost an inserted hash")?;
        entry.seeders = max_optional(entry.seeders, torrent.seeders);
        entry.leechers = max_optional(entry.leechers, torrent.leechers);
        if entry.video_resolution.is_none() {
            entry.video_resolution = torrent.quality.as_deref().and_then(video_resolution);
        }
        if let Some((season, episode)) = torrent.episode {
            add_episode(&mut entry.episodes, season, episode);
            if let Some(file_path) = torrent.file.as_deref() {
                entry.episode_files.push(ButterEpisodeFile {
                    season,
                    episode,
                    file_path: file_path.to_owned(),
                });
            }
        }
    }
    Ok(links.into_values().collect())
}

/// Records compact episode presence in Bitmagnet's compatibility field.
///
/// Exact source-provided paths live in `crown_index.butter_episode_files`.
/// Bitmagnet indexes the JSON value with a B-tree whose per-row limit is too
/// small for large season-pack filenames, so rich metadata must not be placed
/// in this third-party-owned column.
fn add_episode(value: &mut Option<Value>, season: u16, episode: u16) {
    let root = value.get_or_insert_with(|| Value::Object(serde_json::Map::new()));
    let Some(seasons) = root.as_object_mut() else {
        return;
    };
    let episodes = seasons
        .entry(season.to_string())
        .or_insert_with(|| Value::Object(serde_json::Map::new()));
    if let Some(episodes) = episodes.as_object_mut() {
        episodes.insert(episode.to_string(), Value::Bool(true));
    }
}

fn max_optional(left: Option<i32>, right: Option<i32>) -> Option<i32> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (left, right) => left.or(right),
    }
}

fn video_resolution(value: &str) -> Option<String> {
    let normalized = value.trim().to_ascii_lowercase();
    ["2160p", "1080p", "720p", "480p"]
        .into_iter()
        .find(|resolution| normalized.contains(resolution))
        .map(|resolution| format!("V{resolution}"))
}

fn normalized_genre_id(value: &str) -> String {
    value
        .chars()
        .filter_map(|character| {
            if character.is_ascii_alphanumeric() {
                Some(character.to_ascii_lowercase())
            } else if character.is_whitespace() || character == '-' {
                Some('-')
            } else {
                None
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_owned()
}

fn order_clause(sort: Sort) -> &'static str {
    match sort {
        Sort::Trending | Sort::Popularity => {
            "popularity DESC NULLS LAST, activity_at DESC, title, content_source, content_id"
        }
        Sort::Updated => {
            "activity_at DESC, popularity DESC NULLS LAST, title, content_source, content_id"
        }
        Sort::LastAdded => {
            "added_at DESC, popularity DESC NULLS LAST, title, content_source, content_id"
        }
        Sort::Year => {
            "release_year DESC NULLS LAST, popularity DESC NULLS LAST, title, content_source, content_id"
        }
        Sort::Title => "title, release_year DESC NULLS LAST, content_source, content_id",
        Sort::Rating => {
            "vote_average DESC NULLS LAST, popularity DESC NULLS LAST, title, content_source, content_id"
        }
    }
}

fn hex_to_bytes(value: &str) -> Result<Vec<u8>> {
    let bytes = value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let pair = std::str::from_utf8(pair).context("hex value is not ASCII")?;
            u8::from_str_radix(pair, 16).context("invalid hexadecimal value")
        })
        .collect::<Result<Vec<_>>>()?;
    if bytes.len() != 20 {
        anyhow::bail!("info hash is not 20 bytes");
    }
    Ok(bytes)
}

fn operator_sync_job_query(suffix: &str) -> String {
    format!(
        "SELECT id, query, requested_indexers, status, phase,
                fetched, imported, skipped, deferred, saturated_sources, matched, rejected,
                enrichment_pending, failed,
                error,
                EXTRACT(EPOCH FROM created_at)::bigint AS created_at,
                EXTRACT(EPOCH FROM started_at)::bigint AS started_at,
                EXTRACT(EPOCH FROM finished_at)::bigint AS finished_at
         FROM crown_index.operator_sync_jobs {suffix}"
    )
}

fn truncate_operator_error(error: &str) -> String {
    // Error rows are diagnostic summaries, not an unbounded upstream log sink.
    error.chars().take(1_000).collect()
}

#[cfg(test)]
mod tests;
