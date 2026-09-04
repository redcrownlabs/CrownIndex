//! Maintains the atomically published catalog read snapshot.

use anyhow::{Context, Result};
use sqlx::{FromRow, Postgres, Transaction};
use tracing::{Level, event};

use super::CatalogStore;

#[derive(Debug, Clone, Copy, FromRow)]
pub(crate) struct SnapshotSummary {
    pub(crate) generation: i64,
    pub(crate) movies: i64,
    pub(crate) shows: i64,
    pub(crate) torrents: i64,
}

pub(super) async fn ensure_schema(transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS crown_index.catalog_snapshot_generations (
            id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            active boolean NOT NULL DEFAULT false,
            created_at timestamptz NOT NULL DEFAULT now(),
            activated_at timestamptz NULL,
            movie_count bigint NOT NULL DEFAULT 0,
            show_count bigint NOT NULL DEFAULT 0,
            torrent_count bigint NOT NULL DEFAULT 0
         )",
    )
    .execute(&mut **transaction)
    .await
    .context("failed to create catalog snapshot generations")?;
    sqlx::query(
        "CREATE UNIQUE INDEX IF NOT EXISTS catalog_snapshot_one_active_idx
         ON crown_index.catalog_snapshot_generations (active)
         WHERE active",
    )
    .execute(&mut **transaction)
    .await
    .context("failed to constrain the active catalog snapshot")?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS crown_index.catalog_snapshot_torrents (
            generation_id bigint NOT NULL REFERENCES
                crown_index.catalog_snapshot_generations(id) ON DELETE CASCADE,
            content_type text NOT NULL,
            content_source text NOT NULL,
            content_id text NOT NULL,
            info_hash bytea NOT NULL CHECK (octet_length(info_hash) = 20),
            torrent_name text NOT NULL,
            size bigint NOT NULL,
            video_resolution text NULL,
            seeders integer NULL,
            leechers integer NULL,
            provider text NOT NULL,
            created_at timestamptz NOT NULL,
            updated_at timestamptz NOT NULL,
            PRIMARY KEY (
                generation_id, content_type, content_source, content_id, info_hash
            )
         )",
    )
    .execute(&mut **transaction)
    .await
    .context("failed to create catalog torrent snapshots")?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS catalog_snapshot_torrents_identity_idx
         ON crown_index.catalog_snapshot_torrents (
            generation_id, content_type, content_source, content_id
         )",
    )
    .execute(&mut **transaction)
    .await
    .context("failed to index catalog torrent snapshots")?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS crown_index.catalog_snapshot_items (
            generation_id bigint NOT NULL REFERENCES
                crown_index.catalog_snapshot_generations(id) ON DELETE CASCADE,
            content_type text NOT NULL,
            content_source text NOT NULL,
            content_id text NOT NULL,
            title text NOT NULL,
            release_year integer NULL,
            overview text NULL,
            vote_average double precision NULL,
            popularity double precision NULL,
            original_language text NULL,
            poster_path text NULL,
            backdrop_path text NULL,
            genres text[] NOT NULL,
            normalized_genres text[] NOT NULL,
            tsv tsvector NULL,
            activity_at timestamptz NOT NULL,
            added_at timestamptz NOT NULL,
            PRIMARY KEY (generation_id, content_type, content_source, content_id)
         )",
    )
    .execute(&mut **transaction)
    .await
    .context("failed to create catalog item snapshots")?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS catalog_snapshot_items_kind_idx
         ON crown_index.catalog_snapshot_items (generation_id, content_type)",
    )
    .execute(&mut **transaction)
    .await
    .context("failed to index catalog item kinds")?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS catalog_snapshot_items_search_idx
         ON crown_index.catalog_snapshot_items USING gin (tsv)",
    )
    .execute(&mut **transaction)
    .await
    .context("failed to index catalog item search")?;
    Ok(())
}

impl CatalogStore {
    pub(crate) async fn ensure_catalog_snapshot(&self) -> Result<SnapshotSummary> {
        self.build_catalog_snapshot(true).await
    }

    pub(crate) async fn refresh_catalog_snapshot(&self) -> Result<SnapshotSummary> {
        self.build_catalog_snapshot(false).await
    }

    async fn build_catalog_snapshot(&self, only_if_missing: bool) -> Result<SnapshotSummary> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .context("failed to begin catalog snapshot transaction")?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .execute(&mut *transaction)
            .await
            .context("failed to select catalog snapshot isolation")?;
        sqlx::query(
            "SELECT pg_advisory_xact_lock(
                hashtextextended('crown_index.catalog_snapshot', 0)
             )",
        )
        .execute(&mut *transaction)
        .await
        .context("failed to lock catalog snapshot generation")?;

        if only_if_missing && let Some(summary) = active_summary(&mut transaction).await? {
            transaction
                .commit()
                .await
                .context("failed to finish catalog snapshot inspection")?;
            return Ok(summary);
        }

        let generation = sqlx::query_scalar::<_, i64>(
            "INSERT INTO crown_index.catalog_snapshot_generations DEFAULT VALUES
             RETURNING id",
        )
        .fetch_one(&mut *transaction)
        .await
        .context("failed to allocate catalog snapshot generation")?;

        insert_torrents(&mut transaction, generation).await?;
        insert_items(&mut transaction, generation).await?;
        let summary = summarize(&mut transaction, generation).await?;
        sqlx::query(
            "UPDATE crown_index.catalog_snapshot_generations SET active = false
             WHERE active",
        )
        .execute(&mut *transaction)
        .await
        .context("failed to retire the previous catalog snapshot")?;
        sqlx::query(
            "UPDATE crown_index.catalog_snapshot_generations
             SET active = true, activated_at = now(), movie_count = $2,
                 show_count = $3, torrent_count = $4
             WHERE id = $1",
        )
        .bind(generation)
        .bind(summary.movies)
        .bind(summary.shows)
        .bind(summary.torrents)
        .execute(&mut *transaction)
        .await
        .context("failed to activate the catalog snapshot")?;
        transaction
            .commit()
            .await
            .context("failed to publish the catalog snapshot")?;

        if let Err(error) = self.remove_retired_snapshots().await {
            event!(
                name: "catalog.snapshot.cleanup.failed",
                Level::WARN,
                error.message = %error,
                "retired catalog snapshot cleanup failed"
            );
        }
        Ok(summary)
    }

    async fn remove_retired_snapshots(&self) -> Result<()> {
        sqlx::query(
            "DELETE FROM crown_index.catalog_snapshot_generations generation
             WHERE NOT generation.active
               AND generation.id NOT IN (
                    SELECT id
                    FROM crown_index.catalog_snapshot_generations
                    WHERE NOT active
                    ORDER BY id DESC
                    LIMIT 1
               )",
        )
        .execute(&self.pool)
        .await
        .context("failed to remove retired catalog snapshots")?;
        Ok(())
    }
}

async fn active_summary(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<Option<SnapshotSummary>> {
    sqlx::query_as::<_, SnapshotSummary>(
        "SELECT id AS generation, movie_count AS movies, show_count AS shows,
                torrent_count AS torrents
         FROM crown_index.catalog_snapshot_generations
         WHERE active",
    )
    .fetch_optional(&mut **transaction)
    .await
    .context("failed to inspect the active catalog snapshot")
}

async fn insert_torrents(
    transaction: &mut Transaction<'_, Postgres>,
    generation: i64,
) -> Result<()> {
    sqlx::query(
        "WITH resolved AS (
            SELECT tc.info_hash,
                   COALESCE(correlation.canonical_type, tc.content_type)
                       AS content_type,
                   COALESCE(correlation.canonical_source, tc.content_source)
                       AS content_source,
                   COALESCE(correlation.canonical_id, tc.content_id)
                       AS content_id,
                   (array_agg(tc.video_resolution ORDER BY
                        (tc.video_resolution IS NOT NULL) DESC, tc.updated_at DESC
                    ) FILTER (WHERE tc.video_resolution IS NOT NULL))[1]
                       AS video_resolution,
                   MAX(tc.seeders) AS seeders,
                   MAX(tc.leechers) AS leechers,
                   MIN(tc.created_at) AS created_at,
                   MAX(tc.updated_at) AS updated_at
            FROM torrent_contents tc
            LEFT JOIN crown_index.content_correlations correlation
              ON correlation.content_type = tc.content_type
             AND correlation.content_source = tc.content_source
             AND correlation.content_id = tc.content_id
            WHERE tc.content_type IN ('movie', 'tv_show')
              AND tc.content_source IS NOT NULL
              AND tc.content_id IS NOT NULL
            GROUP BY tc.info_hash,
                     COALESCE(correlation.canonical_type, tc.content_type),
                     COALESCE(correlation.canonical_source, tc.content_source),
                     COALESCE(correlation.canonical_id, tc.content_id)
         )
         INSERT INTO crown_index.catalog_snapshot_torrents (
            generation_id, content_type, content_source, content_id,
            info_hash, torrent_name, size, video_resolution, seeders, leechers,
            provider, created_at, updated_at
         )
         SELECT $1, resolved.content_type, resolved.content_source,
                resolved.content_id, resolved.info_hash, torrent.name,
                torrent.size, resolved.video_resolution, resolved.seeders,
                resolved.leechers,
                COALESCE(source_names.names, 'DHT'), resolved.created_at,
                resolved.updated_at
         FROM resolved
         JOIN content
           ON content.type = resolved.content_type
          AND content.source = resolved.content_source
          AND content.id = resolved.content_id
         JOIN torrents torrent ON torrent.info_hash = resolved.info_hash
         LEFT JOIN LATERAL (
            SELECT string_agg(DISTINCT source.name, ', ' ORDER BY source.name)
                AS names
            FROM torrents_torrent_sources link
            JOIN torrent_sources source ON source.key = link.source
            WHERE link.info_hash = resolved.info_hash
         ) source_names ON true
         WHERE content.adult IS NOT TRUE
           AND NOT EXISTS (
                SELECT 1
                FROM crown_index.content_correlations correlation
                WHERE correlation.content_type = content.type
                  AND correlation.content_source = content.source
                  AND correlation.content_id = content.id
           )",
    )
    .bind(generation)
    .execute(&mut **transaction)
    .await
    .context("failed to populate catalog torrent snapshot")?;
    Ok(())
}

async fn insert_items(transaction: &mut Transaction<'_, Postgres>, generation: i64) -> Result<()> {
    sqlx::query(
        "INSERT INTO crown_index.catalog_snapshot_items (
            generation_id, content_type, content_source, content_id, title,
            release_year, overview, vote_average, popularity,
            original_language, poster_path, backdrop_path, genres,
            normalized_genres, tsv, activity_at, added_at
         )
         SELECT $1, content.type, content.source, content.id, content.title,
                content.release_year, content.overview, content.vote_average,
                content.popularity, content.original_language,
                (SELECT attribute.value FROM content_attributes attribute
                 WHERE attribute.content_type = content.type
                   AND attribute.content_source = content.source
                   AND attribute.content_id = content.id
                   AND attribute.source = content.source
                   AND attribute.key = 'poster_path' LIMIT 1),
                (SELECT attribute.value FROM content_attributes attribute
                 WHERE attribute.content_type = content.type
                   AND attribute.content_source = content.source
                   AND attribute.content_id = content.id
                   AND attribute.source = content.source
                   AND attribute.key = 'backdrop_path' LIMIT 1),
                COALESCE((SELECT array_agg(collection.name ORDER BY collection.name)
                 FROM content_collections_content membership
                 JOIN content_collections collection
                   ON collection.type = membership.content_collection_type
                  AND collection.source = membership.content_collection_source
                  AND collection.id = membership.content_collection_id
                 WHERE membership.content_type = content.type
                   AND membership.content_source = content.source
                   AND membership.content_id = content.id
                   AND collection.type = 'genre'), ARRAY[]::text[]),
                COALESCE((SELECT array_agg(lower(collection.name) ORDER BY lower(collection.name))
                 FROM content_collections_content membership
                 JOIN content_collections collection
                   ON collection.type = membership.content_collection_type
                  AND collection.source = membership.content_collection_source
                  AND collection.id = membership.content_collection_id
                 WHERE membership.content_type = content.type
                   AND membership.content_source = content.source
                   AND membership.content_id = content.id
                   AND collection.type = 'genre'), ARRAY[]::text[]),
                content.tsv, MAX(snapshot.updated_at), MIN(snapshot.created_at)
         FROM content
         JOIN crown_index.catalog_snapshot_torrents snapshot
           ON snapshot.generation_id = $1
          AND snapshot.content_type = content.type
          AND snapshot.content_source = content.source
          AND snapshot.content_id = content.id
         GROUP BY content.type, content.source, content.id, content.title,
                  content.release_year, content.overview, content.vote_average,
                  content.popularity, content.original_language, content.tsv",
    )
    .bind(generation)
    .execute(&mut **transaction)
    .await
    .context("failed to populate catalog item snapshot")?;
    Ok(())
}

async fn summarize(
    transaction: &mut Transaction<'_, Postgres>,
    generation: i64,
) -> Result<SnapshotSummary> {
    sqlx::query_as::<_, SnapshotSummary>(
        "SELECT $1::bigint AS generation,
                COUNT(*) FILTER (WHERE content_type = 'movie')::bigint AS movies,
                COUNT(*) FILTER (WHERE content_type = 'tv_show')::bigint AS shows,
                (SELECT COUNT(DISTINCT info_hash)::bigint
                 FROM crown_index.catalog_snapshot_torrents
                 WHERE generation_id = $1) AS torrents
         FROM crown_index.catalog_snapshot_items
         WHERE generation_id = $1",
    )
    .bind(generation)
    .fetch_one(&mut **transaction)
    .await
    .context("failed to summarize catalog snapshot")
}
