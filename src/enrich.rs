//! Correlates imported torrents with TMDB content records.

use anyhow::Result;
use tracing::{Level, event};

use crate::media_match::{ParsedMedia, parse_torrent_name};
use crate::store::{CatalogStore, PendingEnrichment};
use crate::tmdb::Tmdb;

#[derive(Debug, Clone, Copy)]
pub(crate) struct Summary {
    pub(crate) scanned: usize,
    pub(crate) matched: usize,
    pub(crate) rejected: usize,
    pub(crate) failed: usize,
}

pub(crate) async fn run_once(store: &CatalogStore, tmdb: &Tmdb, limit: i64) -> Result<Summary> {
    tmdb.check_availability().await?;
    let pending = store.pending_tmdb_enrichment(limit).await?;
    let mut summary = Summary {
        scanned: pending.len(),
        matched: 0,
        rejected: 0,
        failed: 0,
    };
    for item in pending {
        match enrich_one(store, tmdb, &item).await {
            Ok(EnrichResult::Matched) => summary.matched += 1,
            Ok(EnrichResult::Rejected) => summary.rejected += 1,
            Err(error) => {
                summary.failed += 1;
                store
                    .record_tmdb_enrichment_failure(
                        &item.info_hash,
                        item.name.as_str(),
                        "error",
                        &error.to_string(),
                    )
                    .await?;
                event!(
                    name: "tmdb.enrichment.failed",
                    Level::WARN,
                    torrent.info_hash = item.info_hash,
                    error.message = %error,
                    "TMDB enrichment failed"
                );
            }
        }
    }
    Ok(summary)
}

pub(crate) async fn run_hash(
    store: &CatalogStore,
    tmdb: &Tmdb,
    info_hash: &str,
) -> Result<Summary> {
    tmdb.check_availability().await?;
    let Some(item) = store.tmdb_enrichment_by_hash(info_hash).await? else {
        return Ok(Summary {
            scanned: 0,
            matched: 0,
            rejected: 0,
            failed: 0,
        });
    };
    let result = enrich_one(store, tmdb, &item).await;
    match result {
        Ok(EnrichResult::Matched) => Ok(Summary {
            scanned: 1,
            matched: 1,
            rejected: 0,
            failed: 0,
        }),
        Ok(EnrichResult::Rejected) => Ok(Summary {
            scanned: 1,
            matched: 0,
            rejected: 1,
            failed: 0,
        }),
        Err(error) => {
            store
                .record_tmdb_enrichment_failure(
                    &item.info_hash,
                    item.name.as_str(),
                    "error",
                    &error.to_string(),
                )
                .await?;
            Err(error)
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum EnrichResult {
    Matched,
    Rejected,
}

async fn enrich_one(
    store: &CatalogStore,
    tmdb: &Tmdb,
    item: &PendingEnrichment,
) -> Result<EnrichResult> {
    if let Some(external_id) = item.external_id.as_deref()
        && let Some(content) = tmdb
            .match_imdb_id(external_id, item.content_type.as_deref())
            .await?
    {
        let candidate = ParsedMedia {
            kind: content.kind,
            title: content.title.clone(),
            year: content.release_year,
        };
        store
            .persist_tmdb_match(&item.info_hash, &item.name, &candidate, &content)
            .await?;
        return Ok(EnrichResult::Matched);
    }
    let Some(candidate) = parse_torrent_name(&item.name, item.content_type.as_deref()) else {
        store
            .record_tmdb_enrichment_failure(
                &item.info_hash,
                &item.name,
                "unparsed",
                "torrent name did not produce a stable media title",
            )
            .await?;
        return Ok(EnrichResult::Rejected);
    };
    let Some(content) = tmdb.match_media(&candidate).await? else {
        store
            .record_tmdb_enrichment_failure(
                &item.info_hash,
                &item.name,
                "no_match",
                "TMDB did not return a conservative title/year match",
            )
            .await?;
        return Ok(EnrichResult::Rejected);
    };
    store
        .persist_tmdb_match(&item.info_hash, &item.name, &candidate, &content)
        .await?;
    Ok(EnrichResult::Matched)
}
