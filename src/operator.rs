//! Runs durable, operator-requested Jackett searches through the normal pipeline.
//!
//! Jobs are stored before execution, claimed by one database-backed worker, and
//! remain inspectable after browser refreshes or process restarts. Imports keep
//! the same source/hash watermark and additive enrichment invariants as
//! periodic ingestion.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::watch;
use tokio::time::sleep;
use tracing::{Level, event};
use url::Url;

use crate::enrich;
use crate::importer;
use crate::record::TorrentRecord;
use crate::store::{CatalogStore, OperatorSyncJob};
use crate::tmdb::Tmdb;
use crate::torznab::Jackett;

/// Maximum results requested from each indexer for one targeted job.
///
/// Jackett and several adapters do not provide reliable pagination. A bounded
/// large page is more honest than pretending an offset walk is complete.
pub(crate) const TARGETED_RESULT_LIMIT: u16 = 1_000;
const IDLE_POLL_INTERVAL: Duration = Duration::from_secs(1);
const MATERIALIZATION_POLL_INTERVAL: Duration = Duration::from_secs(5);
const MATERIALIZATION_ATTEMPTS: usize = 6;
const MAX_IMMEDIATE_ENRICHMENTS: usize = 250;

#[derive(Debug, Default)]
struct Totals {
    fetched: i32,
    imported: i32,
    skipped: i32,
    deferred: i32,
    saturated_sources: i32,
    matched: i32,
    rejected: i32,
    enrichment_pending: i32,
    failed: i32,
    errors: Vec<String>,
}

pub(crate) async fn run_worker(
    store: CatalogStore,
    bitmagnet_url: Url,
    jackett: Jackett,
    tmdb: Option<Tmdb>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let resumed = store.requeue_interrupted_operator_syncs().await?;
    if resumed > 0 {
        event!(
            name: "operator.sync.requeued",
            Level::WARN,
            operator.sync.sources = resumed,
            "interrupted operator sync work requeued"
        );
    }
    loop {
        while let Some(job) = store.claim_operator_sync().await? {
            if let Err(error) =
                process_job(&store, &bitmagnet_url, &jackett, tmdb.as_ref(), &job).await
            {
                event!(
                    name: "operator.sync.failed",
                    Level::ERROR,
                    operator.sync.id = job.id,
                    error.message = %error,
                    "operator sync failed"
                );
                store
                    .finish_operator_sync(
                        job.id,
                        "failed",
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        1,
                        Some(&error.to_string()),
                    )
                    .await?;
            }
        }
        tokio::select! {
            () = sleep(IDLE_POLL_INTERVAL) => {}
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
        }
    }
}

async fn process_job(
    store: &CatalogStore,
    bitmagnet_url: &Url,
    jackett: &Jackett,
    tmdb: Option<&Tmdb>,
    job: &OperatorSyncJob,
) -> Result<()> {
    let mut totals = Totals::default();
    let mut source_failures = 0_i32;
    let mut enrichment_records = Vec::new();
    for indexer in &job.requested_indexers {
        store.operator_sync_source_started(job.id, indexer).await?;
        let page = match jackett
            .fetch_search(indexer, &job.query, TARGETED_RESULT_LIMIT)
            .await
        {
            Ok(page) => page,
            Err(error) => {
                source_failures = source_failures.saturating_add(1);
                totals.failed = totals.failed.saturating_add(1);
                totals.errors.push(format!("{indexer}: {error}"));
                store
                    .finish_operator_sync_source(job.id, indexer, 0, Some(&error.to_string()))
                    .await?;
                continue;
            }
        };
        let fetched = count(page.records.len());
        let mut skipped = count(page.skipped);
        let deferred = count(page.deferred);
        let saturated = page.records.len() >= usize::from(TARGETED_RESULT_LIMIT);
        if saturated {
            totals.saturated_sources = totals.saturated_sources.saturating_add(1);
        }
        enrichment_records.extend(page.records.clone());
        let unseen = store.unseen_records(&page.records).await?;
        skipped = skipped.saturating_add(fetched.saturating_sub(count(unseen.len())));
        totals.fetched = totals.fetched.saturating_add(fetched);
        totals.skipped = totals.skipped.saturating_add(skipped);
        totals.deferred = totals.deferred.saturating_add(deferred);
        store
            .operator_sync_source_importing(job.id, indexer, fetched, skipped, deferred, saturated)
            .await?;
        match importer::import_records(bitmagnet_url, &unseen).await {
            Ok(imported) => {
                store.mark_ingested(&unseen).await?;
                let imported = count(imported);
                totals.imported = totals.imported.saturating_add(imported);
                store
                    .finish_operator_sync_source(job.id, indexer, imported, None)
                    .await?;
            }
            Err(error) => {
                source_failures = source_failures.saturating_add(1);
                totals.failed = totals.failed.saturating_add(1);
                totals.errors.push(format!("{indexer}: {error}"));
                store
                    .finish_operator_sync_source(job.id, indexer, 0, Some(&error.to_string()))
                    .await?;
            }
        }
    }

    if !enrichment_records.is_empty() {
        store.set_operator_sync_phase(job.id, "enriching").await?;
        if let Some(tmdb) = tmdb {
            enrich_imports(store, tmdb, &enrichment_records, &mut totals).await?;
        } else {
            totals.enrichment_pending = count(unique_records(enrichment_records).len());
        }
        store.set_operator_sync_phase(job.id, "publishing").await?;
        store.refresh_catalog_snapshot().await?;
    }

    let status = terminal_status(source_failures, job.requested_indexers.len(), &totals);
    let error = (!totals.errors.is_empty()).then(|| totals.errors.join("; "));
    store
        .finish_operator_sync(
            job.id,
            status,
            totals.fetched,
            totals.imported,
            totals.skipped,
            totals.deferred,
            totals.saturated_sources,
            totals.matched,
            totals.rejected,
            totals.enrichment_pending,
            totals.failed,
            error.as_deref(),
        )
        .await?;
    event!(
        name: "operator.sync.completed",
        Level::INFO,
        operator.sync.id = job.id,
        operator.sync.status = status,
        operator.sync.fetched = totals.fetched,
        operator.sync.imported = totals.imported,
        operator.sync.enrichment_pending = totals.enrichment_pending,
        "operator sync completed"
    );
    Ok(())
}

async fn enrich_imports(
    store: &CatalogStore,
    tmdb: &Tmdb,
    records: &[TorrentRecord],
    totals: &mut Totals,
) -> Result<()> {
    let mut unique = unique_records(records.to_vec());
    if unique.len() > MAX_IMMEDIATE_ENRICHMENTS {
        totals.enrichment_pending = count(unique.len() - MAX_IMMEDIATE_ENRICHMENTS);
        unique.truncate(MAX_IMMEDIATE_ENRICHMENTS);
    }
    let mut pending = unique;
    for attempt in 0..MATERIALIZATION_ATTEMPTS {
        if pending.is_empty() {
            break;
        }
        if attempt > 0 {
            sleep(MATERIALIZATION_POLL_INTERVAL).await;
        }
        let materialized = store.materialized_info_hashes(&pending).await?;
        let mut remaining = Vec::new();
        for record in pending {
            if !materialized.contains(&record.info_hash) {
                remaining.push(record);
                continue;
            }
            match enrich::run_hash(store, tmdb, &record.info_hash).await {
                Ok(summary) => {
                    totals.matched = totals.matched.saturating_add(count(summary.matched));
                    totals.rejected = totals.rejected.saturating_add(count(summary.rejected));
                    totals.failed = totals.failed.saturating_add(count(summary.failed));
                }
                Err(error) => {
                    totals.failed = totals.failed.saturating_add(1);
                    totals
                        .errors
                        .push(format!("TMDB {}: {error}", record.info_hash));
                }
            }
        }
        pending = remaining;
    }
    totals.enrichment_pending = totals
        .enrichment_pending
        .saturating_add(count(pending.len()));
    Ok(())
}

fn unique_records(records: Vec<TorrentRecord>) -> Vec<TorrentRecord> {
    let mut by_hash = HashMap::new();
    for record in records {
        by_hash.entry(record.info_hash.clone()).or_insert(record);
    }
    by_hash.into_values().collect()
}

fn count(value: usize) -> i32 {
    i32::try_from(value).unwrap_or(i32::MAX)
}

fn terminal_status(source_failures: i32, source_count: usize, totals: &Totals) -> &'static str {
    if source_failures >= count(source_count) {
        "failed"
    } else if totals.failed > 0 || totals.deferred > 0 || totals.saturated_sources > 0 {
        "partial"
    } else {
        "completed"
    }
}

#[cfg(test)]
mod tests {
    use super::{TARGETED_RESULT_LIMIT, count, unique_records};
    use crate::record::TorrentRecord;

    #[test]
    fn targeted_search_is_explicitly_bounded() {
        assert_eq!(TARGETED_RESULT_LIMIT, 1_000);
    }

    #[test]
    fn enrichment_collapses_source_duplicates_by_hash() {
        let hash = "0123456789abcdef0123456789abcdef01234567";
        let records = vec![
            TorrentRecord::new("jackett-a", hash, "Dark Matter S02", 10).expect("record"),
            TorrentRecord::new("jackett-b", hash, "Dark Matter S02", 10).expect("record"),
        ];
        assert_eq!(unique_records(records).len(), 1);
    }

    #[test]
    fn counters_saturate_at_database_integer_capacity() {
        assert_eq!(count(usize::MAX), i32::MAX);
    }
}
