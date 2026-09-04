//! Runs resumable year-partitioned Jackett history imports.

use std::time::Duration;

use anyhow::Result;
use tokio::time::sleep;
use tracing::{Level, event};
use url::Url;

use crate::importer;
use crate::store::{BackfillState, CatalogStore};
use crate::torznab::{FetchPage, Jackett};

// Two identical non-empty year partitions indicate that an indexer ignored
// the query. Requiring two repeats avoids stopping on one accidental overlap.
const MAX_REPEATED_PAGES: i32 = 2;

#[derive(Debug, Clone)]
pub(crate) struct Options {
    pub(crate) indexers: Vec<String>,
    pub(crate) start_year: i32,
    pub(crate) min_year: i32,
    pub(crate) result_limit: u16,
    pub(crate) delay: Duration,
    pub(crate) max_partitions: Option<u32>,
}

pub(crate) async fn run(
    store: &CatalogStore,
    bitmagnet_url: &Url,
    jackett: &Jackett,
    options: &Options,
) -> Result<()> {
    validate_options(options)?;
    let mut attempts = 0_u32;
    loop {
        let mut active = false;
        for indexer in &options.indexers {
            let state = store
                .backfill_state(indexer, options.start_year, options.min_year)
                .await?;
            if state.completed {
                continue;
            }
            active = true;
            attempts = attempts.saturating_add(1);
            match process_partition(store, bitmagnet_url, jackett, indexer, options, &state).await {
                Ok(summary) => log_partition(indexer, &summary),
                Err(error) => event!(
                    name: "jackett.backfill.partition.failed",
                    Level::WARN,
                    jackett.indexer.id = indexer,
                    jackett.backfill.year = state.next_year,
                    error.message = %error,
                    "Jackett backfill partition failed"
                ),
            }
            if options
                .max_partitions
                .is_some_and(|maximum| attempts >= maximum)
            {
                return Ok(());
            }
            sleep(options.delay).await;
        }
        if !active {
            return Ok(());
        }
    }
}

#[derive(Debug)]
struct PartitionSummary {
    year: i32,
    fetched: usize,
    imported: usize,
    skipped: usize,
    deferred: usize,
    repeated_pages: i32,
    completed: bool,
    checkpointed: bool,
    saturated: bool,
}

async fn process_partition(
    store: &CatalogStore,
    bitmagnet_url: &Url,
    jackett: &Jackett,
    indexer: &str,
    options: &Options,
    state: &BackfillState,
) -> Result<PartitionSummary> {
    let page = jackett
        .fetch_year(indexer, state.next_year, options.result_limit)
        .await?;
    let fetched = page.records.len();
    let unseen = store.unseen_records(&page.records).await?;
    let imported = importer::import_records(bitmagnet_url, &unseen).await?;
    store.mark_ingested(&unseen).await?;
    let saturated = fetched >= usize::from(options.result_limit);
    let repeated_pages = repeated_pages(state, &page);
    let completed = state.next_year <= options.min_year || repeated_pages >= MAX_REPEATED_PAGES;
    let checkpointed = page.deferred == 0;
    if checkpointed {
        store
            .advance_backfill(
                indexer,
                state.next_year,
                state.next_year.saturating_sub(1),
                (!page.records.is_empty()).then_some(&page.fingerprint),
                repeated_pages,
                completed,
            )
            .await?;
    }
    Ok(PartitionSummary {
        year: state.next_year,
        fetched,
        imported,
        skipped: page.skipped,
        deferred: page.deferred,
        repeated_pages,
        completed,
        checkpointed,
        saturated,
    })
}

fn repeated_pages(state: &BackfillState, page: &FetchPage) -> i32 {
    if page.records.is_empty() {
        return 0;
    }
    if state.last_fingerprint.as_deref() == Some(page.fingerprint.as_slice()) {
        state.repeated_pages.saturating_add(1)
    } else {
        0
    }
}

fn log_partition(indexer: &str, summary: &PartitionSummary) {
    event!(
        name: "jackett.backfill.partition.completed",
        Level::INFO,
        jackett.indexer.id = indexer,
        jackett.backfill.year = summary.year,
        jackett.records.fetched = summary.fetched,
        jackett.records.imported = summary.imported,
        jackett.records.skipped = summary.skipped,
        jackett.records.deferred = summary.deferred,
        jackett.backfill.repeated_pages = summary.repeated_pages,
        jackett.backfill.checkpointed = summary.checkpointed,
        jackett.backfill.completed = summary.completed,
        jackett.backfill.saturated = summary.saturated,
        "Jackett backfill partition completed"
    );
}

fn validate_options(options: &Options) -> Result<()> {
    if options.indexers.is_empty() {
        anyhow::bail!("Jackett backfill requires at least one indexer");
    }
    if !(1_800..=2_100).contains(&options.min_year)
        || !(1_800..=2_100).contains(&options.start_year)
    {
        anyhow::bail!("Jackett backfill years must be between 1800 and 2100");
    }
    if options.min_year > options.start_year {
        anyhow::bail!("Jackett backfill minimum year cannot exceed its start year");
    }
    if options.result_limit == 0 {
        anyhow::bail!("Jackett backfill result limit must be positive");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Options, repeated_pages, validate_options};
    use crate::store::BackfillState;
    use crate::torznab::FetchPage;
    use std::time::Duration;

    fn options() -> Options {
        Options {
            indexers: vec!["yts".to_owned()],
            start_year: 2026,
            min_year: 1900,
            result_limit: 1000,
            delay: Duration::from_secs(1),
            max_partitions: None,
        }
    }

    #[test]
    fn validates_year_range() {
        assert!(validate_options(&options()).is_ok());
        let mut invalid = options();
        invalid.min_year = 2027;
        assert!(validate_options(&invalid).is_err());
    }

    #[test]
    fn detects_repeated_non_empty_pages() {
        let fingerprint = [7_u8; 32];
        let state = BackfillState {
            next_year: 2025,
            last_fingerprint: Some(fingerprint.to_vec()),
            repeated_pages: 1,
            completed: false,
        };
        let page = FetchPage {
            records: vec![
                crate::record::TorrentRecord::new(
                    "jackett-yts",
                    "0123456789abcdef0123456789abcdef01234567",
                    "Example 2025",
                    42,
                )
                .expect("valid record"),
            ],
            skipped: 0,
            deferred: 0,
            fingerprint,
        };
        assert_eq!(repeated_pages(&state, &page), 2);
    }
}
