//! Runs the `CrownIndex` API and authorized import commands.

mod api;
mod backfill;
mod butter;
mod config;
mod enrich;
mod ext;
mod importer;
mod media_match;
mod model;
mod record;
mod store;
mod tmdb;
mod torrent;
mod torznab;

use anyhow::{Context, Result};
use clap::Parser;
use config::{Cli, Command, Config};
use mimalloc::MiMalloc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::time::{Instant, MissedTickBehavior, interval, interval_at};
use tracing::{Level, event};
use tracing_subscriber::EnvFilter;

use crate::store::CatalogStore;
use crate::tmdb::Tmdb;
use crate::torznab::Jackett;

/// Retry interval for Bitmagnet's asynchronous import materialization.
///
/// Bitmagnet acknowledges imports before every torrent is queryable. A short
/// interval keeps associations responsive without polling `PostgreSQL` in a
/// tight loop.
const BUTTER_RECONCILIATION_INTERVAL: Duration = Duration::from_secs(5);
/// Maximum age of the catalog read snapshot while the API is running.
///
/// Five minutes keeps external DHT and importer changes visible without
/// repeatedly aggregating the full compatibility schema for every request.
const CATALOG_SNAPSHOT_INTERVAL: Duration = Duration::from_secs(5 * 60);

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("crown_index=info")),
        )
        .try_init()
        .map_err(|error| anyhow::anyhow!("failed to initialize tracing: {error}"))?;

    let cli = Cli::parse();
    let config = Config::from_env()?;
    run_command(cli.command.unwrap_or(Command::Serve), &config).await
}

async fn run_command(command: Command, config: &Config) -> Result<()> {
    match command {
        Command::Serve => serve(config).await,
        Command::ImportExtSnapshots { directory } => {
            let records = ext::load_snapshots(&directory).await?;
            let count = importer::import_records(&config.bitmagnet_url, &records).await?;
            event!(Level::INFO, import.count = count, "EXT snapshots imported");
            Ok(())
        }
        Command::ImportExtLive { pages } => {
            let records = ext::crawl_live(&config.ext_url, &config.ext_user_agent, pages).await?;
            let count = importer::import_records(&config.bitmagnet_url, &records).await?;
            event!(
                Level::INFO,
                import.count = count,
                "EXT live records imported"
            );
            Ok(())
        }
        Command::ImportJackett => {
            let jackett = config
                .jackett
                .clone()
                .context("JACKETT_API_KEY is required for Jackett imports")?;
            let store = CatalogStore::connect(&config.database_url).await?;
            let summary = run_jackett_import(
                &store,
                &config.bitmagnet_url,
                &Jackett::new(jackett.clone(), store.clone())?,
            )
            .await?;
            if summary.failures == jackett.indexers.len() {
                anyhow::bail!("all configured Jackett indexers failed");
            }
            log_import_summary(&summary);
            Ok(())
        }
        Command::BackfillButter { max_pages } => {
            let butter = config
                .butter
                .clone()
                .context("CROWN_INDEX_BUTTER_URLS is required for Butter backfills")?;
            let store = CatalogStore::connect(&config.database_url).await?;
            butter::backfill(&store, &config.bitmagnet_url, butter, max_pages).await
        }
        Command::BackfillJackett {
            indexers,
            min_year,
            start_year,
            result_limit,
            delay_seconds,
            max_partitions,
        } => {
            let jackett_config = config
                .jackett
                .clone()
                .context("JACKETT_API_KEY is required for Jackett backfills")?;
            let selected = if indexers.is_empty() {
                jackett_config.indexers.clone()
            } else {
                for indexer in &indexers {
                    if !jackett_config.indexers.contains(indexer) {
                        anyhow::bail!("backfill indexer is not configured: {indexer}");
                    }
                }
                indexers
            };
            let store = CatalogStore::connect(&config.database_url).await?;
            let options = backfill::Options {
                indexers: selected,
                start_year: match start_year {
                    Some(year) => year,
                    None => store.current_year().await?,
                },
                min_year,
                result_limit,
                delay: Duration::from_secs(delay_seconds),
                max_partitions,
            };
            let jackett = Jackett::new(jackett_config, store.clone())?;
            backfill::run(&store, &config.bitmagnet_url, &jackett, &options).await
        }
        Command::EnrichTmdb { limit, info_hash } => {
            run_tmdb_command(config, limit, info_hash.as_deref()).await
        }
    }
}

async fn run_tmdb_command(config: &Config, limit: i64, info_hash: Option<&str>) -> Result<()> {
    let tmdb_config = config
        .tmdb
        .clone()
        .context("TMDB_API_KEY is required for TMDB enrichment")?;
    if !tmdb_config.enabled {
        anyhow::bail!("TMDB_ENABLED=true is required for TMDB enrichment");
    }
    let store = CatalogStore::connect(&config.database_url).await?;
    let tmdb = Tmdb::new(tmdb_config)?;
    let summary = if let Some(info_hash) = info_hash {
        enrich::run_hash(&store, &tmdb, info_hash).await?
    } else {
        enrich::run_once(&store, &tmdb, limit).await?
    };
    log_tmdb_summary(&summary);
    Ok(())
}

async fn serve(config: &Config) -> Result<()> {
    let store = CatalogStore::connect(&config.database_url).await?;
    let listener = TcpListener::bind(config.listen)
        .await
        .with_context(|| format!("failed to bind API to {}", config.listen))?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let worker = config.jackett.clone().map(|jackett| {
        tokio::spawn(run_jackett_worker(
            store.clone(),
            config.bitmagnet_url.clone(),
            jackett,
            shutdown_rx.clone(),
        ))
    });
    let tmdb_worker = config
        .tmdb
        .clone()
        .filter(|tmdb| tmdb.enabled)
        .map(|tmdb| tokio::spawn(run_tmdb_worker(store.clone(), tmdb, shutdown_rx.clone())));
    let reconciliation_worker = tokio::spawn(run_butter_reconciliation_worker(
        store.clone(),
        shutdown_rx.clone(),
    ));
    let snapshot_worker = tokio::spawn(run_catalog_snapshot_worker(
        store.clone(),
        shutdown_rx.clone(),
    ));
    let signal = tokio::spawn(signal_shutdown(shutdown_tx.clone()));
    event!(Level::INFO, server.address = %config.listen, "CrownIndex API listening: {{server.address}}");
    let server_result = axum::serve(listener, api::router(store))
        .with_graceful_shutdown(wait_for_shutdown(shutdown_rx))
        .await
        .context("HTTP server failed");
    let _ = shutdown_tx.send(true);
    signal.abort();
    if let Some(worker) = worker {
        worker.await.context("Jackett worker task failed")??;
    }
    if let Some(worker) = tmdb_worker {
        worker.await.context("TMDB worker task failed")??;
    }
    reconciliation_worker
        .await
        .context("Butter reconciliation worker task failed")??;
    snapshot_worker
        .await
        .context("catalog snapshot worker task failed")??;
    server_result
}

async fn signal_shutdown(shutdown: watch::Sender<bool>) {
    if let Err(error) = tokio::signal::ctrl_c().await {
        event!(
            name: "process.signal.failed",
            Level::ERROR,
            error.message = %error,
            "failed to install shutdown signal handler: {{error.message}}"
        );
    }
    let _ = shutdown.send(true);
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    while !*shutdown.borrow() && shutdown.changed().await.is_ok() {}
}

#[derive(Debug)]
struct ImportSummary {
    fetched: usize,
    imported: usize,
    skipped: usize,
    failures: usize,
}

async fn run_jackett_import(
    store: &CatalogStore,
    bitmagnet_url: &url::Url,
    jackett: &Jackett,
) -> Result<ImportSummary> {
    let batch = jackett.fetch_recent().await;
    for failure in &batch.failures {
        event!(
            name: "jackett.indexer.failed",
            Level::WARN,
            jackett.indexer.id = failure.indexer,
            error.message = failure.message,
            "Jackett indexer failed"
        );
    }
    let fetched = batch.records.len();
    let unseen = store.unseen_records(&batch.records).await?;
    let imported = importer::import_records(bitmagnet_url, &unseen).await?;
    store.mark_ingested(&unseen).await?;
    Ok(ImportSummary {
        fetched,
        imported,
        skipped: batch.skipped,
        failures: batch.failures.len(),
    })
}

async fn run_jackett_worker(
    store: CatalogStore,
    bitmagnet_url: url::Url,
    config: config::JackettConfig,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let poll_interval = config.poll_interval;
    let jackett = Jackett::new(config, store.clone())?;
    let mut ticks = interval(poll_interval);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = ticks.tick() => match run_jackett_import(&store, &bitmagnet_url, &jackett).await {
                Ok(summary) => log_import_summary(&summary),
                Err(error) => event!(
                    name: "jackett.poll.failed",
                    Level::ERROR,
                    error.message = %error,
                    "Jackett poll failed"
                ),
            },
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
        }
    }
}

fn log_import_summary(summary: &ImportSummary) {
    event!(
        name: "jackett.poll.completed",
        Level::INFO,
        jackett.records.fetched = summary.fetched,
        jackett.records.imported = summary.imported,
        jackett.records.skipped = summary.skipped,
        jackett.indexers.failed = summary.failures,
        "Jackett poll completed"
    );
}

async fn run_tmdb_worker(
    store: CatalogStore,
    config: config::TmdbConfig,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let poll_interval = config.poll_interval;
    let batch_size = config.batch_size;
    let tmdb = Tmdb::new(config)?;
    let mut ticks = interval(poll_interval);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = ticks.tick() => match enrich::run_once(&store, &tmdb, batch_size).await {
                Ok(summary) => log_tmdb_summary(&summary),
                Err(error) => event!(
                    name: "tmdb.worker.failed",
                    Level::ERROR,
                    error.message = %error,
                    "TMDB enrichment worker failed"
                ),
            },
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
        }
    }
}

fn log_tmdb_summary(summary: &enrich::Summary) {
    event!(
        name: "tmdb.enrichment.completed",
        Level::INFO,
        tmdb.items.scanned = summary.scanned,
        tmdb.items.matched = summary.matched,
        tmdb.items.rejected = summary.rejected,
        tmdb.items.failed = summary.failed,
        "TMDB enrichment completed"
    );
}

async fn run_butter_reconciliation_worker(
    store: CatalogStore,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut ticks = interval(BUTTER_RECONCILIATION_INTERVAL);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = ticks.tick() => match store.reconcile_pending_butter_links().await {
                Ok(0) => {}
                Ok(reconciled) => event!(
                    name: "butter.reconciliation.completed",
                    Level::INFO,
                    butter.torrents.reconciled = reconciled,
                    "pending Butter associations reconciled"
                ),
                Err(error) => event!(
                    name: "butter.reconciliation.failed",
                    Level::ERROR,
                    error.message = %error,
                    "Butter association reconciliation failed"
                ),
            },
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
        }
    }
}

async fn run_catalog_snapshot_worker(
    store: CatalogStore,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let first_refresh = Instant::now() + CATALOG_SNAPSHOT_INTERVAL;
    let mut ticks = interval_at(first_refresh, CATALOG_SNAPSHOT_INTERVAL);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = ticks.tick() => match store.refresh_catalog_snapshot().await {
                Ok(summary) => event!(
                    name: "catalog.snapshot.completed",
                    Level::INFO,
                    catalog.snapshot.generation = summary.generation,
                    catalog.movies = summary.movies,
                    catalog.shows = summary.shows,
                    catalog.torrents = summary.torrents,
                    "catalog snapshot refreshed"
                ),
                Err(error) => event!(
                    name: "catalog.snapshot.failed",
                    Level::ERROR,
                    error.message = %error,
                    "catalog snapshot refresh failed"
                ),
            },
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
        }
    }
}
