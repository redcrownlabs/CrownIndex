# CrownIndex

[![CI](https://github.com/redcrownlabs/CrownIndex/actions/workflows/ci.yml/badge.svg)](https://github.com/redcrownlabs/CrownIndex/actions/workflows/ci.yml)

CrownIndex is a local BitTorrent metadata index and compatibility API. It uses
[Bitmagnet](https://github.com/bitmagnet-io/bitmagnet) for DHT discovery,
metadata verification, classification, and PostgreSQL storage. A small Rust
adapter exposes the media-oriented API expected by RedCrown and provides
guarded EXT HTML and Jackett/Torznab ingestion.

The project indexes metadata; it does not download payload files. DHT coverage
is partial and grows over time. Operators are responsible for lawful use and
for complying with source-site terms and robots policies.

## Start locally

```powershell
Copy-Item .env.example .env
# Optional: set TMDB_API_KEY and TMDB_ENABLED=true in .env
docker compose up --build -d
```

- RedCrown-compatible API: `http://127.0.0.1:8080/`
- Bitmagnet UI and GraphQL: `http://127.0.0.1:3333/`
- Jackett indexer configuration: `http://127.0.0.1:9117/`
- FlareSolverr status: `http://127.0.0.1:8191/`
- DHT: TCP/UDP port `3334`

If another local application owns port 8080, set
`CROWN_INDEX_HOST_PORT=8081` (or another free loopback port) in `.env` and use
that same URL in RedCrown. The container's internal API remains on port 8080.

Configure `http://127.0.0.1:8080/` as a RedCrown catalog URL. Without a TMDB
key, Bitmagnet still indexes generic torrents, but the Butter movie/show routes
only return records that were confidently classified as media.

## TMDB enrichment

Set `TMDB_API_KEY` and `TMDB_ENABLED=true` in `.env` to enable cover, rating,
genre, and movie/show correlation. CrownIndex parses imported torrent names,
searches TMDB, verifies title and year conservatively, writes Bitmagnet
content rows, and links the torrent to the matched TMDB title. Rejected names
are recorded so the worker does not repeatedly query TMDB for the same
non-media or ambiguous torrent. Butter items with an IMDb ID use TMDB's exact
external-ID lookup instead of filename matching, so season packs merge into the
same canonical series without overwriting source metadata. When a fallback API
supplies its TMDB ID directly, CrownIndex uses that stronger identity during
ingestion and does not wait for a shared torrent hash or enrichment pass.
Both a TMDB v3 API key and a v4 read token are accepted. CrownIndex keeps a v4
token in an Authorization header and never forwards either credential to
Bitmagnet.

The API service runs enrichment at startup and then every 15 minutes by
default. It verifies TMDB availability once per batch and retries transient
transport failures after six hours; rejected title matches remain terminal.
Backfill existing imports manually with:

```powershell
docker compose run --rm api enrich-tmdb --limit 500
```

Repair or verify one known torrent immediately with:

```powershell
docker compose run --rm api enrich-tmdb --info-hash INFO_HASH
```

Tune `CROWN_INDEX_TMDB_BATCH_SIZE`, `CROWN_INDEX_TMDB_LANGUAGE`,
`CROWN_INDEX_TMDB_POLL_SECONDS`, and `CROWN_INDEX_TMDB_TIMEOUT_SECONDS` in
`.env` when needed.

## Jackett indexers

Jackett owns tracker-specific scraping and authentication. CrownIndex polls
each configured indexer through Torznab, imports validated observations through
Bitmagnet's `/import` endpoint, and remembers `(source, info hash)` pairs so a
release is not imported repeatedly.

After the first `docker compose up`:

1. Open `http://127.0.0.1:9117/`.
2. Add and test these indexers: `1337x`, `AudioBookBay`, `YTS`,
   `The Pirate Bay`, `EZTV`, `DonTorrent`, `Nyaa.si`, `Sk-CzTorrent`, and
   `Polskie-Torrenty`.
3. In Jackett's server settings, set the FlareSolverr API URL to
   `http://flaresolverr:8191/`. This is needed only by sources that present an
   anti-bot challenge; the service is not exposed beyond loopback.
4. Copy the API key shown by Jackett into `JACKETT_API_KEY` in `.env`.
5. Apply the setting with `docker compose up -d --force-recreate api`.

Sk-CzTorrent and Polskie-Torrenty are semi-private and require operator-provided
accounts or cookies. Other indexers can also require a working mirror or
additional Jackett configuration. A failure from one source is logged and does
not stop the remaining sources.

When a Torznab row provides only a Jackett `/dl/` URL, CrownIndex downloads the
bounded `.torrent` metadata document and hashes its exact bencoded `info`
dictionary. It never downloads torrent payload data and refuses download URLs
outside the configured Jackett origin. Secret-free locator fingerprints cache
successful resolutions, and new resolutions are rate-limited to at most 25 per
poll and 50 per hour per indexer. Hashless feeds are therefore filled in over
multiple polls without repeatedly consuming the source's download allowance.

The worker runs immediately at API startup and then every 30 minutes by
default. Each source request may take up to five minutes because some indexers
are slow. A manual poll is available for diagnostics:

```powershell
docker compose run --rm api import-jackett
```

Change `CROWN_INDEX_JACKETT_INDEXERS` or
`CROWN_INDEX_JACKETT_POLL_SECONDS` in `.env` when needed. The minimum polling
interval is 60 seconds. `CROWN_INDEX_JACKETT_TIMEOUT_SECONDS` accepts 30–600
seconds.

## Historical backfill

The regular worker intentionally reads recent releases. Historical ingestion
uses separate year-partitioned searches because several Jackett adapters accept
Torznab `offset` while returning no second page. The backfill searches every
configured indexer for each year from the current year down to the configured
minimum, imports unseen hashes, and stores its next year in PostgreSQL.

Start the resumable background backfill after configuring Jackett:

```powershell
docker compose --profile backfill up --build -d backfill
docker compose logs --follow backfill
```

The default history sources are YTS, Nyaa, EZTV, and The Pirate Bay. Change
`CROWN_INDEX_BACKFILL_INDEXERS` in `.env` to add sources after verifying their
search and rate-limit behavior. The job is resumable and only advances
a source/year checkpoint after Bitmagnet accepts the records. Metadata rows
deferred by the global hourly resolution quota are retried before advancing.
Once every configured checkpoint is complete, the container exits successfully
and stays stopped.

For a controlled foreground run:

```powershell
docker compose run --rm backfill `
  backfill-jackett --indexer yts --max-partitions 2 --delay-seconds 1
```

The worker stops an indexer after two identical non-empty year pages, which
indicates that the adapter ignored the year query. A partition returning the
configured maximum is logged as saturated; that source/year may still be
incomplete because Jackett is a search proxy rather than a full archive.

## RedCrown fallback catalog import

CrownIndex can ingest the complete movie/show history exposed by multiple
Butter/Popcorn-compatible APIs. Set the same enabled URLs used by RedCrown:

```dotenv
CROWN_INDEX_BUTTER_URLS=https://primary.example/api/,https://fallback.example/api/
```

Then start the resumable importer:

```powershell
docker compose --profile backfill up --build butter-backfill
```

Every URL is crawled independently and merged into one logical catalog source.
CrownIndex imports movie torrents, show and episode torrents, titles, years,
synopses, ratings, images, genres, sizes, qualities, and peer observations.
Exact episode-to-file paths from show details are retained so multi-episode
packs can select the correct media file during playback. Rich filenames are
stored in CrownIndex's own schema and merged into API responses; Bitmagnet gets
only the compact episode-presence map its indexed schema can safely store.
Each endpoint has separate durable movie/show cursors, so a completed or failed
endpoint cannot hide data from another. A page advances only after Bitmagnet
accepts its torrents and the additive metadata transaction commits. Restarting
the command therefore resumes rather than starting over. Endpoint work is
round-robin; transient failures are retried with a bounded delay while healthy
endpoints continue advancing.

For a controlled validation run:

```powershell
docker compose run --rm butter-backfill backfill-butter --max-pages 1
```

An empty page completes that partition. To intentionally re-import a catalog
from page one, choose a new `CROWN_INDEX_BUTTER_SOURCE_ID`; this keeps prior
provenance and progress auditable instead of silently deleting state. Changing
an endpoint URL also creates a new endpoint-specific cursor.

## EXT imports

EXT currently answers automated requests with HTTP 403, including requests for
`robots.txt`. CrownIndex does not bypass that protection. Snapshot imports are
deterministic and can be tested from HTML pages saved by an authorized operator:

```powershell
docker compose run --rm `
  -v "${PWD}/snapshots:/snapshots:ro" `
  api import-ext-snapshots /snapshots
```

Live ingestion is an explicit command. It first requires an accessible
`robots.txt`, honors disallow rules and crawl delay, and fetches pages
sequentially:

```powershell
docker compose run --rm api import-ext-live --pages 1
```

The importer posts normalized JSON Lines to Bitmagnet's supported `/import`
endpoint instead of writing to Bitmagnet tables directly.

## Development checks

```powershell
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
docker compose config --quiet
```

Ignored integration tests require the pinned Bitmagnet PostgreSQL schema. CI
runs them in Docker; locally, set `CROWN_INDEX_TEST_DATABASE_URL` to a disposable
Bitmagnet v0.10.0 database before running `cargo test -- --ignored`.

## Data and upgrades

PostgreSQL data and Jackett configuration live in the named Docker volumes
`crown-index_postgres-data` and `crown-index_jackett-config`. Rebuilding or
recreating containers preserves those volumes. Do not use `docker compose down
--volumes` unless you intentionally want to erase the index and Jackett state.

Schema changes owned by CrownIndex are additive and run transactionally at API
startup. The adapter validates the pinned Bitmagnet schema before serving.
Review [docs/architecture.md](docs/architecture.md) before changing the
Bitmagnet image tag.

See [docs/architecture.md](docs/architecture.md) for versioning and data-model
decisions.
