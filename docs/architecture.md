# Architecture decision: reuse Bitmagnet

Date: 2026-07-17
Status: Accepted

## Requirement

The system must run locally in Docker, accumulate verified BitTorrent metadata,
ingest permitted external index snapshots, and expose a Butter-compatible API
that RedCrown can configure as a source.

## Decision

Bitmagnet `v0.10.0` owns DHT participation, BEP 9 metadata retrieval,
classification, PostgreSQL migrations, and torrent import. CrownIndex owns only
the compatibility boundary:

```text
DHT ──────────────> Bitmagnet ──> PostgreSQL <── CrownIndex Butter API
                         ^              ^
                         │              │ ingestion watermarks
Jackett/Torznab ─────────┤              │
Butter API mirrors ──────┤              │
EXT snapshot/live ───────┘              │
                  through Bitmagnet /import
```

Building another DHT crawler would duplicate a mature subsystem and create a
large protocol/security maintenance burden. Bitmagnet already has Docker,
GraphQL, Torznab, classification, and an import endpoint. CrownIndex reads the
database because deterministic content-level pagination and grouping cannot be
expressed by Bitmagnet's current torrent-level GraphQL search. It validates the
expected schema at startup and pins the Bitmagnet image; upgrades must update
the compatibility query and tests together.

## Invariants

- The public HTTP port binds to loopback by default.
- External import writes go through Bitmagnet's `/import` API.
- Tracker-specific requests and credentials remain owned by Jackett.
- Jackett owns tracker response caching; CrownIndex does not force cache
  bypasses that would amplify slow or temporarily unavailable upstreams.
- Each Jackett source is polled independently so one failure cannot block the
  remaining configured sources.
- A Jackett observation is marked imported only after Bitmagnet accepts its
  batch; the watermark key is `(source, info hash)`.
- An info hash is derived from the magnet URI and validated before import.
- Torznab download URLs are followed only when they resolve back to Jackett's
  own `/dl/` route. The response is bounded, parsed as bencode, and its exact
  v1 `info` dictionary bytes are SHA-1 hashed before import.
- Successful `/dl/` resolutions are cached by `(source, SHA-256 locator)` after
  API-key query parameters are removed. A PostgreSQL reservation limits new
  resolutions across all workers to 50 per hour per indexer; cached resolutions
  consume no source quota.
- Historical ingestion uses persistent `(indexer, next year)` checkpoints.
  Checkpoints advance only after Bitmagnet import and ingestion watermarking
  succeed, and never while metadata resolutions are deferred.
- Backfill uses year-partitioned searches because live verification showed that
  configured adapters can accept Torznab offsets while returning no second
  page. Two repeated non-empty page fingerprints stop an indexer that ignored
  the year query rather than recording false progress.
- Every Butter catalog URL is crawled as an independent union member. The
  importer persists endpoint-specific movie/show cursors and advances each only
  after torrent import and the additive metadata transaction both succeed.
  Round-robin scheduling and delayed retries prevent one large or temporarily
  unavailable endpoint from starving the other union members. Within each
  endpoint, the least-advanced movie/show partition runs next so a long movie
  history cannot starve show synchronization.
- Butter item metadata is stored under an explicit Bitmagnet metadata source.
  Torrent/content associations are inserted alongside existing classifications
  rather than overwriting TMDB or DHT-derived associations.
- Content enrichment is additive. `crown_index.content_correlations` maps a
  source-owned identity to one canonical TMDB identity using a fallback API's
  exact TMDB ID when available, then an exact IMDb ID, and only otherwise
  shared-info-hash evidence; it never changes or deletes the source identity or
  its torrent associations. Direct TMDB IDs are persisted with fallback
  metadata so either ingestion order produces the same correlation. IMDb IDs
  are resolved through TMDB's external-ID endpoint, which avoids trying to
  infer a series from season-pack filenames. A conflicting canonical match is
  rejected instead of allowing the result to depend on importer order.
- The Butter read model resolves correlations before pagination and unions
  torrent hashes, swarm counts, and classifications under the canonical item.
  The same correlation resolution is required by show-detail reads; list and
  detail routes must expose the same additive torrent set. Repeated
  observations of one hash are collapsed. Uncorrelated Jackett/DHT torrents
  remain searchable raw data until conservative enrichment supplies identity
  evidence; they are never guessed into a catalog title.
- Show torrents are attached to an episode only when an exact episode can be
  derived from Bitmagnet metadata or an upstream episode record. When a Butter
  source supplies the file path for an episode, CrownIndex preserves that path
  in `crown_index.butter_episode_files` and merges it into the API read model.
  Bitmagnet receives only a compact Boolean episode map. Its pinned schema has a
  B-tree index on the JSON field whose per-row limit rejects realistic season
  packs with many long filenames; rich source metadata therefore remains in
  CrownIndex-owned tables instead of modifying a third-party-owned index.
- Pending Butter metadata from versions that stored paths in Bitmagnet-shaped
  JSON is migrated transactionally into `butter_episode_files` and compacted at
  startup. One oversized season can never block reconciliation for unrelated
  torrents.
- Whole-season torrents without file metadata are not guessed onto episodes.
- EXT live crawling never attempts challenge bypass and stops when robots policy
  cannot be retrieved or permits no relevant path.
- API pagination operates on grouped content, not raw torrent rows.
- Interactive catalog reads use an atomically published, generation-based
  PostgreSQL snapshot. The snapshot is built from one repeatable-read view of
  additive correlations and torrent associations, refreshed every five
  minutes, and activated only after both item and torrent rows are complete.
  A failed refresh leaves the previous generation active.
- The host API port is configurable because loopback ports are shared with
  unrelated local applications; the container always listens on port 8080.
- Operator-requested searches are durable PostgreSQL jobs and reuse the normal
  Jackett, Bitmagnet, enrichment, and snapshot path. Source failures,
  resolution deferrals, and result saturation remain visible instead of being
  reported as complete. See `docs/operator-sync.md`.
- TMDB batch work performs one availability request before selecting rows.
  Transport failures are retried after a bounded delay, while unparseable and
  conservatively rejected titles remain terminal. This prevents a network-wide
  TLS or DNS failure from permanently poisoning every item in a batch.

## Tradeoffs

The adapter intentionally couples its read model to Bitmagnet `v0.10.0`.
Pinning and startup schema validation make that coupling explicit and fail-fast.
The alternative GraphQL API is currently alpha and returns one row per torrent,
which cannot provide stable Butter pagination after grouping multiple qualities
under a title.

TMDB enrichment is optional because it requires an operator-provided key. When
disabled, DHT indexing and generic Bitmagnet search still work, but the media
catalog is necessarily sparse. When enabled, CrownIndex parses torrent names
into conservative movie/show candidates, verifies title and year through TMDB,
then writes Bitmagnet-compatible content, genre, poster, rating, and torrent
correlation rows. Failed and rejected attempts are recorded in
`crown_index.tmdb_enrichment_attempts` so repeated worker passes do not
continuously query TMDB for the same unsuitable torrent names. CrownIndex does
not guess a cover or category unless a TMDB result passes these checks.
Transient transport failures become eligible for retry after six hours;
unparsed names and valid no-match responses are not retried automatically.

TMDB credentials belong only to CrownIndex. Bitmagnet's independent classifier
is deliberately left without TMDB credentials because CrownIndex owns the
canonical matching rules and can use bearer authentication without placing a
token in a query string or Bitmagnet log.

TMDB is the canonical identity and presentation source after a correlation,
while Butter metadata remains intact in its own source rows. This accepts that
the public catalog presents TMDB's normalized title, artwork, genres, and
rating, while preserving fallback metadata for auditing and future field-level
fallbacks. Jackett contributes torrent observations and source provenance; it
does not replace title metadata.

Jackett is a separate pinned service rather than linked code. This keeps its
GPL-licensed tracker implementations and rapidly changing site knowledge behind
the stable Torznab protocol boundary. CrownIndex stores ingestion watermarks
and secret-free Jackett download resolutions in its own `crown_index`
PostgreSQL schema; it never modifies Jackett's configuration files or
Bitmagnet-owned tables directly.

FlareSolverr is likewise an out-of-process, loopback-only optional dependency
of Jackett. CrownIndex never sends tracker URLs or requests to it directly.
This preserves the Torznab boundary while allowing an operator to enable
Jackett's standard challenge-handling path for affected indexers. Browser-based
challenge solving consumes more memory and is not guaranteed to solve CAPTCHAs.

Jackett is optimized for recent feeds and search, not complete archival export.
Year partitioning recovers discoverable history without tracker-specific code,
but a saturated or search-limited partition can remain incomplete. CrownIndex
reports that condition explicitly instead of claiming exhaustive coverage.

Butter-compatible APIs are better suited to media history because they expose
stable item pages and show details. Configured endpoints often overlap but can
lag or contain source-specific torrent variants, so CrownIndex crawls all of
them and relies on stable item IDs, info hashes, and canonical correlations for
deduplication. This increases request volume; the importer intentionally runs
as a dedicated, rate-limited Docker job rather than delaying API startup.
