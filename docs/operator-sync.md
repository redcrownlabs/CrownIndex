# Architecture decision: durable targeted syncs

Date: 2026-09-11
Status: Accepted

## Requirement

An operator must be able to request an immediate, narrow tracker search such as
`Dark Matter S02`, see what every configured source did, and have accepted
results appear through CrownIndex without replacing existing catalog data.

## Decision

The operator portal is an embedded, same-origin HTML/CSS/JavaScript client.
Submitting the form creates a PostgreSQL job and one child row per selected
Jackett indexer. The API process runs one queue worker that claims the oldest
queued job with `FOR UPDATE SKIP LOCKED`.

The worker calls Jackett through the existing Torznab adapter and passes unseen
observations through the existing Bitmagnet importer. Watermarks are still
keyed by source and info hash. All fetched hashes, including already-ingested
ones, are eligible for an explicit TMDB retry; this makes a targeted refresh
useful after metadata or connectivity was repaired without creating duplicate
torrent associations.

Bitmagnet materializes accepted imports asynchronously. The worker checks
materialization for at most 30 seconds, enriches at most 250 unique hashes
immediately, records the remainder as pending, and leaves them to the regular
TMDB worker. These limits prevent one interactive request from monopolizing the
service. Each Jackett search requests at most 1,000 rows because several
adapters do not implement reliable offset pagination. A full page marks the job
partial and visible as saturated rather than claiming false completeness.

After processing, the worker atomically publishes a new catalog snapshot.
Interrupted running jobs are requeued when the single local API process starts;
the import and enrichment operations are idempotent.

## Security boundary

Docker publishes the API on loopback by default. Mutations accept JSON only and
require an explicit intent header, which forces a browser cross-origin
preflight; CrownIndex does not enable CORS. Installations exposed beyond
loopback should configure `CROWN_INDEX_OPERATOR_TOKEN`, which protects all
operator JSON routes with bearer authentication. The static portal never
interpolates torrent data as markup and is served with a restrictive content
security policy.

## Invariants

- Targeted sync never writes directly into Bitmagnet-owned tables.
- One source failure cannot discard successful results from another source.
- Existing content identities and torrent associations are never deleted or
  replaced.
- A job is not completed until its source outcomes and aggregate counters are
  persisted.
- Deferred, saturated, and failed work remains visible to the operator.
- Browser refreshes and process restarts do not erase queued work or history.

## Tradeoffs

The portal is deliberately small and dependency-free because it is an operator
surface, not a second media client. Polling job state adds bounded read traffic
but avoids a separate WebSocket lifecycle. Immediate enrichment is bounded;
eventual enrichment remains the responsibility of the existing background
worker.
