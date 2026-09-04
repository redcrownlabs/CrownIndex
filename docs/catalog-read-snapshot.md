# Catalog read snapshot

Date: 2026-08-15
Status: Accepted

## Requirement

Butter-compatible browse and show-detail requests must remain responsive while
Bitmagnet contains millions of torrents. Imports, DHT observations, fallback
catalog records, and TMDB correlations must continue to merge additively.

## Problem

The original browse query resolved and grouped every populated
`torrent_contents` association for each request. With 1.6 million associations,
one page required 6-19 seconds and five concurrent Home requests amplified the
same work. Show detail repeated the global aggregation even though it needed
only one canonical identity.

## Decision

CrownIndex owns a generation-based catalog read snapshot in its PostgreSQL
schema. A refresh builds resolved movie and show torrent rows, then derives the
catalog item rows from that same repeatable-read transaction. Only after both
sets and their counts are complete does the transaction mark the generation
active. Readers therefore observe either the complete old generation or the
complete new generation.

The API refreshes the snapshot every five minutes. The previous generation is
retained until a later generation has been published, so a failed build cannot
remove the last usable catalog. One PostgreSQL advisory transaction lock
serializes refreshes across CrownIndex processes. Older retired generations are
deleted only after activation succeeds.

Browse, filtering, sorting, paging, artwork, genres, and torrent source names
read from the active snapshot. Show detail remains live but restricts its base
query to the requested canonical identity and correlated aliases. A composite
identity index supports both the snapshot build and detail lookup.

## Invariants

- Snapshot publication is atomic across item metadata and torrent unions.
- Correlated fallback associations are added to the canonical identity; source
  rows and their torrent associations are never overwritten or deleted.
- A refresh failure leaves the currently active generation unchanged.
- At most one refresh builds at a time across all API processes.
- An active generation contains only non-adult movies and shows that have at
  least one materialized torrent.
- API paging and sorting operate on snapshot items before torrent rows expand
  the selected page.
- While the service runs, imported or DHT-derived changes become visible within
  five minutes after a successful refresh.

## Tradeoffs

Catalog reads are intentionally eventually consistent by at most five minutes.
This bounded delay replaces repeated multi-million-row work on every request.
Refresh temporarily uses storage for the active, newly built, and one rollback
generation; the older retired generation is removed after successful
publication. The extra storage and periodic batch work are accepted to provide
predictable interactive latency and failure-safe reads.
