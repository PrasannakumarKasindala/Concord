# ADR 0001: Poll the outbox table; do not read the WAL via logical replication

- Status: Accepted
- Date: 2026-06-18
- Deciders: P. Kasindala

## Context

Concord has to detect newly-committed outbox rows and get them onto the stream.
There are two well-established ways to do the detection half:

1. **Poll** the `outbox` table for `PENDING` rows on an interval, claiming them
   with `SELECT ... FOR UPDATE SKIP LOCKED`.
2. **Change Data Capture (CDC)** via Postgres logical replication (a replication
   slot + a decoding plugin like `pgoutput` / `wal2json`, or Debezium in front),
   streaming committed changes off the write-ahead log.

CDC is the more fashionable answer, and in my previous work building Kafka
ingestion off transactional databases it is genuinely the right tool at a
certain scale. That is exactly why this decision needs to be written down rather
than assumed.

## Decision

Concord uses **polling with `FOR UPDATE SKIP LOCKED`** as its default and only
built-in detection strategy for v0.x.

## Why

- **Operational surface area.** A replication slot is a stateful, easily-footgunned
  object. If a consumer stalls, an un-advanced slot pins the WAL and Postgres disk
  usage grows until the instance falls over. That failure mode has taken down
  production databases at companies far more careful than a portfolio project. A
  polling relay that dies simply stops polling; the data waits safely in a table.
- **`SKIP LOCKED` already gives horizontal scale.** The usual argument for CDC is
  throughput. But multiple relay workers polling with `FOR UPDATE SKIP LOCKED`
  scale out linearly without coordination, because each worker claims a disjoint
  set of rows. The benchmark shows a single 6-worker relay sustaining ~4k
  records/sec with single-digit-ms p95 before the (simulated) broker becomes the
  bottleneck. That covers the throughput most services will ever need.
- **Portability and testability.** Polling works against any relational database
  and against an in-memory fake, which is why the exact relay logic runs in the
  test suite and the benchmark with no infra. A WAL-decoding path would be
  Postgres-specific and effectively untestable without a live server.
- **Latency is good enough.** Polling adds at most one poll interval of latency
  (default 50ms, tunable). For an outbox whose entire purpose is durable,
  eventually-consistent delivery, that is well within budget. If a use case needed
  sub-10ms end-to-end, CDC would earn its complexity.

## Consequences

- We accept up to `poll_interval_ms` of added latency and a small, constant query
  load on the primary even when idle. The partial index on `(next_attempt_at)
  WHERE status='PENDING'` keeps that query cheap by ignoring published tombstones.
- A periodic reaper (future work) should archive or delete `PUBLISHED` rows so the
  table does not grow without bound.
- **When we would revisit this:** sustained throughput above what horizontal
  polling can serve, or a hard end-to-end latency SLA in the low milliseconds. At
  that point a logical-replication adapter would slot in behind the existing
  `OutboxStore` port without touching the relay. The port boundary is what keeps
  this decision cheap to reverse.
