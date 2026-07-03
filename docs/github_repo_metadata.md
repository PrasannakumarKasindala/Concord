# GitHub repo metadata

Paste these when creating the repo, or set them via `gh repo edit` after pushing.

## Repository name

    concord

## Description (shows under the repo name)

A transactional outbox relay that guarantees your database and event stream never
disagree. Postgres + Kafka/Redpanda, exponential-backoff retries, dead-letter
archival to S3/MinIO, and effectively-once delivery. Ports-and-adapters core with
zero runtime deps, a real load benchmark, and full production tooling.

## Topics (comma-separated in the GitHub UI)

transactional-outbox, event-driven, kafka, redpanda, postgres, cdc,
data-consistency, dead-letter-queue, exactly-once, reliability, data-engineering,
python, hexagonal-architecture, distributed-systems, docker

## `gh` CLI one-liner (after pushing)

```bash
gh repo edit \
  --description "A transactional outbox relay that guarantees your database and event stream never disagree. Postgres + Kafka/Redpanda, backoff retries, dead-letter archival, effectively-once delivery." \
  --add-topic transactional-outbox --add-topic event-driven --add-topic kafka \
  --add-topic redpanda --add-topic postgres --add-topic cdc \
  --add-topic data-consistency --add-topic dead-letter-queue \
  --add-topic exactly-once --add-topic reliability \
  --add-topic data-engineering --add-topic python \
  --add-topic hexagonal-architecture --add-topic distributed-systems \
  --add-topic docker
```

## Suggested pinned-repo caption (for your profile)

> Solves the dual-write problem in event-driven data platforms. Committed DB
> changes can never be silently absent from the stream. Real load benchmark,
> ADRs, and production tooling included.
