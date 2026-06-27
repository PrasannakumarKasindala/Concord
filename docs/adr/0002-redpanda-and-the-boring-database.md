# ADR 0002: Redpanda for local dev, Kafka-compatible in prod; Postgres as the store

- Status: Accepted
- Date: 2026-06-20
- Deciders: P. Kasindala

This ADR records two related choices: the broker we target, and the "boring"
database decision the brief specifically asks to see justified.

## Part A: Redpanda locally, Kafka protocol everywhere

### Context

Local `docker-compose` needs a broker. The two realistic options are Apache
Kafka and Redpanda.

### Decision

`docker-compose` runs **Redpanda**. Application code talks to it through the
standard `confluent-kafka` client over the **Kafka wire protocol**, so nothing in
Concord is Redpanda-specific.

### Why

- **Startup cost for contributors.** A traditional Kafka setup means a broker plus
  either ZooKeeper or KRaft configuration, tuned heap sizes, and a slow first
  boot. Redpanda is a single container that reports healthy in seconds. For a repo
  a reviewer clones and runs once, first-boot friction is the difference between
  "I tried it" and "I read the README and moved on."
- **Zero lock-in.** Because we depend on the Kafka *protocol*, not Redpanda
  features, production can point `CONCORD_KAFKA_BOOTSTRAP` at MSK, Confluent, or a
  self-managed Kafka cluster with no code change. The broker is a deployment
  detail, deliberately.
- **The durability settings are the part that matters, and they are identical on
  both.** `acks=all` plus idempotent producer semantics behave the same whether
  the broker is Kafka or Redpanda. That is where correctness lives, so that is
  what the adapter pins.

### Consequences

We test the *adapter* against Redpanda locally and trust protocol compatibility
in prod. If we ever used a Redpanda-only extension, this ADR would need revisiting;
today we intentionally do not.

## Part B: Postgres is the store, and that is the boring, correct choice

### Context

The outbox needs a durable place to live. A document store or a log-structured
store could hold the rows. The brief asks: if you chose relational, justify it on
consistency grounds.

### Decision

The store is **Postgres** (any ANSI-SQL relational database with
`SELECT ... FOR UPDATE SKIP LOCKED` would do).

### Why

- **The entire pattern depends on one transaction spanning two writes.** The
  outbox row and the business row must commit atomically, or the guarantee
  evaporates. That is a textbook use of a single-node relational transaction. A
  design that put the outbox in a separate NoSQL store would reintroduce exactly
  the dual-write problem Concord exists to eliminate. The relational transaction
  is not a preference here; it is the mechanism.
- **`FOR UPDATE SKIP LOCKED` is the concurrency primitive** that lets many workers
  claim disjoint rows without a distributed lock. It is a mature, well-understood
  relational feature, not something to reimplement over a key-value store.
- **We do not need what NoSQL trades transactions away for.** Horizontal write
  scaling to millions of partitions is not a requirement; the outbox is a
  transient buffer, not a system of record. Choosing a datastore for scale we do
  not need, at the cost of the consistency we absolutely need, would be the
  clever-over-correct mistake.

### Consequences

The primary database takes a small amount of extra write traffic (one outbox row
per event) and needs the `PENDING` partial index. Both are cheap and bounded. If
outbox volume ever rivalled the business workload, we would move the outbox to a
dedicated Postgres instance before we would reach for a different data model.
