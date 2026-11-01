# ADR-002: NATS JetStream for Single-Host Durability vs. Apache Kafka at Multi-Node Scale

- **Status**: Accepted
- **Date**: 2026-10-31

## Context

Requirement #1 mandates reliable ingestion and storage. Placing a durable log broker between `ingestor` and `processor` decouples HTTP device ingestion from ClickHouse background merges, absorbs restarts without dropping data, and allows `processor` to assemble large columnar insert batches.

However, the challenge also requires the entire stack to boot cleanly with a single `docker compose up` command and benchmark maximum sustainable device count on a single host.

## Decision

1. **For single-host Docker Compose deployment**: Use **NATS JetStream** (`FileStorage` WAL, `DiscardNew`, explicit `AckExplicitPolicy`) combined with **Group-Commit Micro-Batching** in `adapter/outbound/natspub`.
2. **For multi-node horizontal scale (10,000+ devices HA)**: Implement a Kafka (KRaft) outbound/inbound adapter pair behind the existing `ports.StreamPublisher` and `ports.BatchProcessor` interfaces, partitioning topics by `device_id`.

## Key Reliability Mechanics

- **WorkQueue Retention + No Silent Drops (`WorkQueuePolicy` + `DiscardNew`)**: Because JetStream acts as a durable in-flight write-ahead log in front of ClickHouse rather than long-term archival storage, `WorkQueuePolicy` reclaims disk and page-cache blocks immediately once `processor` ACKs a persisted batch, preventing 35M+ historical backfill bursts from exhausting container memory. Pairing this with `DiscardNew` (`2 GiB` cap) ensures that if ClickHouse is unavailable long enough to fill the WAL, JetStream rejects new publishes, `natspub` returns `domain.ErrBackpressure`, and `httpingest` responds `503 Service Unavailable` so device agents retry with backoff rather than losing data silently.
- **Group-Commit PubAck**: `natspub` coalesces concurrent HTTP requests over an `8ms` / `4,000`-sample window into a single binary `TLMB` frame and **blocks every waiting HTTP handler until JetStream confirms disk persistence (`PubAck`)**.
- **Post-Commit Consumer ACK**: `natssub` calls `msg.Ack()` *only after* ClickHouse commits `WriteBatch`. Failures trigger `msg.NakWithDelay(2s)`.

## Alternatives Considered

- **Apache Kafka (KRaft) in Local Compose**: Industry standard for partitioned log storage and multi-consumer replay, and our preferred choice in a multi-node production cluster. Deferred for the single-node Compose deliverable because a JVM broker reserves `1–2 GiB` RAM and longer startup warmup on a constrained single-machine benchmark, whereas NATS JetStream runs in `~25 MiB` RAM with `< 1s` startup while providing identical disk-backed ACK semantics.
- **In-Memory Go Channel Only**: Zero external broker, but violates reliable ingestion if `ingestor`/`processor` crashes or restarts before flushing to ClickHouse.

## Evolution Trigger

Migrate from single-node NATS JetStream to a 3-broker **Apache Kafka (KRaft)** cluster (or 3-node clustered JetStream) when multi-AZ broker replication or `> 5` horizontally scaled `processor` instances partitioned deterministically by `device_id` are required.
