# ADR-004: Wide Row Schema and At-Least-Once Delivery Semantics

- **Status**: Accepted
- **Date**: 2026-10-31

## Context

Every monitored device reports the same fixed set of 7 percentage sensors (`cpu`, `memory`, `disk_space`, `disk_busy`, `net_rx`, `net_tx`, `fan`) every second. Between HTTP ingestion, NATS JetStream, and ClickHouse, we must balance write throughput, ClickHouse part counts, and honest retry semantics.

## Decision

1. **Wide Row Schema**: Store all 7 sensors in a single row `(device_id, ts, cpu, memory, disk_space, disk_busy, net_rx, net_tx, fan)` ordered by `(device_id, ts)` and partitioned monthly (`toYYYYMM(ts)`).
2. **At-Least-Once Delivery with In-Place Block Retry Deduplication**:
   - Coalesce up to `32` JetStream micro-frames per ClickHouse insert in `natssub`, sort rows deterministically by `(device_id, ts)` in `usecase.ProcessBatchService`, and configure `non_replicated_deduplication_window = 1000` on `telemetry_raw`, `telemetry_1m`, `telemetry_1h`, and `devices_registry` together with `deduplicate_blocks_in_dependent_materialized_views = 1`.
   - Retry transient ClickHouse `WriteBatch` failures once in-place on the identical slice before sending `NakWithDelay` to JetStream.

## Rationale

### Wide Schema vs. Narrow `(device_id, ts, sensor_name, value)` Schema
- **7x Fewer Rows**: `31.5M` rows/device/year instead of `220.8M` rows/device/year.
- **No Repeated Metadata**: `device_id` and `ts` are stored once per second rather than 7 times.
- **Single-Pass Multi-Sensor Reads**: Selecting 4 sensors on one chart reads 1 contiguous stream of rows with zero self-joins or `GROUP BY` pivoting across sensor names.

### Delivery Semantics & Why We Accept At-Least-Once
- **Why `natssub` merges `Fetch(32)` frames**: `natspub` flushes every `8 ms` across `6` workers to keep HTTP `PubAck` latency low, emitting `~650–750` frames/sec at `40,000` devices. Inserting 1 ClickHouse part per frame would hit ClickHouse's `Too many parts (300)` limit. Merging up to `32` frames keeps ClickHouse inserts near `~21` parts/sec.
- **What block-hash deduplication catches**: When `natssub` retries the *exact same* sorted slice in-place after a transient TCP timeout (with `deduplicate_blocks_in_dependent_materialized_views = 1`), ClickHouse drops the duplicate block across both `telemetry_raw` and the Materialized Views.
- **Where duplicates still happen (At-Least-Once)**:
  1. **Crash or `NAK` regrouping**: If `processor` crashes after ClickHouse commits a block but before `msg.Ack()`, redelivered JetStream messages are fetched alongside newer messages in a different `Fetch(32)` batch. That changes the block hash, writing duplicate rows to `telemetry_raw` and double-counting `avgState` in `telemetry_1m` / `telemetry_1h` (`min`, `max`, and `last_seen` remain idempotent).
  2. **HTTP client retry**: `natspub` coalesces concurrent HTTP requests into shared frames without per-device `Nats-Msg-Id` keys, so if a device times out waiting for `202 Accepted` and retries its POST, those samples are stored twice.

## Trade-offs

- **Gauge Tolerance vs. Strict Idempotency**: For `0–100%` hardware gauges, `min`/`max` envelopes are mathematically idempotent and rare crash-window `avg` weighting is an acceptable trade-off to keep `8 ms` HTTP ingest latency and low ClickHouse part creation. If strict end-to-end idempotency were ever needed (e.g., billing counters), we would require client `Idempotency-Key` headers at the HTTP edge and widen per-partition broker frames (`100–250 ms`) so `processor` inserts 1 ClickHouse batch per broker message keyed by `insert_deduplication_token = <stream>:<seq>`.
- **Schema Evolution**: Adding an 8th sensor requires an `ALTER TABLE ADD COLUMN` (metadata-only in ClickHouse) and updating Materialized View definitions, which is well worth a 7x row reduction.
