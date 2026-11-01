# ADR-004: Wide Row Schema and Block-Hash Deduplication

- **Status**: Accepted
- **Date**: 2026-10-31

## Context

Every monitored device reports the same fixed set of 7 percentage sensors (`cpu`, `memory`, `disk_space`, `disk_busy`, `net_rx`, `net_tx`, `fan`) every second. In addition, our NATS JetStream → ClickHouse pipeline uses **at-least-once delivery**: if `processor` crashes after ClickHouse commits a batch but before sending `msg.Ack()` to JetStream, the batch will be redelivered.

## Decision

1. **Wide Row Schema**: Store all 7 sensors in a single row `(device_id, ts, cpu, memory, disk_space, disk_busy, net_rx, net_tx, fan)` ordered by `(device_id, ts)` and partitioned monthly (`toYYYYMM(ts)`).
2. **Block-Hash Deduplication**: Configure `SETTINGS non_replicated_deduplication_window = 1000` on `telemetry_raw` (`MergeTree`) and deterministically sort every batch by `(device_id, ts)` in `usecase.ProcessBatchService` prior to insertion.

## Rationale

### Wide Schema vs. Narrow `(device_id, ts, sensor_name, value)` Schema
- **7x Fewer Rows**: `31.5M` rows/device/year instead of `220.8M` rows/device/year.
- **No Repeated Metadata**: `device_id` and `ts` are stored once per second rather than 7 times.
- **Single-Pass Multi-Sensor Reads**: Selecting 4 sensors on one chart reads 1 contiguous stream of rows with zero self-joins or `GROUP BY` pivoting across sensor names.

### Why Block-Hash Deduplication Beats `ReplacingMergeTree` Alone
A subtle trap in ClickHouse is that Incremental Materialized Views (`mv_telemetry_1m`, `mv_telemetry_1h`) trigger **on the inserted block at `INSERT` time**, *before* `ReplacingMergeTree` background merges deduplicate rows. If a retried batch were inserted twice, `ReplacingMergeTree` would eventually clean up `telemetry_raw`, **but `telemetry_1m` and `telemetry_1h` would double-count that batch's `avgState` sum/count**.

By sorting each batch deterministically in `ProcessBatchService` and enabling `non_replicated_deduplication_window = 1000` (which becomes native ZooKeeper/Keeper-backed deduplication on `ReplicatedMergeTree` in production), ClickHouse computes the SHA hash of the inserted block, recognizes an exact retry of any of the last 1,000 blocks, and ignores the duplicate **before firing Materialized Views**.

## Trade-offs

- Adding an 8th sensor in the future requires an `ALTER TABLE ADD COLUMN` (metadata-only operation in ClickHouse for nullable/default columns) and updating the MV definitions. For a well-defined hardware agent schema, a 7x row reduction outweighs schema-less EAV flexibility; arbitrary user-defined tags later can use a separate `Map(LowCardinality(String), Float32)` table if ever needed.
