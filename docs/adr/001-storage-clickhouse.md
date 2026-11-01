# ADR-001: ClickHouse as Primary Time-Series Storage Engine

- **Status**: Accepted
- **Date**: 2026-10-31

## Context

Each monitored server emits 7 percentage sensors (`0–100%`) every second:
- 1 device = `86,400` samples/day = `31,536,000` samples/year (`220,752,000` individual sensor readings/year).
- 1,000 devices = `31.5B` wide rows/year; 10,000 devices = `315.4B` wide rows/year (`~2.21 trillion` sensor values).

The storage layer must sustain high-throughput append-heavy writes, compress monotonic timestamps and bounded floating-point gauges aggressively, support hot/cold storage tiering for older partitions, and serve 1-year analytical time-bucket queries in `< 1 second`.

## Decision

Select **ClickHouse** (`MergeTree` family) as the time-series storage engine, accessed exclusively behind the `ports.SampleWriter` and `ports.SampleReader` outbound interfaces.

## Alternatives Considered

| Engine | Pros | Why Rejected for Primary Choice |
| :--- | :--- | :--- |
| **TimescaleDB (PostgreSQL)** | Full SQL familiarity, continuous aggregates, strong transactional ecosystem. | Row-oriented heap + B-tree chunk indexes incur ~3–5x higher disk footprint and significantly higher CPU/IO overhead per inserted row above ~100k rows/sec on a single node, requiring aggressive compression jobs that compete with live writes. |
| **InfluxDB (OSS v2 / v3)** | Purpose-built TSM engine for metrics. | OSS clustering/high-availability story is restricted in open-source editions; cardinality and long-horizon analytical scans are less predictable than ClickHouse `MergeTree`. |
| **Prometheus / VictoriaMetrics** | Excellent operational simplicity for scraping metrics. | Optimized for pull-based PromQL rather than push-based reliable event ingestion with custom wide-row schemas and deterministic per-batch commit ACKs. |

## Consequences

### Positive
- **Columnar Codec Efficiency**: `DoubleDelta + ZSTD(1)` reduces regular `1s` `DateTime` columns to fractions of a bit per row; `Gorilla + ZSTD(1)` compresses slowly varying `Float32` gauges (`0..100`) to a few bytes per 7-sensor row.
- **Vectorized Execution & Sparse Primary Index**: `ORDER BY (device_id, ts)` keeps every device's history physically contiguous on disk so single-device scans read only matching `8,192`-row granules.
- **Native Hot/Cold Tiering**: `PARTITION BY toYYYYMM(ts)` allows transparent `TTL ts + INTERVAL 30 DAY TO VOLUME 'cold'` policies onto S3/object storage without application changes.

### Negative & Mitigations
- **Small-Insert Penalty (`Too many parts`)**: ClickHouse cannot accept thousands of individual 1-row inserts/sec directly. *Mitigation*: All writes are buffered in NATS JetStream and flushed by `processor` in large columnar batches via `PrepareBatch` (`Column(i).Append`).
- **Eventual Background Merges**: Aggregations and deduplication finalize asynchronously during merges. *Mitigation*: Rollup queries apply `GROUP BY bucket` with `avgMerge`/`min`/`max` at read time so unmerged parts still produce exact results immediately.
