# ADR-003: Sub-Second 1-Year Queries via Incremental Materialized View Rollups

- **Status**: Accepted
- **Date**: 2026-10-31

## Context

Requirement #2 states that users must inspect any sensor of any device across any window from **5 minutes to 1 year**, and **the 1-year chart must load in `< 1 second`**.

A single device generates `31,536,000` raw `1s` rows per year. Furthermore:
1. A browser viewport is `~1,000–1,600` pixels wide; shipping 31.5M JSON points over HTTP would transfer hundreds of megabytes and freeze the browser.
2. Downsampling 31.5M raw rows on the fly (`GROUP BY toStartOfHour(ts)`) on every dashboard load wastes CPU and disk IOPS under concurrent write load—and fails the `< 1s` target once old partitions move to cold object storage (S3).

## Decision

Maintain **two Incremental Materialized View rollup tiers** (`telemetry_1m` and `telemetry_1h`) plus a lightweight **`devices_registry`** Materialized View, updated automatically at insert time:

| Query Window | Domain Resolution | Target Table | Rows Scanned (Max) | Aggregates Stored |
| :--- | :--- | :--- | :--- | :--- |
| `5m` – `1h` | `1s` | `telemetry_raw` | `300` – `3,600` | Exact raw values |
| `> 1h` – `7d` | `1m` | `telemetry_1m` | `61` – `10,080` | `avgState`, `min`, `max` |
| `> 7d` – `1y` | `1h` | `telemetry_1h` | `169` – `8,760` | `avgState`, `min`, `max` |

Resolution selection is modeled purely inside `internal/domain` (`TimeRange.SelectResolution()`), while `adapter/outbound/clickhouse` maps each `domain.Resolution` to its physical table.

## Why Store `avgState`, `min`, and `max`?

- Using `AggregateFunction(avg, Float32)` (`avgState` / `avgMerge`) guarantees mathematically exact weighted averages even when partial blocks merge asynchronously, unlike naive average-of-averages.
- Storing `SimpleAggregateFunction(min, Float32)` and `SimpleAggregateFunction(max, Float32)` preserves spike visibility: a CPU spike to `99%` lasting 60 seconds inside a 1-hour bucket remains visible in the `min–max` envelope and hover tooltip instead of being smoothed away by the hourly mean.

## Consequences

- **3,600x Read Reduction for 1-Year Charts**: Querying 1 full year reads **`8,760` contiguous rows** (`~2 granules` in ClickHouse) instead of `31,536,000` rows, completing in **`10–25 ms`** even during heavy ingestion storms.
- **Negligible Storage Overhead**: `1m` (`525,600` rows/device/yr) and `1h` (`8,760` rows/device/yr) together add `< 3%` to raw storage volume.
- **Sub-Millisecond Device Autocomplete**: `devices_registry` (`ORDER BY device_id`) serves `/api/v1/devices?q=...` in `< 2 ms` without scanning raw telemetry tables.
