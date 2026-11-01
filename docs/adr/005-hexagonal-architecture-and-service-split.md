# ADR-005: Strict Hexagonal Architecture (Ports & Adapters) and Read/Write Service Isolation

- **Status**: Accepted
- **Date**: 2026-10-31

## Context

This system must remain maintainable by a multi-engineer team over several years, survive high write bursts without degrading `< 1s` chart queries, and support future infrastructure migrations (e.g., NATS JetStream → Kafka, or ClickHouse → an alternative analytical store) without rewriting core logic.

## Decision

1. **Strict Hexagonal Architecture (`domain` → `ports` → `usecase` ← `adapter`)**:
   - `internal/domain`: Pure Go standard library (`errors`, `math`, `strings`, `time`). Encapsulates `DeviceID`, `Percentage` (`0..100` invariant), `Sensor`, `Sample`, `TimeRange`, and `Resolution` routing rules (`1s`, `1m`, `1h`). Zero JSON tags, SQL strings, HTTP statuses, or SDK types.
   - `internal/ports`: Inbound (`TelemetryIngestor`, `BatchProcessor`, `TelemetryQuery`) and Outbound (`StreamPublisher`, `SampleWriter`, `SampleReader`, `MetricsRecorder`) interfaces referencing only `context.Context` and `domain` types.
   - `internal/usecase`: Pure orchestration (`IngestService`, `ProcessBatchService`, `QueryService`) with zero infrastructure imports.
   - `internal/adapter/{inbound,outbound}`: Owns HTTP JSON/gzip DTOs, embedded HTML UI, NATS wire framing, and ClickHouse columnar SQL.
2. **Three Independent Runtime Binaries (`cmd/ingestor`, `cmd/processor`, `cmd/query-api`)**:
   - Compiled from the same repository and wired solely in thin `main.go` composition roots.

## Consequences

### Positive
- **Read SLO Isolation**: Under heavy write saturation or GC pressure in `ingestor`/`processor`, `query-api` runs in an isolated process with dedicated connection pools, keeping 1-year chart latencies flat.
- **Independent Horizontal Scaling**: At 10,000+ devices, stateless `ingestor` pods scale on CPU/HTTP RPS, `processor` scales on consumer lag, and `query-api` scales on dashboard traffic.
- **Zero-Touch Core During Storage Migration**: Implementing a `DualWriteSampleWriter` or shadow-read verifier for zero-downtime database migration happens strictly inside `internal/adapter/outbound`—`domain`, `ports`, and `usecase` remain untouched.

### Negative
- Mapping between transport DTOs and `domain.Sample` introduces explicit adapter structs rather than reusing one struct with `json:` and `ch:` tags everywhere. We accept this tiny boilerplate cost to guarantee strict boundary isolation.
