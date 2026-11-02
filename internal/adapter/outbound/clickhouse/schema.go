package clickhouse

var schemaStatements = []string{
	`CREATE TABLE IF NOT EXISTS telemetry_raw (
		device_id LowCardinality(String),
		ts DateTime('UTC') CODEC(DoubleDelta, ZSTD(1)),
		cpu Float32 CODEC(Gorilla, ZSTD(1)),
		memory Float32 CODEC(Gorilla, ZSTD(1)),
		disk_space Float32 CODEC(Gorilla, ZSTD(1)),
		disk_busy Float32 CODEC(Gorilla, ZSTD(1)),
		net_rx Float32 CODEC(Gorilla, ZSTD(1)),
		net_tx Float32 CODEC(Gorilla, ZSTD(1)),
		fan Float32 CODEC(Gorilla, ZSTD(1))
	)
	ENGINE = MergeTree()
	PARTITION BY toYYYYMM(ts)
	ORDER BY (device_id, ts)
	SETTINGS non_replicated_deduplication_window = 1000`,

	`CREATE TABLE IF NOT EXISTS telemetry_1m (
		device_id LowCardinality(String),
		bucket DateTime('UTC') CODEC(DoubleDelta, ZSTD(1)),
		cpu_avg AggregateFunction(avg, Float32),
		cpu_min SimpleAggregateFunction(min, Float32),
		cpu_max SimpleAggregateFunction(max, Float32),
		memory_avg AggregateFunction(avg, Float32),
		memory_min SimpleAggregateFunction(min, Float32),
		memory_max SimpleAggregateFunction(max, Float32),
		disk_space_avg AggregateFunction(avg, Float32),
		disk_space_min SimpleAggregateFunction(min, Float32),
		disk_space_max SimpleAggregateFunction(max, Float32),
		disk_busy_avg AggregateFunction(avg, Float32),
		disk_busy_min SimpleAggregateFunction(min, Float32),
		disk_busy_max SimpleAggregateFunction(max, Float32),
		net_rx_avg AggregateFunction(avg, Float32),
		net_rx_min SimpleAggregateFunction(min, Float32),
		net_rx_max SimpleAggregateFunction(max, Float32),
		net_tx_avg AggregateFunction(avg, Float32),
		net_tx_min SimpleAggregateFunction(min, Float32),
		net_tx_max SimpleAggregateFunction(max, Float32),
		fan_avg AggregateFunction(avg, Float32),
		fan_min SimpleAggregateFunction(min, Float32),
		fan_max SimpleAggregateFunction(max, Float32)
	)
	ENGINE = AggregatingMergeTree()
	PARTITION BY toYYYYMM(bucket)
	ORDER BY (device_id, bucket)
	SETTINGS non_replicated_deduplication_window = 1000`,

	`CREATE MATERIALIZED VIEW IF NOT EXISTS mv_telemetry_1m TO telemetry_1m AS
	SELECT
		device_id,
		toStartOfMinute(ts) AS bucket,
		avgState(cpu) AS cpu_avg, min(cpu) AS cpu_min, max(cpu) AS cpu_max,
		avgState(memory) AS memory_avg, min(memory) AS memory_min, max(memory) AS memory_max,
		avgState(disk_space) AS disk_space_avg, min(disk_space) AS disk_space_min, max(disk_space) AS disk_space_max,
		avgState(disk_busy) AS disk_busy_avg, min(disk_busy) AS disk_busy_min, max(disk_busy) AS disk_busy_max,
		avgState(net_rx) AS net_rx_avg, min(net_rx) AS net_rx_min, max(net_rx) AS net_rx_max,
		avgState(net_tx) AS net_tx_avg, min(net_tx) AS net_tx_min, max(net_tx) AS net_tx_max,
		avgState(fan) AS fan_avg, min(fan) AS fan_min, max(fan) AS fan_max
	FROM telemetry_raw
	GROUP BY device_id, bucket`,

	`CREATE TABLE IF NOT EXISTS telemetry_1h (
		device_id LowCardinality(String),
		bucket DateTime('UTC') CODEC(DoubleDelta, ZSTD(1)),
		cpu_avg AggregateFunction(avg, Float32),
		cpu_min SimpleAggregateFunction(min, Float32),
		cpu_max SimpleAggregateFunction(max, Float32),
		memory_avg AggregateFunction(avg, Float32),
		memory_min SimpleAggregateFunction(min, Float32),
		memory_max SimpleAggregateFunction(max, Float32),
		disk_space_avg AggregateFunction(avg, Float32),
		disk_space_min SimpleAggregateFunction(min, Float32),
		disk_space_max SimpleAggregateFunction(max, Float32),
		disk_busy_avg AggregateFunction(avg, Float32),
		disk_busy_min SimpleAggregateFunction(min, Float32),
		disk_busy_max SimpleAggregateFunction(max, Float32),
		net_rx_avg AggregateFunction(avg, Float32),
		net_rx_min SimpleAggregateFunction(min, Float32),
		net_rx_max SimpleAggregateFunction(max, Float32),
		net_tx_avg AggregateFunction(avg, Float32),
		net_tx_min SimpleAggregateFunction(min, Float32),
		net_tx_max SimpleAggregateFunction(max, Float32),
		fan_avg AggregateFunction(avg, Float32),
		fan_min SimpleAggregateFunction(min, Float32),
		fan_max SimpleAggregateFunction(max, Float32)
	)
	ENGINE = AggregatingMergeTree()
	PARTITION BY toYYYYMM(bucket)
	ORDER BY (device_id, bucket)
	SETTINGS non_replicated_deduplication_window = 1000`,

	`CREATE MATERIALIZED VIEW IF NOT EXISTS mv_telemetry_1h TO telemetry_1h AS
	SELECT
		device_id,
		toStartOfHour(ts) AS bucket,
		avgState(cpu) AS cpu_avg, min(cpu) AS cpu_min, max(cpu) AS cpu_max,
		avgState(memory) AS memory_avg, min(memory) AS memory_min, max(memory) AS memory_max,
		avgState(disk_space) AS disk_space_avg, min(disk_space) AS disk_space_min, max(disk_space) AS disk_space_max,
		avgState(disk_busy) AS disk_busy_avg, min(disk_busy) AS disk_busy_min, max(disk_busy) AS disk_busy_max,
		avgState(net_rx) AS net_rx_avg, min(net_rx) AS net_rx_min, max(net_rx) AS net_rx_max,
		avgState(net_tx) AS net_tx_avg, min(net_tx) AS net_tx_min, max(net_tx) AS net_tx_max,
		avgState(fan) AS fan_avg, min(fan) AS fan_min, max(fan) AS fan_max
	FROM telemetry_raw
	GROUP BY device_id, bucket`,

	`CREATE TABLE IF NOT EXISTS devices_registry (
		device_id LowCardinality(String),
		last_seen SimpleAggregateFunction(max, DateTime('UTC'))
	)
	ENGINE = AggregatingMergeTree()
	ORDER BY device_id
	SETTINGS non_replicated_deduplication_window = 1000`,

	`CREATE MATERIALIZED VIEW IF NOT EXISTS mv_devices_registry TO devices_registry AS
	SELECT
		device_id,
		max(ts) AS last_seen
	FROM telemetry_raw
	GROUP BY device_id`,
}
