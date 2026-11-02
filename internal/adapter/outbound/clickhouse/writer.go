package clickhouse

import (
	"context"
	"fmt"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/danialv/telemetry/internal/domain"
)

func (r *Repository) WriteBatch(ctx context.Context, samples []domain.Sample) error {
	if len(samples) == 0 {
		return nil
	}

	writeCtx := ch.Context(ctx, ch.WithSettings(ch.Settings{
		"deduplicate_blocks_in_dependent_materialized_views": 1,
	}))
	batch, err := r.conn.PrepareBatch(writeCtx, "INSERT INTO telemetry_raw (device_id, ts, cpu, memory, disk_space, disk_busy, net_rx, net_tx, fan)")
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	n := len(samples)
	deviceIDs := make([]string, n)
	timestamps := make([]time.Time, n)
	cols := [7][]float32{
		make([]float32, n),
		make([]float32, n),
		make([]float32, n),
		make([]float32, n),
		make([]float32, n),
		make([]float32, n),
		make([]float32, n),
	}

	for i, s := range samples {
		deviceIDs[i] = s.DeviceID.String()
		timestamps[i] = s.Timestamp
		cols[0][i] = s.Readings.CPU.Float32()
		cols[1][i] = s.Readings.Memory.Float32()
		cols[2][i] = s.Readings.DiskSpace.Float32()
		cols[3][i] = s.Readings.DiskBusy.Float32()
		cols[4][i] = s.Readings.NetRx.Float32()
		cols[5][i] = s.Readings.NetTx.Float32()
		cols[6][i] = s.Readings.Fan.Float32()
	}

	if err := batch.Column(0).Append(deviceIDs); err != nil {
		return err
	}
	if err := batch.Column(1).Append(timestamps); err != nil {
		return err
	}
	for c := 0; c < len(cols); c++ {
		if err := batch.Column(2 + c).Append(cols[c]); err != nil {
			return err
		}
	}

	return batch.Send()
}
