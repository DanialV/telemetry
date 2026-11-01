package clickhouse

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/danialv/telemetry/internal/domain"
)

var validColumns = map[domain.Sensor]string{
	domain.SensorCPU:       "cpu",
	domain.SensorMemory:    "memory",
	domain.SensorDiskSpace: "disk_space",
	domain.SensorDiskBusy:  "disk_busy",
	domain.SensorNetRx:     "net_rx",
	domain.SensorNetTx:     "net_tx",
	domain.SensorFan:       "fan",
}

func (r *Repository) ReadSeries(ctx context.Context, query domain.SeriesQuery) (domain.SeriesResult, error) {
	projections := make([]string, 0, 1+len(query.Sensors)*3)
	var sqlQuery string
	fromBound := query.Range.From

	switch query.Resolution {
	case domain.Resolution1s:
		projections = append(projections, "ts")
		for _, s := range query.Sensors {
			col, ok := validColumns[s]
			if !ok {
				return domain.SeriesResult{}, domain.ErrUnsupportedSensor
			}
			projections = append(projections,
				fmt.Sprintf("avg(%s)", col),
				fmt.Sprintf("min(%s)", col),
				fmt.Sprintf("max(%s)", col),
			)
		}
		sqlQuery = fmt.Sprintf(
			"SELECT %s FROM telemetry_raw WHERE device_id = ? AND ts >= ? AND ts <= ? GROUP BY ts ORDER BY ts ASC",
			strings.Join(projections, ", "),
		)
	case domain.Resolution1m, domain.Resolution1h:
		table := "telemetry_1m"
		fromBound = query.Range.From.Truncate(time.Minute)
		if query.Resolution == domain.Resolution1h {
			table = "telemetry_1h"
			fromBound = query.Range.From.Truncate(time.Hour)
		}
		projections = append(projections, "bucket")
		for _, s := range query.Sensors {
			col, ok := validColumns[s]
			if !ok {
				return domain.SeriesResult{}, domain.ErrUnsupportedSensor
			}
			projections = append(projections,
				fmt.Sprintf("avgMerge(%s_avg)", col),
				fmt.Sprintf("min(%s_min)", col),
				fmt.Sprintf("max(%s_max)", col),
			)
		}
		sqlQuery = fmt.Sprintf(
			"SELECT %s FROM %s WHERE device_id = ? AND bucket >= ? AND bucket <= ? GROUP BY bucket ORDER BY bucket ASC",
			strings.Join(projections, ", "),
			table,
		)
	default:
		return domain.SeriesResult{}, domain.ErrInvalidTimeRange
	}

	rows, err := r.conn.Query(ctx, sqlQuery, query.DeviceID.String(), fromBound, query.Range.To)
	if err != nil {
		return domain.SeriesResult{}, fmt.Errorf("query series: %w", err)
	}
	defer rows.Close()

	sensorCount := len(query.Sensors)
	estimatedCap := min(int(query.Range.Duration()/query.Resolution.Step())+2, 16384)
	timestamps := make([]time.Time, 0, estimatedCap)
	series := make([]domain.SensorSeries, sensorCount)
	for i, s := range query.Sensors {
		series[i] = domain.SensorSeries{
			Sensor: s,
			Avg:    make([]float32, 0, estimatedCap),
			Min:    make([]float32, 0, estimatedCap),
			Max:    make([]float32, 0, estimatedCap),
		}
	}

	var ts time.Time
	avgVals := make([]float64, sensorCount)
	minVals := make([]float32, sensorCount)
	maxVals := make([]float32, sensorCount)
	scanDest := make([]any, 1+sensorCount*3)
	scanDest[0] = &ts
	for i := 0; i < sensorCount; i++ {
		scanDest[1+i*3] = &avgVals[i]
		scanDest[1+i*3+1] = &minVals[i]
		scanDest[1+i*3+2] = &maxVals[i]
	}

	for rows.Next() {
		if err := rows.Scan(scanDest...); err != nil {
			return domain.SeriesResult{}, fmt.Errorf("scan series row: %w", err)
		}
		timestamps = append(timestamps, ts.UTC())
		for i := 0; i < sensorCount; i++ {
			series[i].Avg = append(series[i].Avg, float32(avgVals[i]))
			series[i].Min = append(series[i].Min, minVals[i])
			series[i].Max = append(series[i].Max, maxVals[i])
		}
	}
	if err := rows.Err(); err != nil {
		return domain.SeriesResult{}, err
	}

	return domain.SeriesResult{
		DeviceID:   query.DeviceID,
		Resolution: query.Resolution,
		From:       query.Range.From,
		To:         query.Range.To,
		Timestamps: timestamps,
		Series:     series,
	}, nil
}

func (r *Repository) ListDevices(ctx context.Context, search domain.DeviceSearch) ([]domain.DeviceSummary, error) {
	trimmed := strings.TrimSpace(search.Query)
	var (
		rows driver.Rows
		err  error
	)
	if trimmed == "" {
		rows, err = r.conn.Query(ctx,
			"SELECT device_id, max(last_seen) AS last_seen FROM devices_registry GROUP BY device_id ORDER BY device_id ASC LIMIT ?",
			search.Limit,
		)
	} else {
		rows, err = r.conn.Query(ctx,
			"SELECT device_id, max(last_seen) AS last_seen FROM devices_registry WHERE positionCaseInsensitive(device_id, ?) > 0 GROUP BY device_id ORDER BY device_id ASC LIMIT ?",
			trimmed, search.Limit,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("list devices: %w", err)
	}
	defer rows.Close()

	devices := make([]domain.DeviceSummary, 0, 64)
	for rows.Next() {
		var rawID string
		var lastSeen time.Time
		if err := rows.Scan(&rawID, &lastSeen); err != nil {
			return nil, err
		}
		if id, idErr := domain.NewDeviceID(rawID); idErr == nil {
			devices = append(devices, domain.DeviceSummary{ID: id, LastSeen: lastSeen.UTC()})
		}
	}
	return devices, rows.Err()
}
