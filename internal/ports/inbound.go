package ports

import (
	"context"

	"github.com/danialv/telemetry/internal/domain"
)

type TelemetryIngestor interface {
	Ingest(ctx context.Context, samples []domain.Sample) error
}

type BatchProcessor interface {
	Process(ctx context.Context, samples []domain.Sample) error
}

type TelemetryQuery interface {
	QuerySeries(ctx context.Context, query domain.SeriesQuery) (domain.SeriesResult, error)
	SearchDevices(ctx context.Context, search domain.DeviceSearch) ([]domain.DeviceSummary, error)
}
