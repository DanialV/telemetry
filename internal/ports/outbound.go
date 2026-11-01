package ports

import (
	"context"
	"time"

	"github.com/danialv/telemetry/internal/domain"
)

type StreamPublisher interface {
	Publish(ctx context.Context, samples []domain.Sample) error
}

type SampleWriter interface {
	WriteBatch(ctx context.Context, samples []domain.Sample) error
}

type SampleReader interface {
	ReadSeries(ctx context.Context, query domain.SeriesQuery) (domain.SeriesResult, error)
	ListDevices(ctx context.Context, search domain.DeviceSearch) ([]domain.DeviceSummary, error)
}

type MetricsRecorder interface {
	RecordIngested(samples int, duration time.Duration)
	RecordIngestError()
	RecordPersisted(samples int, duration time.Duration)
	RecordPersistError()
	RecordQuery(resolution domain.Resolution, duration time.Duration)
	RecordQueryError()
}
