package usecase

import (
	"context"
	"time"

	"github.com/danialv/telemetry/internal/domain"
	"github.com/danialv/telemetry/internal/ports"
)

type QueryService struct {
	reader  ports.SampleReader
	metrics ports.MetricsRecorder
}

func NewQueryService(reader ports.SampleReader, metrics ports.MetricsRecorder) *QueryService {
	return &QueryService{
		reader:  reader,
		metrics: metrics,
	}
}

func (s *QueryService) QuerySeries(ctx context.Context, query domain.SeriesQuery) (domain.SeriesResult, error) {
	started := time.Now()
	result, err := s.reader.ReadSeries(ctx, query)
	if err != nil {
		if s.metrics != nil {
			s.metrics.RecordQueryError()
		}
		return domain.SeriesResult{}, err
	}
	if s.metrics != nil {
		s.metrics.RecordQuery(query.Resolution, time.Since(started))
	}
	return result, nil
}

func (s *QueryService) SearchDevices(ctx context.Context, search domain.DeviceSearch) ([]domain.DeviceSummary, error) {
	return s.reader.ListDevices(ctx, search)
}
