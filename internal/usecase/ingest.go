package usecase

import (
	"context"
	"time"

	"github.com/danialv/telemetry/internal/domain"
	"github.com/danialv/telemetry/internal/ports"
)

type IngestService struct {
	publisher ports.StreamPublisher
	metrics   ports.MetricsRecorder
}

func NewIngestService(publisher ports.StreamPublisher, metrics ports.MetricsRecorder) *IngestService {
	return &IngestService{
		publisher: publisher,
		metrics:   metrics,
	}
}

func (s *IngestService) Ingest(ctx context.Context, samples []domain.Sample) error {
	if len(samples) == 0 {
		return domain.ErrEmptyBatch
	}
	started := time.Now()
	if err := s.publisher.Publish(ctx, samples); err != nil {
		if s.metrics != nil {
			s.metrics.RecordIngestError()
		}
		return err
	}
	if s.metrics != nil {
		s.metrics.RecordIngested(len(samples), time.Since(started))
	}
	return nil
}
