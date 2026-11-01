package usecase

import (
	"cmp"
	"context"
	"slices"
	"time"

	"github.com/danialv/telemetry/internal/domain"
	"github.com/danialv/telemetry/internal/ports"
)

type ProcessBatchService struct {
	writer  ports.SampleWriter
	metrics ports.MetricsRecorder
}

func NewProcessBatchService(writer ports.SampleWriter, metrics ports.MetricsRecorder) *ProcessBatchService {
	return &ProcessBatchService{
		writer:  writer,
		metrics: metrics,
	}
}

func (s *ProcessBatchService) Process(ctx context.Context, samples []domain.Sample) error {
	if len(samples) == 0 {
		return nil
	}
	ordered := slices.Clone(samples)
	slices.SortFunc(ordered, func(a, b domain.Sample) int {
		if diff := cmp.Compare(a.DeviceID, b.DeviceID); diff != 0 {
			return diff
		}
		return a.Timestamp.Compare(b.Timestamp)
	})

	started := time.Now()
	if err := s.writer.WriteBatch(ctx, ordered); err != nil {
		if s.metrics != nil {
			s.metrics.RecordPersistError()
		}
		return err
	}
	if s.metrics != nil {
		s.metrics.RecordPersisted(len(ordered), time.Since(started))
	}
	return nil
}
