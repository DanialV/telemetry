package usecase_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/danialv/telemetry/internal/domain"
	"github.com/danialv/telemetry/internal/usecase"
)

type spyPublisher struct {
	published []domain.Sample
	err       error
}

func (s *spyPublisher) Publish(_ context.Context, samples []domain.Sample) error {
	if s.err != nil {
		return s.err
	}
	s.published = append(s.published, samples...)
	return nil
}

type spyWriter struct {
	written []domain.Sample
}

func (s *spyWriter) WriteBatch(_ context.Context, samples []domain.Sample) error {
	s.written = append(s.written, samples...)
	return nil
}

type spyReader struct {
	lastQuery domain.SeriesQuery
}

func (s *spyReader) ReadSeries(_ context.Context, q domain.SeriesQuery) (domain.SeriesResult, error) {
	s.lastQuery = q
	return domain.SeriesResult{
		DeviceID:   q.DeviceID,
		Resolution: q.Resolution,
		From:       q.Range.From,
		To:         q.Range.To,
	}, nil
}

func (s *spyReader) ListDevices(_ context.Context, _ domain.DeviceSearch) ([]domain.DeviceSummary, error) {
	return []domain.DeviceSummary{{ID: "srv-01", LastSeen: time.Unix(1730000000, 0).UTC()}}, nil
}

func makeSample(t *testing.T, id string, ts int64) domain.Sample {
	t.Helper()
	sample, err := domain.NewSample(id, time.Unix(ts, 0), domain.RawReadings{
		CPU: 20, Memory: 40, DiskSpace: 50, DiskBusy: 10, NetRx: 5, NetTx: 5, Fan: 35,
	})
	if err != nil {
		t.Fatal(err)
	}
	return sample
}

func TestIngestServiceRejectsEmptyBatch(t *testing.T) {
	pub := &spyPublisher{}
	svc := usecase.NewIngestService(pub, nil)
	if err := svc.Ingest(context.Background(), nil); !errors.Is(err, domain.ErrEmptyBatch) {
		t.Fatalf("expected ErrEmptyBatch, got %v", err)
	}
}

func TestProcessBatchDeterministicOrdering(t *testing.T) {
	writer := &spyWriter{}
	svc := usecase.NewProcessBatchService(writer, nil)

	batch := []domain.Sample{
		makeSample(t, "srv-02", 1730000010),
		makeSample(t, "srv-01", 1730000020),
		makeSample(t, "srv-01", 1730000010),
	}

	if err := svc.Process(context.Background(), batch); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(writer.written) != 3 {
		t.Fatalf("expected 3 samples written, got %d", len(writer.written))
	}
	if writer.written[0].DeviceID != "srv-01" || writer.written[0].Timestamp.Unix() != 1730000010 {
		t.Fatalf("unexpected first sample: %+v", writer.written[0])
	}
	if writer.written[1].DeviceID != "srv-01" || writer.written[1].Timestamp.Unix() != 1730000020 {
		t.Fatalf("unexpected second sample: %+v", writer.written[1])
	}
	if writer.written[2].DeviceID != "srv-02" {
		t.Fatalf("unexpected third sample: %+v", writer.written[2])
	}
}

func TestQueryServiceForwardsResolution(t *testing.T) {
	reader := &spyReader{}
	svc := usecase.NewQueryService(reader, nil)

	to := time.Unix(1730000000, 0).UTC()
	from := to.Add(-365 * 24 * time.Hour)
	q, err := domain.NewSeriesQuery("srv-01", []string{"cpu", "memory"}, from, to)
	if err != nil {
		t.Fatal(err)
	}

	res, err := svc.QuerySeries(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	if res.Resolution != domain.Resolution1h {
		t.Fatalf("expected 1h resolution, got %s", res.Resolution)
	}
}
