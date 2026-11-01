package httpingest_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v3"

	"github.com/danialv/telemetry/internal/adapter/inbound/httpingest"
	"github.com/danialv/telemetry/internal/domain"
)

type stubIngestor struct {
	received []domain.Sample
	err      error
}

func (s *stubIngestor) Ingest(_ context.Context, samples []domain.Sample) error {
	if s.err != nil {
		return s.err
	}
	s.received = append(s.received, samples...)
	return nil
}

func TestHTTPIngestSingleAndGzipBatch(t *testing.T) {
	ing := &stubIngestor{}
	app := fiber.New(fiber.Config{BodyLimit: httpingest.MaxRequestBodyBytes})
	httpingest.NewHandler(ing).RegisterRoutes(app)

	singleBody := `{"device_id":"srv-01","timestamp":1730000000,"cpu":20,"memory":50,"disk_space":60,"disk_busy":15,"net_rx":5,"net_tx":7,"fan":40}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/telemetry", bytes.NewBufferString(singleBody))
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	bodyBytes, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202, got %d (%s)", resp.StatusCode, string(bodyBytes))
	}
	if len(ing.received) != 1 {
		t.Fatalf("expected 1 sample, got %d", len(ing.received))
	}

	var compressed bytes.Buffer
	gz := gzip.NewWriter(&compressed)
	_, _ = gz.Write([]byte(`{"device_id":"srv-02","samples":[{"timestamp":1730000001,"cpu":30,"memory":50,"disk_space":60,"disk_busy":15,"net_rx":5,"net_tx":7,"fan":40}]}`))
	_ = gz.Close()

	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/telemetry/batch", &compressed)
	req2.Header.Set("Content-Encoding", "gzip")
	resp2, err := app.Test(req2)
	if err != nil {
		t.Fatal(err)
	}
	bodyBytes2, _ := io.ReadAll(resp2.Body)
	_ = resp2.Body.Close()

	if resp2.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202 on gzip batch, got %d (%s)", resp2.StatusCode, string(bodyBytes2))
	}
	if len(ing.received) != 2 || ing.received[1].DeviceID != "srv-02" {
		t.Fatalf("unexpected received state: %+v", ing.received)
	}

	invalidBody := `{"device_id":"srv-01","timestamp":1730000000,"cpu":150,"memory":50,"disk_space":60,"disk_busy":15,"net_rx":5,"net_tx":7,"fan":40}`
	req3 := httptest.NewRequest(http.MethodPost, "/api/v1/telemetry", bytes.NewBufferString(invalidBody))
	resp3, err := app.Test(req3)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp3.Body.Close()
	if resp3.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 on out-of-range percentage, got %d", resp3.StatusCode)
	}
}
