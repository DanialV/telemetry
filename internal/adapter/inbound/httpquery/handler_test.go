package httpquery_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"

	"github.com/danialv/telemetry/internal/adapter/inbound/httpquery"
	"github.com/danialv/telemetry/internal/domain"
	"github.com/danialv/telemetry/web"
)

type stubQuery struct{}

func (stubQuery) QuerySeries(_ context.Context, q domain.SeriesQuery) (domain.SeriesResult, error) {
	return domain.SeriesResult{
		DeviceID:   q.DeviceID,
		Resolution: q.Resolution,
		From:       q.Range.From,
		To:         q.Range.To,
		Timestamps: []time.Time{q.Range.From},
		Series: []domain.SensorSeries{
			{Sensor: q.Sensors[0], Avg: []float32{45.5}, Min: []float32{20.0}, Max: []float32{80.0}},
		},
	}, nil
}

func (stubQuery) SearchDevices(_ context.Context, _ domain.DeviceSearch) ([]domain.DeviceSummary, error) {
	return []domain.DeviceSummary{
		{ID: "server-0001", LastSeen: time.Unix(1730000000, 0).UTC()},
	}, nil
}

func TestHTTPQueryUIAndEndpoints(t *testing.T) {
	app := fiber.New()
	httpquery.NewHandler(stubQuery{}, web.IndexHTML).RegisterRoutes(app)

	uiReq := httptest.NewRequest(http.MethodGet, "/", nil)
	uiResp, err := app.Test(uiReq)
	if err != nil {
		t.Fatal(err)
	}
	uiBody, _ := io.ReadAll(uiResp.Body)
	_ = uiResp.Body.Close()
	if uiResp.StatusCode != http.StatusOK || !strings.Contains(string(uiBody), "Server Sensor Telemetry") {
		t.Fatalf("expected HTML UI on /, got %d", uiResp.StatusCode)
	}

	devReq := httptest.NewRequest(http.MethodGet, "/api/v1/devices?q=server", nil)
	devResp, err := app.Test(devReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = devResp.Body.Close()
	if devResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 on devices, got %d", devResp.StatusCode)
	}

	seriesReq := httptest.NewRequest(http.MethodGet, "/api/v1/series?device_id=server-0001&sensors=cpu,memory&range=1y", nil)
	seriesResp, err := app.Test(seriesReq)
	if err != nil {
		t.Fatal(err)
	}
	seriesBody, _ := io.ReadAll(seriesResp.Body)
	_ = seriesResp.Body.Close()
	if seriesResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 on series, got %d (%s)", seriesResp.StatusCode, string(seriesBody))
	}

	var decoded struct {
		Resolution string `json:"resolution"`
	}
	if err := json.Unmarshal(seriesBody, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Resolution != "1h" {
		t.Fatalf("expected 1h resolution for 1y preset, got %s", decoded.Resolution)
	}
}
