package httpquery

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v3"

	"github.com/danialv/telemetry/internal/domain"
)

type seriesItemDTO struct {
	Sensor string    `json:"sensor"`
	Avg    []float32 `json:"avg"`
	Min    []float32 `json:"min"`
	Max    []float32 `json:"max"`
}

type seriesResponseDTO struct {
	DeviceID   string          `json:"device_id"`
	Resolution string          `json:"resolution"`
	From       int64           `json:"from"`
	To         int64           `json:"to"`
	QueryMs    float64         `json:"query_ms"`
	Timestamps []int64         `json:"timestamps"`
	Series     []seriesItemDTO `json:"series"`
}

var presetDurations = map[string]time.Duration{
	"5m":  5 * time.Minute,
	"15m": 15 * time.Minute,
	"1h":  time.Hour,
	"6h":  6 * time.Hour,
	"24h": 24 * time.Hour,
	"7d":  7 * 24 * time.Hour,
	"30d": 30 * 24 * time.Hour,
	"90d": 90 * 24 * time.Hour,
	"1y":  365 * 24 * time.Hour,
}

func (h *Handler) handleSeries(c fiber.Ctx) error {
	from, to, err := resolveTimeWindow(c.Query("range"), c.Query("from"), c.Query("to"))
	if err != nil {
		return writeError(c, fiber.StatusBadRequest, err.Error())
	}

	var rawSensors []string
	for _, p := range strings.Split(c.Query("sensors"), ",") {
		if t := strings.TrimSpace(p); t != "" {
			rawSensors = append(rawSensors, strings.Clone(t))
		}
	}

	query, err := domain.NewSeriesQuery(strings.Clone(c.Query("device_id")), rawSensors, from, to)
	if err != nil {
		return writeError(c, fiber.StatusBadRequest, err.Error())
	}

	started := time.Now()
	result, err := h.querySvc.QuerySeries(c.Context(), query)
	if err != nil {
		return writeError(c, fiber.StatusInternalServerError, "failed to query telemetry series")
	}
	elapsedMs := float64(time.Since(started).Microseconds()) / 1000.0

	unixTimestamps := make([]int64, len(result.Timestamps))
	for i, ts := range result.Timestamps {
		unixTimestamps[i] = ts.Unix()
	}

	items := make([]seriesItemDTO, len(result.Series))
	for i, s := range result.Series {
		items[i] = seriesItemDTO{
			Sensor: string(s.Sensor),
			Avg:    s.Avg,
			Min:    s.Min,
			Max:    s.Max,
		}
	}

	return c.JSON(seriesResponseDTO{
		DeviceID:   result.DeviceID.String(),
		Resolution: string(result.Resolution),
		From:       result.From.Unix(),
		To:         result.To.Unix(),
		QueryMs:    elapsedMs,
		Timestamps: unixTimestamps,
		Series:     items,
	})
}

func resolveTimeWindow(preset, rawFrom, rawTo string) (time.Time, time.Time, error) {
	if preset != "" {
		dur, ok := presetDurations[strings.TrimSpace(preset)]
		if !ok {
			return time.Time{}, time.Time{}, errors.New("unsupported range preset")
		}
		now := time.Now().UTC()
		return now.Add(-dur), now, nil
	}
	if rawFrom == "" || rawTo == "" {
		return time.Time{}, time.Time{}, errors.New("either range or both from and to parameters are required")
	}
	parseTS := func(raw string) (time.Time, error) {
		if unixSec, err := strconv.ParseInt(raw, 10, 64); err == nil {
			return time.Unix(unixSec, 0).UTC(), nil
		}
		return time.Parse(time.RFC3339, raw)
	}
	from, err := parseTS(rawFrom)
	if err != nil {
		return time.Time{}, time.Time{}, errors.New("invalid from timestamp")
	}
	to, err := parseTS(rawTo)
	if err != nil {
		return time.Time{}, time.Time{}, errors.New("invalid to timestamp")
	}
	return from, to, nil
}
