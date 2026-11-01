package httpingest

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"strings"
	"sync"

	"github.com/gofiber/fiber/v3"

	"github.com/danialv/telemetry/internal/domain"
	"github.com/danialv/telemetry/internal/ports"
)

const MaxRequestBodyBytes = 32 << 20

type Handler struct {
	ingestor ports.TelemetryIngestor
	gzipPool sync.Pool
}

func NewHandler(ingestor ports.TelemetryIngestor) *Handler {
	return &Handler{ingestor: ingestor}
}

func (h *Handler) RegisterRoutes(app *fiber.App) {
	app.Post("/api/v1/telemetry", h.handleTelemetry)
	app.Post("/api/v1/telemetry/batch", h.handleTelemetry)
	app.Get("/healthz", h.handleHealth)
}

func (h *Handler) handleHealth(c fiber.Ctx) error {
	return c.Status(fiber.StatusOK).JSON(fiber.Map{"status": "ok"})
}

func (h *Handler) handleTelemetry(c fiber.Ctx) error {
	payload := c.BodyRaw()
	if strings.EqualFold(strings.TrimSpace(c.Get(fiber.HeaderContentEncoding)), "gzip") {
		rawReader := bytes.NewReader(payload)
		var gz *gzip.Reader
		if pooled := h.gzipPool.Get(); pooled != nil {
			gz = pooled.(*gzip.Reader)
			if err := gz.Reset(rawReader); err != nil {
				return writeJSONError(c, fiber.StatusBadRequest, "invalid gzip payload")
			}
		} else {
			var err error
			gz, err = gzip.NewReader(rawReader)
			if err != nil {
				return writeJSONError(c, fiber.StatusBadRequest, "invalid gzip payload")
			}
		}
		decompressed, err := io.ReadAll(io.LimitReader(gz, MaxRequestBodyBytes+1))
		_ = gz.Close()
		h.gzipPool.Put(gz)
		if err != nil {
			return writeJSONError(c, fiber.StatusBadRequest, "failed to read gzip payload")
		}
		payload = decompressed
	}

	if len(payload) > MaxRequestBodyBytes {
		return writeJSONError(c, fiber.StatusRequestEntityTooLarge, "payload exceeds size limit")
	}

	samples, err := decodeSamplesPayload(payload)
	if err != nil {
		return writeJSONError(c, fiber.StatusBadRequest, err.Error())
	}

	if err := h.ingestor.Ingest(c.Context(), samples); err != nil {
		switch {
		case errors.Is(err, domain.ErrBackpressure):
			return writeJSONError(c, fiber.StatusServiceUnavailable, err.Error())
		case errors.Is(err, domain.ErrEmptyBatch),
			errors.Is(err, domain.ErrInvalidDeviceID),
			errors.Is(err, domain.ErrInvalidTimestamp),
			errors.Is(err, domain.ErrPercentageOutOfRange):
			return writeJSONError(c, fiber.StatusBadRequest, err.Error())
		default:
			return writeJSONError(c, fiber.StatusServiceUnavailable, "temporary ingestion failure")
		}
	}

	return c.Status(fiber.StatusAccepted).JSON(fiber.Map{"accepted": len(samples)})
}

func writeJSONError(c fiber.Ctx, status int, message string) error {
	return c.Status(status).JSON(fiber.Map{"error": message})
}
