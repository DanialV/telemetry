package httpquery

import (
	"strings"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/compress"

	"github.com/danialv/telemetry/internal/domain"
	"github.com/danialv/telemetry/internal/ports"
)

type Handler struct {
	querySvc ports.TelemetryQuery
	uiHTML   []byte
}

func NewHandler(querySvc ports.TelemetryQuery, uiHTML []byte) *Handler {
	return &Handler{
		querySvc: querySvc,
		uiHTML:   uiHTML,
	}
}

func (h *Handler) RegisterRoutes(app *fiber.App) {
	app.Get("/", h.handleIndex)
	app.Get("/healthz", h.handleHealth)
	app.Get("/api/v1/devices", h.handleDevices)
	app.Get("/api/v1/series", compress.New(), h.handleSeries)
}

func (h *Handler) handleIndex(c fiber.Ctx) error {
	c.Set(fiber.HeaderContentType, "text/html; charset=utf-8")
	return c.Status(fiber.StatusOK).Send(h.uiHTML)
}

func (h *Handler) handleHealth(c fiber.Ctx) error {
	return c.Status(fiber.StatusOK).JSON(fiber.Map{"status": "ok"})
}

type deviceDTO struct {
	ID       string `json:"id"`
	LastSeen int64  `json:"last_seen"`
}

func (h *Handler) handleDevices(c fiber.Ctx) error {
	q := strings.Clone(c.Query("q"))
	limit := fiber.Query[int](c, "limit", 0)
	search := domain.NewDeviceSearch(q, limit)

	devices, err := h.querySvc.SearchDevices(c.Context(), search)
	if err != nil {
		return writeError(c, fiber.StatusInternalServerError, "failed to search devices")
	}

	out := make([]deviceDTO, len(devices))
	for i, d := range devices {
		out[i] = deviceDTO{
			ID:       d.ID.String(),
			LastSeen: d.LastSeen.Unix(),
		}
	}

	return c.JSON(fiber.Map{"devices": out})
}

func writeError(c fiber.Ctx, status int, message string) error {
	return c.Status(status).JSON(fiber.Map{"error": message})
}
