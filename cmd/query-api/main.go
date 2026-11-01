package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v3"

	"github.com/danialv/telemetry/internal/adapter/inbound/httpquery"
	"github.com/danialv/telemetry/internal/adapter/outbound/clickhouse"
	"github.com/danialv/telemetry/internal/adapter/outbound/metrics"
	"github.com/danialv/telemetry/internal/usecase"
	"github.com/danialv/telemetry/web"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	chCfg := clickhouse.Config{
		Addr:     envOr("CLICKHOUSE_ADDR", "localhost:9000"),
		Database: envOr("CLICKHOUSE_DB", "default"),
		Username: envOr("CLICKHOUSE_USER", "default"),
		Password: os.Getenv("CLICKHOUSE_PASSWORD"),
	}

	var repo *clickhouse.Repository
	for {
		var err error
		repo, err = clickhouse.NewRepository(ctx, chCfg)
		if err == nil {
			err = repo.Migrate(ctx)
		}
		if err == nil {
			break
		}
		logger.Warn("waiting for clickhouse", "error", err)
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return
		}
	}
	defer repo.Close()

	recorder := metrics.NewRecorder()
	querySvc := usecase.NewQueryService(repo, recorder)

	app := fiber.New(fiber.Config{
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	})
	httpquery.NewHandler(querySvc, web.IndexHTML).RegisterRoutes(app)
	app.Get("/metrics", recorder.Handler())

	addr := envOr("HTTP_ADDR", ":8084")
	logger.Info("query-api listening", "addr", addr)
	if err := app.Listen(addr, fiber.ListenConfig{
		GracefulContext:       ctx,
		ShutdownTimeout:       10 * time.Second,
		DisableStartupMessage: true,
		ListenerNetwork:       fiber.NetworkTCP,
	}); err != nil {
		logger.Error("query-api http error", "error", err)
	}
	logger.Info("query-api shut down cleanly")
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
