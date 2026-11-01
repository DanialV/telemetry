package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v3"

	"github.com/danialv/telemetry/internal/adapter/inbound/httpingest"
	"github.com/danialv/telemetry/internal/adapter/outbound/metrics"
	"github.com/danialv/telemetry/internal/adapter/outbound/natspub"
	"github.com/danialv/telemetry/internal/usecase"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pubCfg := natspub.DefaultConfig(envOr("NATS_URL", "nats://localhost:4222"))
	pubCfg.Workers = envIntOr("INGEST_WORKERS", pubCfg.Workers)
	pubCfg.MaxBatchSamples = envIntOr("INGEST_BATCH_SIZE", pubCfg.MaxBatchSamples)
	pubCfg.FlushInterval = time.Duration(envIntOr("INGEST_FLUSH_MS", 8)) * time.Millisecond

	var publisher *natspub.Publisher
	for {
		var err error
		publisher, err = natspub.NewPublisher(ctx, pubCfg)
		if err == nil {
			break
		}
		logger.Warn("waiting for nats jetstream", "error", err)
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return
		}
	}
	defer publisher.Close()

	recorder := metrics.NewRecorder()
	ingestSvc := usecase.NewIngestService(publisher, recorder)

	app := fiber.New(fiber.Config{
		BodyLimit:    httpingest.MaxRequestBodyBytes,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	})
	httpingest.NewHandler(ingestSvc).RegisterRoutes(app)
	app.Get("/metrics", recorder.Handler())

	addr := envOr("HTTP_ADDR", ":8082")
	logger.Info("ingestor listening", "addr", addr)
	if err := app.Listen(addr, fiber.ListenConfig{
		GracefulContext:       ctx,
		ShutdownTimeout:       10 * time.Second,
		DisableStartupMessage: true,
		ListenerNetwork:       fiber.NetworkTCP,
	}); err != nil {
		logger.Error("ingestor http error", "error", err)
	}
	logger.Info("ingestor shut down cleanly")
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envIntOr(key string, fallback int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil && v > 0 {
		return v
	}
	return fallback
}
