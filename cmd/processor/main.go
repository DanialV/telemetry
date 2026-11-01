package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v3"

	"github.com/danialv/telemetry/internal/adapter/inbound/natssub"
	"github.com/danialv/telemetry/internal/adapter/outbound/clickhouse"
	"github.com/danialv/telemetry/internal/adapter/outbound/metrics"
	"github.com/danialv/telemetry/internal/usecase"
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
	processSvc := usecase.NewProcessBatchService(repo, recorder)

	subCfg := natssub.DefaultConfig(envOr("NATS_URL", "nats://localhost:4222"))
	subCfg.Workers = envIntOr("PROCESSOR_WORKERS", subCfg.Workers)
	subCfg.FetchBatch = envIntOr("PROCESSOR_FETCH_BATCH", subCfg.FetchBatch)

	var consumer *natssub.Consumer
	for {
		var err error
		consumer, err = natssub.NewConsumer(ctx, subCfg, processSvc, logger)
		if err == nil {
			break
		}
		logger.Warn("waiting for nats consumer setup", "error", err)
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return
		}
	}
	defer consumer.Close()

	app := fiber.New(fiber.Config{ReadTimeout: 5 * time.Second})
	app.Get("/metrics", recorder.Handler())
	app.Get("/healthz", func(c fiber.Ctx) error {
		return c.Status(fiber.StatusOK).JSON(fiber.Map{"status": "ok"})
	})

	metricsAddr := envOr("METRICS_ADDR", ":8083")
	var httpWG sync.WaitGroup
	httpWG.Add(1)
	go func() {
		defer httpWG.Done()
		if err := app.Listen(metricsAddr, fiber.ListenConfig{
			GracefulContext:       ctx,
			ShutdownTimeout:       5 * time.Second,
			DisableStartupMessage: true,
			ListenerNetwork:       fiber.NetworkTCP,
		}); err != nil {
			logger.Error("processor metrics http error", "error", err)
		}
	}()

	logger.Info("processor started", "workers", subCfg.Workers, "fetch_batch", subCfg.FetchBatch)
	_ = consumer.Run(ctx)
	stop()
	httpWG.Wait()
	logger.Info("processor shut down cleanly")
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
