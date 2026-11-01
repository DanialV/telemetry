package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type backfillTask struct {
	deviceID  string
	deviceIdx int
	startUnix int64
	endUnix   int64
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	ingestURL := flag.String("ingest-url", envOr("INGEST_URL", "http://localhost:8082/api/v1/telemetry/batch"), "ingestor batch endpoint")
	devices := flag.Int("devices", envIntOr("SIM_DEVICES", 15), "number of simulated devices")
	backfillDays := flag.Int("backfill-days", envIntOr("SIM_BACKFILL_DAYS", 3), "days of historical data for all devices")
	yearDevices := flag.Int("year-devices", envIntOr("SIM_YEAR_DEVICES", 1), "subset of devices backfilled for 365 days")
	backfillWorkers := flag.Int("backfill-workers", envIntOr("SIM_BACKFILL_WORKERS", 12), "concurrent backfill workers")
	liveOnly := flag.Bool("live-only", false, "skip historical backfill")
	backfillOnly := flag.Bool("backfill-only", false, "exit after historical backfill completes")
	flag.Parse()

	client := &http.Client{
		Timeout: 20 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        256,
			MaxIdleConnsPerHost: 256,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	nowUnix := time.Now().UTC().Unix()
	yearOrigin := nowUnix - 365*86400

	var liveWg sync.WaitGroup
	if !*backfillOnly {
		liveWg.Add(1)
		go func() {
			defer liveWg.Done()
			runLiveLoop(ctx, client, *ingestURL, *devices, yearOrigin, logger)
		}()
	}

	if !*liveOnly && *backfillDays > 0 {
		runBackfill(ctx, client, *ingestURL, *devices, *backfillDays, *yearDevices, *backfillWorkers, nowUnix, yearOrigin, logger)
	}

	if *backfillOnly {
		return
	}

	<-ctx.Done()
	liveWg.Wait()
}

func runLiveLoop(ctx context.Context, client *http.Client, url string, devices int, yearOrigin int64, logger *slog.Logger) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	const devicesPerFrame = 500
	logger.Info("live telemetry streaming started", "devices", devices)

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			ts := now.UTC().Unix()
			for start := 0; start < devices; start += devicesPerFrame {
				end := min(start+devicesPerFrame, devices)
				buf := make([]byte, 0, (end-start)*160+32)
				buf = append(buf, `{"samples":[`...)
				for idx := start; idx < end; idx++ {
					if idx > start {
						buf = append(buf, ',')
					}
					devID := fmt.Sprintf("server-%04d", idx+1)
					vals := synthesizeReadings(idx, ts, yearOrigin)
					buf = appendSampleJSON(buf, devID, ts, vals)
				}
				buf = append(buf, `]}`...)
				if err := postWithRetry(ctx, client, url, buf, end-start >= 100); err != nil && ctx.Err() == nil {
					logger.Warn("live tick publish error", "error", err)
				}
			}
		}
	}
}

func runBackfill(ctx context.Context, client *http.Client, url string, devices, backfillDays, yearDevices, workers int, nowUnix, yearOrigin int64, logger *slog.Logger) {
	const chunkSeconds = int64(3600)
	taskCh := make(chan backfillTask, 1024)
	var persisted atomic.Uint64
	started := time.Now()

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 0, 600*1024)
			for task := range taskCh {
				if ctx.Err() != nil {
					return
				}
				buf = appendDeviceBatchJSON(buf, task.deviceID, task.deviceIdx, task.startUnix, task.endUnix, yearOrigin)
				if err := postWithRetry(ctx, client, url, buf, true); err != nil {
					if ctx.Err() == nil {
						logger.Warn("backfill chunk failed", "device", task.deviceID, "error", err)
					}
					continue
				}
				persisted.Add(uint64(task.endUnix - task.startUnix + 1))
			}
		}()
	}

	logger.Info("starting historical backfill", "devices", devices, "days", backfillDays, "year_devices", yearDevices)

	go func() {
		defer close(taskCh)
		for idx := 0; idx < devices; idx++ {
			days := backfillDays
			if idx < yearDevices {
				days = 365
			}
			devID := fmt.Sprintf("server-%04d", idx+1)
			oldest := nowUnix - int64(days)*86400
			for end := nowUnix - 1; end >= oldest; end -= chunkSeconds {
				start := max(oldest, end-chunkSeconds+1)
				select {
				case taskCh <- backfillTask{deviceID: devID, deviceIdx: idx, startUnix: start, endUnix: end}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	wg.Wait()
	logger.Info("historical backfill completed",
		"samples", persisted.Load(),
		"elapsed_sec", fmt.Sprintf("%.2f", time.Since(started).Seconds()),
	)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envIntOr(key string, fallback int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil && v >= 0 {
		return v
	}
	return fallback
}
