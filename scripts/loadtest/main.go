package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type stepResult struct {
	Devices           int                       `json:"devices"`
	DurationSec       float64                   `json:"duration_sec"`
	SentSamples       uint64                    `json:"sent_samples"`
	HTTPErrors        uint64                    `json:"http_errors"`
	IngestRatePerSec  float64                   `json:"ingest_rate_per_sec"`
	PersistRatePerSec float64                   `json:"persist_rate_per_sec"`
	QueueLagSamples   int64                     `json:"queue_lag_samples"`
	Queries           map[string]latencySummary `json:"queries"`
	Resources         map[string]containerUsage `json:"resources"`
	Sustainable       bool                      `json:"sustainable"`
	FailureReason     string                    `json:"failure_reason,omitempty"`
}

func main() {
	ingestBase := flag.String("ingest-url", envOr("INGEST_BASE_URL", "http://localhost:8082"), "ingestor base url")
	procBase := flag.String("processor-url", envOr("PROCESSOR_BASE_URL", "http://localhost:8083"), "processor base url")
	queryBase := flag.String("query-url", envOr("QUERY_BASE_URL", "http://localhost:8084"), "query-api base url")
	stepsFlag := flag.String("steps", "1000,3000,6000,10000,15000,20000,28000,36000", "comma-separated device counts")
	stepDuration := flag.Duration("step-duration", 15*time.Second, "measurement window per step")
	batchPerReq := flag.Int("batch-per-req", 1, "samples per HTTP request (1 = realistic 1 req/sec per device)")
	workers := flag.Int("workers", 256, "concurrent HTTP load workers")
	outDir := flag.String("out-dir", "docs/graphs", "directory for output graphs and raw data")
	fromJSON := flag.String("from-json", "", "regenerate SVG charts from an existing results.json file")
	flag.Parse()

	if *fromJSON != "" {
		raw, err := os.ReadFile(*fromJSON)
		if err != nil {
			fmt.Fprintf(os.Stderr, "read json: %v\n", err)
			os.Exit(1)
		}
		var saved []stepResult
		if err := json.Unmarshal(raw, &saved); err != nil {
			fmt.Fprintf(os.Stderr, "parse json: %v\n", err)
			os.Exit(1)
		}
		if err := generateAllCharts(*outDir, saved); err != nil {
			fmt.Fprintf(os.Stderr, "generate charts: %v\n", err)
			os.Exit(1)
		}
		return
	}

	steps := parseSteps(*stepsFlag)
	client := &http.Client{Timeout: 5 * time.Second}

	fmt.Printf("Starting step-ramp load test (step=%v, batch_per_req=%d, workers=%d)\n", *stepDuration, *batchPerReq, *workers)
	fmt.Println(strings.Repeat("-", 104))
	fmt.Printf("%-8s | %-11s | %-11s | %-7s | %-9s | %-9s | %-9s | %-6s\n",
		"Devices", "Ingest/s", "Persist/s", "HTTPErr", "5m p95", "1y p50", "1y p95", "Status")
	fmt.Println(strings.Repeat("-", 104))

	results := make([]stepResult, 0, len(steps))
	maxSustainable := 0

	for _, devCount := range steps {
		beforeIng, _ := scrapeMetrics(client, *ingestBase+"/metrics")
		beforeProc, _ := scrapeMetrics(client, *procBase+"/metrics")

		stepCtx, cancel := context.WithTimeout(context.Background(), *stepDuration)
		started := time.Now()

		var (
			outcome loadOutcome
			queries map[string]latencySummary
			stats   map[string]containerUsage
			wg      sync.WaitGroup
		)

		wg.Add(3)
		go func() {
			defer wg.Done()
			outcome = runWriteStorm(stepCtx, *ingestBase+"/api/v1/telemetry", devCount, *batchPerReq, *workers)
		}()
		go func() {
			defer wg.Done()
			queries = runQueryProbes(stepCtx, *queryBase)
		}()
		go func() {
			defer wg.Done()
			stats = sampleDockerStats(stepCtx)
		}()

		wg.Wait()
		cancel()
		elapsed := time.Since(started).Seconds()

		time.Sleep(1500 * time.Millisecond)
		afterIng, _ := scrapeMetrics(client, *ingestBase+"/metrics")
		afterProc, _ := scrapeMetrics(client, *procBase+"/metrics")

		ingDelta := afterIng.IngestedSamples - beforeIng.IngestedSamples
		persDelta := afterProc.PersistedSamples - beforeProc.PersistedSamples
		lag := int64(afterIng.IngestedSamples) - int64(afterProc.PersistedSamples)

		ingRate := float64(ingDelta) / elapsed
		persRate := float64(persDelta) / elapsed

		sustainable, reason := evaluateStep(devCount, outcome, ingRate, persRate, queries)
		if sustainable {
			maxSustainable = devCount
		}

		res := stepResult{
			Devices:           devCount,
			DurationSec:       elapsed,
			SentSamples:       outcome.sentSamples,
			HTTPErrors:        outcome.httpErrors,
			IngestRatePerSec:  ingRate,
			PersistRatePerSec: persRate,
			QueueLagSamples:   lag,
			Queries:           queries,
			Resources:         stats,
			Sustainable:       sustainable,
			FailureReason:     reason,
		}
		results = append(results, res)

		statusStr := "PASS"
		if !sustainable {
			statusStr = "LIMIT (" + reason + ")"
		}
		fmt.Printf("%-8d | %11.0f | %11.0f | %7d | %7.1fms | %7.1fms | %7.1fms | %s\n",
			devCount, ingRate, persRate, outcome.httpErrors,
			queries["5m"].P95Ms, queries["1y"].P50Ms, queries["1y"].P95Ms, statusStr)

		time.Sleep(2 * time.Second)
	}

	fmt.Println(strings.Repeat("-", 104))
	fmt.Printf("Max sustainable devices on current hardware: %d\n", maxSustainable)

	if err := saveRawResults(*outDir, results); err != nil {
		fmt.Fprintf(os.Stderr, "failed saving raw results: %v\n", err)
		os.Exit(1)
	}
	if err := generateAllCharts(*outDir, results); err != nil {
		fmt.Fprintf(os.Stderr, "failed generating SVG charts: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Artifacts written to %s (results.json, results.csv, write_rate.svg, query_latency.svg, resource_usage.svg)\n", *outDir)
}

func evaluateStep(devices int, outcome loadOutcome, ingRate, persRate float64, queries map[string]latencySummary) (bool, string) {
	if outcome.httpErrors > uint64(devices/100+5) {
		return false, "http_ingest_errors"
	}
	if ingRate < float64(devices)*0.90 {
		return false, "ingest_throughput_lag"
	}
	if persRate < ingRate*0.92 {
		return false, "clickhouse_persist_lag"
	}
	if queries["1y"].P95Ms >= 1000.0 || queries["5m"].P95Ms >= 1000.0 {
		return false, "query_slo_breach"
	}
	if queries["1y"].Errs > 0 {
		return false, "query_errors"
	}
	return true, ""
}

func parseSteps(raw string) []int {
	var out []int
	for _, p := range strings.Split(raw, ",") {
		if v, err := strconv.Atoi(strings.TrimSpace(p)); err == nil && v > 0 {
			out = append(out, v)
		}
	}
	return out
}

func saveRawResults(outDir string, results []stepResult) error {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	jsonBytes, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(outDir, "results.json"), jsonBytes, 0o644); err != nil {
		return err
	}

	f, err := os.Create(filepath.Join(outDir, "results.csv"))
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	_ = w.Write([]string{
		"devices", "ingest_rate_s", "persist_rate_s", "http_errors", "queue_lag",
		"q_5m_p95_ms", "q_24h_p95_ms", "q_1y_p50_ms", "q_1y_p95_ms",
		"ingestor_cpu_pct", "processor_cpu_pct", "nats_cpu_pct", "clickhouse_cpu_pct",
		"clickhouse_mem_mib", "sustainable", "reason",
	})
	for _, r := range results {
		_ = w.Write([]string{
			strconv.Itoa(r.Devices),
			fmt.Sprintf("%.1f", r.IngestRatePerSec),
			fmt.Sprintf("%.1f", r.PersistRatePerSec),
			strconv.FormatUint(r.HTTPErrors, 10),
			strconv.FormatInt(r.QueueLagSamples, 10),
			fmt.Sprintf("%.2f", r.Queries["5m"].P95Ms),
			fmt.Sprintf("%.2f", r.Queries["24h"].P95Ms),
			fmt.Sprintf("%.2f", r.Queries["1y"].P50Ms),
			fmt.Sprintf("%.2f", r.Queries["1y"].P95Ms),
			fmt.Sprintf("%.1f", r.Resources["ingestor"].CPUPercent),
			fmt.Sprintf("%.1f", r.Resources["processor"].CPUPercent),
			fmt.Sprintf("%.1f", r.Resources["nats"].CPUPercent),
			fmt.Sprintf("%.1f", r.Resources["clickhouse"].CPUPercent),
			fmt.Sprintf("%.1f", r.Resources["clickhouse"].MemMiB),
			strconv.FormatBool(r.Sustainable),
			r.FailureReason,
		})
	}
	w.Flush()
	return w.Error()
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
