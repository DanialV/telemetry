package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

type metricsSnap struct {
	IngestedSamples  uint64 `json:"ingested_samples"`
	IngestErrors     uint64 `json:"ingest_errors"`
	PersistedSamples uint64 `json:"persisted_samples"`
	PersistErrors    uint64 `json:"persist_errors"`
}

func scrapeMetrics(client *http.Client, url string) (metricsSnap, error) {
	resp, err := client.Get(url)
	if err != nil {
		return metricsSnap{}, err
	}
	defer resp.Body.Close()
	var snap metricsSnap
	err = json.NewDecoder(resp.Body).Decode(&snap)
	return snap, err
}

type latencySummary struct {
	P50Ms float64 `json:"p50_ms"`
	P95Ms float64 `json:"p95_ms"`
	P99Ms float64 `json:"p99_ms"`
	MaxMs float64 `json:"max_ms"`
	Errs  int     `json:"errors"`
}

func runQueryProbes(ctx context.Context, queryBaseURL string) map[string]latencySummary {
	client := &http.Client{Timeout: 5 * time.Second}
	ranges := []string{"5m", "24h", "1y"}
	var mu sync.Mutex
	samples := map[string][]float64{"5m": {}, "24h": {}, "1y": {}}
	errCounts := map[string]int{}

	var wg sync.WaitGroup
	for _, rng := range ranges {
		wg.Add(1)
		go func(window string) {
			defer wg.Done()
			ticker := time.NewTicker(250 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					dev := "server-0001"
					if window != "1y" {
						dev = fmt.Sprintf("server-%04d", 1+rand.IntN(10))
					}
					url := fmt.Sprintf("%s/api/v1/series?device_id=%s&sensors=cpu,memory,disk_busy,net_rx&range=%s", queryBaseURL, dev, window)
					t0 := time.Now()
					resp, err := client.Get(url)
					ms := float64(time.Since(t0).Microseconds()) / 1000.0
					if err != nil {
						if ctx.Err() == nil {
							mu.Lock()
							errCounts[window]++
							mu.Unlock()
						}
						continue
					}
					_, _ = io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()
					mu.Lock()
					if resp.StatusCode == http.StatusOK {
						samples[window] = append(samples[window], ms)
					} else {
						errCounts[window]++
					}
					mu.Unlock()
				}
			}
		}(rng)
	}

	wg.Wait()
	out := make(map[string]latencySummary, len(ranges))
	for _, rng := range ranges {
		vals := samples[rng]
		slices.Sort(vals)
		out[rng] = latencySummary{
			P50Ms: percentile(vals, 0.50),
			P95Ms: percentile(vals, 0.95),
			P99Ms: percentile(vals, 0.99),
			MaxMs: percentile(vals, 1.00),
			Errs:  errCounts[rng],
		}
	}
	return out
}

func percentile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * q)
	return sorted[min(max(idx, 0), len(sorted)-1)]
}

type containerUsage struct {
	CPUPercent float64 `json:"cpu_percent"`
	MemMiB     float64 `json:"mem_mib"`
}

func sampleDockerStats(ctx context.Context) map[string]containerUsage {
	type acc struct {
		cpuSum float64
		memMax float64
		n      int
	}
	totals := map[string]*acc{}
	services := []string{"ingestor", "processor", "query-api", "nats", "clickhouse"}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	collectOnce := func() {
		cmd := exec.CommandContext(ctx, "docker", "stats", "--no-stream", "--format", "{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}")
		out, err := cmd.Output()
		if err != nil {
			return
		}
		for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			parts := strings.Split(line, "|")
			if len(parts) != 3 {
				continue
			}
			name := parts[0]
			matched := ""
			for _, svc := range services {
				if strings.HasSuffix(name, "-"+svc+"-1") || name == svc {
					matched = svc
					break
				}
			}
			if matched == "" {
				continue
			}
			cpu, _ := strconv.ParseFloat(strings.TrimSuffix(strings.TrimSpace(parts[1]), "%"), 64)
			memMiB := parseMemMiB(parts[2])
			item := totals[matched]
			if item == nil {
				item = &acc{}
				totals[matched] = item
			}
			item.cpuSum += cpu
			item.memMax = max(item.memMax, memMiB)
			item.n++
		}
	}

	collectOnce()
	for {
		select {
		case <-ctx.Done():
			res := make(map[string]containerUsage, len(totals))
			for k, v := range totals {
				if v.n > 0 {
					res[k] = containerUsage{CPUPercent: v.cpuSum / float64(v.n), MemMiB: v.memMax}
				}
			}
			return res
		case <-ticker.C:
			collectOnce()
		}
	}
}

func parseMemMiB(raw string) float64 {
	used := strings.TrimSpace(strings.Split(raw, "/")[0])
	switch {
	case strings.HasSuffix(used, "GiB"):
		v, _ := strconv.ParseFloat(strings.TrimSuffix(used, "GiB"), 64)
		return v * 1024
	case strings.HasSuffix(used, "MiB"):
		v, _ := strconv.ParseFloat(strings.TrimSuffix(used, "MiB"), 64)
		return v
	case strings.HasSuffix(used, "KiB"):
		v, _ := strconv.ParseFloat(strings.TrimSuffix(used, "KiB"), 64)
		return v / 1024
	default:
		return 0
	}
}
