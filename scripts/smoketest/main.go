package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

func main() {
	ingestBase := flag.String("ingest-url", envOr("INGEST_BASE_URL", "http://localhost:8082"), "ingestor base url")
	queryBase := flag.String("query-url", envOr("QUERY_BASE_URL", "http://localhost:8084"), "query-api base url")
	procBase := flag.String("processor-url", envOr("PROCESSOR_BASE_URL", "http://localhost:8083"), "processor base url")
	flag.Parse()

	client := &http.Client{Timeout: 10 * time.Second}

	mustWaitHealthy(client, *ingestBase+"/healthz", "ingestor")
	mustWaitHealthy(client, *procBase+"/healthz", "processor")
	mustWaitHealthy(client, *queryBase+"/healthz", "query-api")
	fmt.Println("[PASS] Service health checks (ingestor, processor, query-api)")

	badPayload := `{"device_id":"smoke-bad","timestamp":1730000000,"cpu":125,"memory":40,"disk_space":50,"disk_busy":10,"net_rx":5,"net_tx":5,"fan":30}`
	badResp, err := client.Post(*ingestBase+"/api/v1/telemetry", "application/json", strings.NewReader(badPayload))
	mustNoErr(err, "post invalid payload")
	_ = badResp.Body.Close()
	if badResp.StatusCode != http.StatusBadRequest {
		failf("expected 400 for out-of-range sensor, got %d", badResp.StatusCode)
	}
	fmt.Println("[PASS] Domain invariant validation rejects out-of-bounds sensor values")

	probeID := fmt.Sprintf("smoke-device-%d", time.Now().UnixNano()%1_000_000)
	now := time.Now().UTC().Truncate(time.Second)

	type sample struct {
		Timestamp int64   `json:"timestamp"`
		CPU       float32 `json:"cpu"`
		Memory    float32 `json:"memory"`
		DiskSpace float32 `json:"disk_space"`
		DiskBusy  float32 `json:"disk_busy"`
		NetRx     float32 `json:"net_rx"`
		NetTx     float32 `json:"net_tx"`
		Fan       float32 `json:"fan"`
	}

	samples := make([]sample, 0, 1200)
	for h := 364 * 24; h >= 1; h -= 6 {
		ts := now.Add(-time.Duration(h) * time.Hour).Unix()
		samples = append(samples, sample{
			Timestamp: ts, CPU: 44.5, Memory: 61.0, DiskSpace: 52.0, DiskBusy: 19.0, NetRx: 12.0, NetTx: 11.0, Fan: 48.0,
		})
	}
	for sec := 60; sec >= 1; sec-- {
		ts := now.Add(-time.Duration(sec) * time.Second).Unix()
		samples = append(samples, sample{
			Timestamp: ts, CPU: 55.0, Memory: 63.0, DiskSpace: 53.0, DiskBusy: 22.0, NetRx: 14.0, NetTx: 13.0, Fan: 50.0,
		})
	}

	bodyBytes, err := json.Marshal(map[string]any{"device_id": probeID, "samples": samples})
	mustNoErr(err, "marshal smoke batch")

	ingResp, err := client.Post(*ingestBase+"/api/v1/telemetry/batch", "application/json", bytes.NewReader(bodyBytes))
	mustNoErr(err, "post smoke batch")
	_ = ingResp.Body.Close()
	if ingResp.StatusCode != http.StatusAccepted {
		failf("expected 202 Accepted on batch ingest, got %d", ingResp.StatusCode)
	}
	fmt.Printf("[PASS] Batch ingestion accepted (%d samples for %s)\n", len(samples), probeID)

	foundDevice := false
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		resp, qErr := client.Get(*queryBase + "/api/v1/devices?q=" + probeID)
		if qErr == nil {
			var devPayload struct {
				Devices []struct {
					ID string `json:"id"`
				} `json:"devices"`
			}
			_ = json.NewDecoder(resp.Body).Decode(&devPayload)
			_ = resp.Body.Close()
			for _, d := range devPayload.Devices {
				if d.ID == probeID {
					foundDevice = true
					break
				}
			}
		}
		if foundDevice {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !foundDevice {
		failf("device %s did not appear in /api/v1/devices within 45s", probeID)
	}
	fmt.Printf("[PASS] Async persistence & device search registry verified (%s)\n", probeID)

	verifySeries(client, *queryBase, probeID, "5m", "1s", 50)
	verifySeries(client, *queryBase, probeID, "24h", "1m", 3)
	yearDur := verifySeries(client, *queryBase, probeID, "1y", "1h", 1400)
	if yearDur >= time.Second {
		failf("1-year probe query exceeded 1s SLO: %v", yearDur)
	}

	heroCheck, err := client.Get(*queryBase + "/api/v1/devices?q=server-0001")
	if err == nil {
		var devList struct {
			Devices []struct {
				ID string `json:"id"`
			} `json:"devices"`
		}
		_ = json.NewDecoder(heroCheck.Body).Decode(&devList)
		_ = heroCheck.Body.Close()
		if len(devList.Devices) > 0 {
			heroDur := verifySeries(client, *queryBase, "server-0001", "1y", "1h", 1)
			if heroDur >= time.Second {
				failf("server-0001 1-year query exceeded 1s SLO: %v", heroDur)
			}
		}
	}

	uiResp, err := client.Get(*queryBase + "/")
	mustNoErr(err, "fetch HTML UI")
	uiBytes, _ := io.ReadAll(uiResp.Body)
	_ = uiResp.Body.Close()
	if uiResp.StatusCode != http.StatusOK || !strings.Contains(string(uiBytes), "<canvas id=\"chartCanvas\">") {
		failf("unexpected HTML UI response (status=%d)", uiResp.StatusCode)
	}
	fmt.Println("[PASS] Single-file HTML UI served properly on GET /")
	fmt.Println("\nALL SMOKE TESTS PASSED.")
}

func verifySeries(client *http.Client, baseURL, deviceID, window, wantRes string, minPoints int) time.Duration {
	url := fmt.Sprintf("%s/api/v1/series?device_id=%s&sensors=cpu,memory,fan&range=%s", baseURL, deviceID, window)
	started := time.Now()
	resp, err := client.Get(url)
	mustNoErr(err, "query series "+window)
	elapsed := time.Since(started)

	var payload struct {
		Resolution string  `json:"resolution"`
		QueryMs    float64 `json:"query_ms"`
		Timestamps []int64 `json:"timestamps"`
		Series     []struct {
			Sensor string    `json:"sensor"`
			Avg    []float32 `json:"avg"`
		} `json:"series"`
	}
	err = json.NewDecoder(resp.Body).Decode(&payload)
	_ = resp.Body.Close()
	mustNoErr(err, "decode series "+window)

	if resp.StatusCode != http.StatusOK {
		failf("range %s returned status %d", window, resp.StatusCode)
	}
	if payload.Resolution != wantRes {
		failf("range %s resolution=%s, want %s", window, payload.Resolution, wantRes)
	}
	if len(payload.Timestamps) < minPoints {
		failf("range %s returned %d points, expected >= %d", window, len(payload.Timestamps), minPoints)
	}
	if len(payload.Series) != 3 {
		failf("range %s returned %d series, expected 3", window, len(payload.Series))
	}
	fmt.Printf("[PASS] Series query %s (%s) -> resolution=%s, points=%d, server=%.2fms, rtt=%v\n",
		deviceID, window, payload.Resolution, len(payload.Timestamps), payload.QueryMs, elapsed.Round(100*time.Microsecond))
	return elapsed
}

func mustWaitHealthy(client *http.Client, url, name string) {
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	failf("service %s failed healthcheck at %s", name, url)
}

func mustNoErr(err error, msg string) {
	if err != nil {
		failf("%s: %v", msg, err)
	}
}

func failf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "[FAIL] "+format+"\n", args...)
	os.Exit(1)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
