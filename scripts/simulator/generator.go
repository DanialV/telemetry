package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"time"
)

func synthesizeReadings(deviceIdx int, unixSec int64, yearOrigin int64) [7]float32 {
	t := float64(unixSec)
	phase := float64(deviceIdx) * 0.73

	dayWave := math.Sin(2*math.Pi*(t/86400.0) + phase)
	hourWave := math.Sin(2*math.Pi*(t/3600.0) + phase*1.9)
	microJitter := math.Sin(t*0.17+phase*3.1)*3.2 + math.Cos(t*0.053+phase)*2.1

	burst := 0.0
	if (unixSec+int64(deviceIdx*137))%1800 < 90 {
		burst = 28.0
	}

	cpu := clampPct(38.0 + 22.0*dayWave + 8.0*hourWave + burst + microJitter)
	memory := clampPct(54.0 + 14.0*dayWave + 4.0*math.Sin(t/7200.0+phase) + microJitter*0.5)

	yearProgress := math.Max(0, math.Min(1, float64(unixSec-yearOrigin)/(365.0*86400.0)))
	diskSpace := clampPct(32.0 + 44.0*yearProgress + float64(deviceIdx%7)*2.5 + 0.4*dayWave)

	diskBusy := clampPct(18.0 + 0.42*cpu + 6.0*math.Abs(hourWave) + microJitter)
	netRx := clampPct(14.0 + 0.38*cpu + 10.0*math.Max(0, dayWave) + microJitter*0.8)
	netTx := clampPct(11.0 + 0.31*cpu + 8.0*math.Max(0, dayWave) + microJitter*0.7)
	fan := clampPct(28.0 + 0.58*cpu + 4.0*dayWave)

	return [7]float32{
		float32(cpu),
		float32(memory),
		float32(diskSpace),
		float32(diskBusy),
		float32(netRx),
		float32(netTx),
		float32(fan),
	}
}

func clampPct(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 100 {
		return 100
	}
	return v
}

func appendDeviceBatchJSON(dst []byte, deviceID string, deviceIdx int, startUnix, endUnix, yearOrigin int64) []byte {
	dst = append(dst[:0], `{"device_id":"`...)
	dst = append(dst, deviceID...)
	dst = append(dst, `","samples":[`...)

	first := true
	for ts := startUnix; ts <= endUnix; ts++ {
		if !first {
			dst = append(dst, ',')
		}
		first = false
		vals := synthesizeReadings(deviceIdx, ts, yearOrigin)
		dst = appendSampleJSON(dst, "", ts, vals)
	}
	dst = append(dst, `]}`...)
	return dst
}

func appendSampleJSON(dst []byte, deviceID string, ts int64, vals [7]float32) []byte {
	dst = append(dst, '{')
	if deviceID != "" {
		dst = append(dst, `"device_id":"`...)
		dst = append(dst, deviceID...)
		dst = append(dst, `",`...)
	}
	dst = append(dst, `"timestamp":`...)
	dst = strconv.AppendInt(dst, ts, 10)
	dst = append(dst, `,"cpu":`...)
	dst = strconv.AppendFloat(dst, float64(vals[0]), 'f', 2, 32)
	dst = append(dst, `,"memory":`...)
	dst = strconv.AppendFloat(dst, float64(vals[1]), 'f', 2, 32)
	dst = append(dst, `,"disk_space":`...)
	dst = strconv.AppendFloat(dst, float64(vals[2]), 'f', 2, 32)
	dst = append(dst, `,"disk_busy":`...)
	dst = strconv.AppendFloat(dst, float64(vals[3]), 'f', 2, 32)
	dst = append(dst, `,"net_rx":`...)
	dst = strconv.AppendFloat(dst, float64(vals[4]), 'f', 2, 32)
	dst = append(dst, `,"net_tx":`...)
	dst = strconv.AppendFloat(dst, float64(vals[5]), 'f', 2, 32)
	dst = append(dst, `,"fan":`...)
	dst = strconv.AppendFloat(dst, float64(vals[6]), 'f', 2, 32)
	dst = append(dst, '}')
	return dst
}

func postWithRetry(ctx context.Context, client *http.Client, url string, jsonPayload []byte, compress bool) error {
	var bodyBytes []byte
	if compress {
		var compressed bytes.Buffer
		gz, _ := gzip.NewWriterLevel(&compressed, gzip.BestSpeed)
		_, _ = gz.Write(jsonPayload)
		_ = gz.Close()
		bodyBytes = compressed.Bytes()
	} else {
		bodyBytes = jsonPayload
	}

	backoff := 200 * time.Millisecond
	for attempt := 0; attempt < 8; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		if compress {
			req.Header.Set("Content-Encoding", "gzip")
		}

		resp, err := client.Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
			if resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests {
				return fmt.Errorf("rejected with status %d", resp.StatusCode)
			}
		}

		select {
		case <-time.After(backoff):
			backoff = min(backoff*2, 3*time.Second)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return fmt.Errorf("exhausted retries posting to %s", url)
}
