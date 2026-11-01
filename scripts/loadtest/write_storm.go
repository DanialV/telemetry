package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type loadOutcome struct {
	sentSamples uint64
	httpErrors  uint64
}

func runWriteStorm(ctx context.Context, ingestURL string, devices, batchPerReq, workers int) loadOutcome {
	transport := &http.Transport{
		MaxIdleConns:        workers * 2,
		MaxIdleConnsPerHost: workers * 2,
		MaxConnsPerHost:     workers * 2,
		IdleConnTimeout:     60 * time.Second,
	}
	client := &http.Client{Timeout: 5 * time.Second, Transport: transport}
	defer transport.CloseIdleConnections()

	type batchJob struct {
		startIdx int
		endIdx   int
		unixSec  int64
	}

	jobCh := make(chan batchJob, workers*8)
	var sent atomic.Uint64
	var errs atomic.Uint64

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 0, max(256, batchPerReq*160))
			for job := range jobCh {
				buf = buildLoadPayload(buf[:0], job.startIdx, job.endIdx, job.unixSec)
				req, err := http.NewRequestWithContext(ctx, http.MethodPost, ingestURL, bytes.NewReader(buf))
				if err != nil {
					continue
				}
				req.Header.Set("Content-Type", "application/json")
				resp, doErr := client.Do(req)
				if doErr != nil {
					if ctx.Err() == nil {
						errs.Add(1)
					}
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode >= 200 && resp.StatusCode < 300 {
					sent.Add(uint64(job.endIdx - job.startIdx))
				} else {
					errs.Add(1)
				}
			}
		}()
	}

	const subTicks = 50
	ticker := time.NewTicker(time.Second / subTicks)
	defer ticker.Stop()

	subIdx := 0
	currentSec := time.Now().UTC().Unix()

	for {
		select {
		case <-ctx.Done():
			close(jobCh)
			wg.Wait()
			return loadOutcome{sentSamples: sent.Load(), httpErrors: errs.Load()}
		case t := <-ticker.C:
			if subIdx == 0 {
				currentSec = t.UTC().Unix()
			}
			sliceStart := (devices * subIdx) / subTicks
			sliceEnd := (devices * (subIdx + 1)) / subTicks
			subIdx = (subIdx + 1) % subTicks

			for s := sliceStart; s < sliceEnd; s += batchPerReq {
				e := min(s+batchPerReq, sliceEnd)
				select {
				case jobCh <- batchJob{startIdx: s, endIdx: e, unixSec: currentSec}:
				default:
					errs.Add(1)
				}
			}
		}
	}
}

func buildLoadPayload(dst []byte, startIdx, endIdx int, ts int64) []byte {
	count := endIdx - startIdx
	if count == 1 {
		return appendOneLoadSample(dst, startIdx, ts)
	}
	dst = append(dst, `{"samples":[`...)
	for i := startIdx; i < endIdx; i++ {
		if i > startIdx {
			dst = append(dst, ',')
		}
		dst = appendOneLoadSample(dst, i, ts)
	}
	dst = append(dst, `]}`...)
	return dst
}

func appendOneLoadSample(dst []byte, idx int, ts int64) []byte {
	base := float64(20 + (idx*7)%60)
	dst = append(dst, `{"device_id":"server-`...)
	dst = append(dst, fmt.Sprintf("%04d", idx+1)...)
	dst = append(dst, `","timestamp":`...)
	dst = strconv.AppendInt(dst, ts, 10)
	dst = append(dst, `,"cpu":`...)
	dst = strconv.AppendFloat(dst, base, 'f', 1, 32)
	dst = append(dst, `,"memory":`...)
	dst = strconv.AppendFloat(dst, min(99, base+12), 'f', 1, 32)
	dst = append(dst, `,"disk_space":54.2,"disk_busy":22.5,"net_rx":15.1,"net_tx":12.8,"fan":48.0}`...)
	return dst
}
