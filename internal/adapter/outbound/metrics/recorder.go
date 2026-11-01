package metrics

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gofiber/fiber/v3"

	"github.com/danialv/telemetry/internal/domain"
)

type Recorder struct {
	startedAt         time.Time
	ingestedSamples   atomic.Uint64
	ingestBatches     atomic.Uint64
	ingestErrors      atomic.Uint64
	ingestMicrosTotal atomic.Uint64

	persistedSamples   atomic.Uint64
	persistBatches     atomic.Uint64
	persistErrors      atomic.Uint64
	persistMicrosTotal atomic.Uint64

	queries1s        atomic.Uint64
	queries1m        atomic.Uint64
	queries1h        atomic.Uint64
	query1sMicros    atomic.Uint64
	query1mMicros    atomic.Uint64
	query1hMicros    atomic.Uint64
	query1sLastUs    atomic.Uint64
	query1mLastUs    atomic.Uint64
	query1hLastUs    atomic.Uint64
	queryErrors      atomic.Uint64
	queryMicrosTotal atomic.Uint64
}

func NewRecorder() *Recorder {
	return &Recorder{startedAt: time.Now()}
}

func (r *Recorder) RecordIngested(samples int, duration time.Duration) {
	r.ingestedSamples.Add(uint64(samples))
	r.ingestBatches.Add(1)
	r.ingestMicrosTotal.Add(uint64(duration.Microseconds()))
}

func (r *Recorder) RecordIngestError() {
	r.ingestErrors.Add(1)
}

func (r *Recorder) RecordPersisted(samples int, duration time.Duration) {
	r.persistedSamples.Add(uint64(samples))
	r.persistBatches.Add(1)
	r.persistMicrosTotal.Add(uint64(duration.Microseconds()))
}

func (r *Recorder) RecordPersistError() {
	r.persistErrors.Add(1)
}

func (r *Recorder) RecordQuery(resolution domain.Resolution, duration time.Duration) {
	us := uint64(duration.Microseconds())
	switch resolution {
	case domain.Resolution1s:
		r.queries1s.Add(1)
		r.query1sMicros.Add(us)
		r.query1sLastUs.Store(us)
	case domain.Resolution1m:
		r.queries1m.Add(1)
		r.query1mMicros.Add(us)
		r.query1mLastUs.Store(us)
	case domain.Resolution1h:
		r.queries1h.Add(1)
		r.query1hMicros.Add(us)
		r.query1hLastUs.Store(us)
	}
	r.queryMicrosTotal.Add(us)
}

func (r *Recorder) RecordQueryError() {
	r.queryErrors.Add(1)
}

type Snapshot struct {
	UptimeSeconds      float64 `json:"uptime_seconds"`
	IngestedSamples    uint64  `json:"ingested_samples"`
	IngestBatches      uint64  `json:"ingest_batches"`
	IngestErrors       uint64  `json:"ingest_errors"`
	IngestMicrosTotal  uint64  `json:"ingest_micros_total"`
	PersistedSamples   uint64  `json:"persisted_samples"`
	PersistBatches     uint64  `json:"persist_batches"`
	PersistErrors      uint64  `json:"persist_errors"`
	PersistMicrosTotal uint64  `json:"persist_micros_total"`
	Queries1s          uint64  `json:"queries_1s"`
	Queries1m          uint64  `json:"queries_1m"`
	Queries1h          uint64  `json:"queries_1h"`
	QueryErrors        uint64  `json:"query_errors"`
	QueryMicrosTotal   uint64  `json:"query_micros_total"`
}

func (r *Recorder) Snapshot() Snapshot {
	return Snapshot{
		UptimeSeconds:      time.Since(r.startedAt).Seconds(),
		IngestedSamples:    r.ingestedSamples.Load(),
		IngestBatches:      r.ingestBatches.Load(),
		IngestErrors:       r.ingestErrors.Load(),
		IngestMicrosTotal:  r.ingestMicrosTotal.Load(),
		PersistedSamples:   r.persistedSamples.Load(),
		PersistBatches:     r.persistBatches.Load(),
		PersistErrors:      r.persistErrors.Load(),
		PersistMicrosTotal: r.persistMicrosTotal.Load(),
		Queries1s:          r.queries1s.Load(),
		Queries1m:          r.queries1m.Load(),
		Queries1h:          r.queries1h.Load(),
		QueryErrors:        r.queryErrors.Load(),
		QueryMicrosTotal:   r.queryMicrosTotal.Load(),
	}
}

func (r *Recorder) Handler() fiber.Handler {
	return func(c fiber.Ctx) error {
		snap := r.Snapshot()
		if c.Query("format") == "prom" {
			cpuSec, rssBytes := readProcessResources()
			c.Set(fiber.HeaderContentType, "text/plain; version=0.0.4")
			return c.SendString(fmt.Sprintf(
				"telemetry_ingested_samples_total %d\n"+
					"telemetry_ingest_batches_total %d\n"+
					"telemetry_ingest_errors_total %d\n"+
					"telemetry_ingest_micros_total %d\n"+
					"telemetry_persisted_samples_total %d\n"+
					"telemetry_persist_batches_total %d\n"+
					"telemetry_persist_errors_total %d\n"+
					"telemetry_persist_micros_total %d\n"+
					"telemetry_queries_total{resolution=\"1s\"} %d\n"+
					"telemetry_queries_total{resolution=\"1m\"} %d\n"+
					"telemetry_queries_total{resolution=\"1h\"} %d\n"+
					"telemetry_query_micros_by_res_total{resolution=\"1s\"} %d\n"+
					"telemetry_query_micros_by_res_total{resolution=\"1m\"} %d\n"+
					"telemetry_query_micros_by_res_total{resolution=\"1h\"} %d\n"+
					"telemetry_query_last_ms{resolution=\"1s\"} %.3f\n"+
					"telemetry_query_last_ms{resolution=\"1m\"} %.3f\n"+
					"telemetry_query_last_ms{resolution=\"1h\"} %.3f\n"+
					"telemetry_query_errors_total %d\n"+
					"process_cpu_seconds_total %.3f\n"+
					"process_resident_memory_bytes %d\n"+
					"go_goroutines %d\n",
				snap.IngestedSamples,
				snap.IngestBatches,
				snap.IngestErrors,
				snap.IngestMicrosTotal,
				snap.PersistedSamples,
				snap.PersistBatches,
				snap.PersistErrors,
				snap.PersistMicrosTotal,
				snap.Queries1s,
				snap.Queries1m,
				snap.Queries1h,
				r.query1sMicros.Load(),
				r.query1mMicros.Load(),
				r.query1hMicros.Load(),
				float64(r.query1sLastUs.Load())/1000.0,
				float64(r.query1mLastUs.Load())/1000.0,
				float64(r.query1hLastUs.Load())/1000.0,
				snap.QueryErrors,
				cpuSec,
				rssBytes,
				runtime.NumGoroutine(),
			))
		}
		return c.JSON(snap)
	}
}

func readProcessResources() (float64, uint64) {
	var cpuSec float64
	if rawStat, err := os.ReadFile("/proc/self/stat"); err == nil {
		if idx := strings.LastIndexByte(string(rawStat), ')'); idx > 0 && idx+2 < len(rawStat) {
			fields := strings.Fields(string(rawStat[idx+2:]))
			if len(fields) > 12 {
				utime, _ := strconv.ParseFloat(fields[11], 64)
				stime, _ := strconv.ParseFloat(fields[12], 64)
				cpuSec = (utime + stime) / 100.0
			}
		}
	}

	var rssBytes uint64
	if rawStatm, err := os.ReadFile("/proc/self/statm"); err == nil {
		fields := strings.Fields(string(rawStatm))
		if len(fields) > 1 {
			if pages, pErr := strconv.ParseUint(fields[1], 10, 64); pErr == nil {
				rssBytes = pages * uint64(os.Getpagesize())
			}
		}
	}
	if rssBytes == 0 {
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		rssBytes = ms.Sys
	}
	return cpuSec, rssBytes
}
