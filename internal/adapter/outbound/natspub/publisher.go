package natspub

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/danialv/telemetry/internal/adapter/codec"
	"github.com/danialv/telemetry/internal/domain"
)

type Config struct {
	URL             string
	StreamName      string
	Subject         string
	MaxStreamBytes  int64
	MaxBatchSamples int
	FlushInterval   time.Duration
	Workers         int
	QueueCapacity   int
}

func DefaultConfig(url string) Config {
	return Config{
		URL:             url,
		StreamName:      "TELEMETRY",
		Subject:         "telemetry.samples",
		MaxStreamBytes:  2 << 30,
		MaxBatchSamples: 4000,
		FlushInterval:   8 * time.Millisecond,
		Workers:         4,
		QueueCapacity:   16384,
	}
}

type publishJob struct {
	samples []domain.Sample
	done    chan error
}

type Publisher struct {
	cfg    Config
	nc     *nats.Conn
	js     jetstream.JetStream
	jobs   chan publishJob
	stopCh chan struct{}
	wg     sync.WaitGroup
}

func NewPublisher(ctx context.Context, cfg Config) (*Publisher, error) {
	if cfg.MaxBatchSamples <= 0 {
		cfg.MaxBatchSamples = 4000
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 8 * time.Millisecond
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 4
	}
	if cfg.QueueCapacity <= 0 {
		cfg.QueueCapacity = 16384
	}

	nc, err := nats.Connect(
		cfg.URL,
		nats.MaxReconnects(-1),
		nats.ReconnectWait(500*time.Millisecond),
		nats.Timeout(5*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("connect nats: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("init jetstream: %w", err)
	}

	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       cfg.StreamName,
		Subjects:   []string{cfg.Subject},
		Storage:    jetstream.FileStorage,
		Retention:  jetstream.WorkQueuePolicy,
		Discard:    jetstream.DiscardNew,
		MaxBytes:   cfg.MaxStreamBytes,
		MaxAge:     24 * time.Hour,
		MaxMsgSize: 4 << 20,
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("ensure stream: %w", err)
	}

	p := &Publisher{
		cfg:    cfg,
		nc:     nc,
		js:     js,
		jobs:   make(chan publishJob, cfg.QueueCapacity),
		stopCh: make(chan struct{}),
	}

	for i := 0; i < cfg.Workers; i++ {
		p.wg.Add(1)
		go p.runWorker()
	}

	return p, nil
}

func (p *Publisher) Publish(ctx context.Context, samples []domain.Sample) error {
	if len(samples) == 0 {
		return nil
	}

	if len(samples) >= p.cfg.MaxBatchSamples {
		return p.publishChunksDirect(ctx, samples)
	}

	job := publishJob{
		samples: samples,
		done:    make(chan error, 1),
	}

	select {
	case p.jobs <- job:
	default:
		return domain.ErrBackpressure
	}

	select {
	case err := <-job.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Publisher) publishChunksDirect(ctx context.Context, samples []domain.Sample) error {
	step := p.cfg.MaxBatchSamples
	for start := 0; start < len(samples); start += step {
		end := start + step
		if end > len(samples) {
			end = len(samples)
		}
		if err := p.publishFrame(ctx, samples[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func (p *Publisher) runWorker() {
	defer p.wg.Done()

	ticker := time.NewTicker(p.cfg.FlushInterval)
	defer ticker.Stop()

	pendingJobs := make([]publishJob, 0, 256)
	pendingSamples := make([]domain.Sample, 0, p.cfg.MaxBatchSamples)

	flush := func() {
		if len(pendingJobs) == 0 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := p.publishFrame(ctx, pendingSamples)
		cancel()

		for i := range pendingJobs {
			pendingJobs[i].done <- err
		}
		pendingJobs = pendingJobs[:0]
		pendingSamples = pendingSamples[:0]
	}

	for {
		select {
		case job := <-p.jobs:
			if len(pendingSamples)+len(job.samples) > p.cfg.MaxBatchSamples && len(pendingJobs) > 0 {
				flush()
			}
			pendingJobs = append(pendingJobs, job)
			pendingSamples = append(pendingSamples, job.samples...)
			if len(pendingSamples) >= p.cfg.MaxBatchSamples {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-p.stopCh:
			for {
				select {
				case job := <-p.jobs:
					pendingJobs = append(pendingJobs, job)
					pendingSamples = append(pendingSamples, job.samples...)
					if len(pendingSamples) >= p.cfg.MaxBatchSamples {
						flush()
					}
				default:
					flush()
					return
				}
			}
		}
	}
}

func (p *Publisher) publishFrame(ctx context.Context, samples []domain.Sample) error {
	payload := codec.EncodeSamples(samples)
	_, err := p.js.Publish(ctx, p.cfg.Subject, payload)
	if err != nil {
		if isStreamFullError(err) {
			return domain.ErrBackpressure
		}
		return err
	}
	return nil
}

func isStreamFullError(err error) bool {
	if errors.Is(err, jetstream.ErrMaxBytesExceeded) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "maximum bytes") || strings.Contains(msg, "insufficient storage")
}

func (p *Publisher) Close() error {
	close(p.stopCh)
	p.wg.Wait()
	return p.nc.Drain()
}
