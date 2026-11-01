package natssub

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/danialv/telemetry/internal/adapter/codec"
	"github.com/danialv/telemetry/internal/domain"
	"github.com/danialv/telemetry/internal/ports"
)

type Config struct {
	URL            string
	StreamName     string
	Subject        string
	ConsumerName   string
	MaxStreamBytes int64
	FetchBatch     int
	FetchMaxWait   time.Duration
	Workers        int
}

func DefaultConfig(url string) Config {
	return Config{
		URL:            url,
		StreamName:     "TELEMETRY",
		Subject:        "telemetry.samples",
		ConsumerName:   "processor-durable",
		MaxStreamBytes: 2 << 30,
		FetchBatch:     32,
		FetchMaxWait:   250 * time.Millisecond,
		Workers:        2,
	}
}

type Consumer struct {
	cfg       Config
	processor ports.BatchProcessor
	logger    *slog.Logger
	nc        *nats.Conn
	cons      jetstream.Consumer
}

func NewConsumer(ctx context.Context, cfg Config, processor ports.BatchProcessor, logger *slog.Logger) (*Consumer, error) {
	if cfg.FetchBatch <= 0 {
		cfg.FetchBatch = 32
	}
	if cfg.FetchMaxWait <= 0 {
		cfg.FetchMaxWait = 250 * time.Millisecond
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 2
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

	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
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

	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       cfg.ConsumerName,
		FilterSubject: cfg.Subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       60 * time.Second,
		MaxDeliver:    15,
		MaxAckPending: 512,
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("ensure consumer: %w", err)
	}

	return &Consumer{
		cfg:       cfg,
		processor: processor,
		logger:    logger,
		nc:        nc,
		cons:      cons,
	}, nil
}

func (c *Consumer) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	for i := 0; i < c.cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.workerLoop(ctx)
		}()
	}
	<-ctx.Done()
	wg.Wait()
	return nil
}

func (c *Consumer) workerLoop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		batch, err := c.cons.Fetch(c.cfg.FetchBatch, jetstream.FetchMaxWait(c.cfg.FetchMaxWait))
		if err != nil {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, nats.ErrTimeout) {
				c.logger.Warn("jetstream fetch failed", "error", err)
				select {
				case <-time.After(300 * time.Millisecond):
				case <-ctx.Done():
					return
				}
			}
			continue
		}

		validMsgs := make([]jetstream.Msg, 0, c.cfg.FetchBatch)
		samples := make([]domain.Sample, 0, c.cfg.FetchBatch*1024)

		for msg := range batch.Messages() {
			decoded, decErr := codec.DecodeSamples(msg.Data())
			if decErr != nil {
				c.logger.Error("terminating corrupt message", "error", decErr)
				_ = msg.Term()
				continue
			}
			validMsgs = append(validMsgs, msg)
			samples = append(samples, decoded...)
		}

		if len(validMsgs) == 0 {
			continue
		}

		writeCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		procErr := c.processor.Process(writeCtx, samples)
		cancel()

		if procErr != nil {
			c.logger.Error("batch persistence failed; naking for redelivery", "samples", len(samples), "error", procErr)
			for _, msg := range validMsgs {
				_ = msg.NakWithDelay(2 * time.Second)
			}
			continue
		}

		for _, msg := range validMsgs {
			if ackErr := msg.Ack(); ackErr != nil {
				c.logger.Warn("failed to ack message", "error", ackErr)
			}
		}
	}
}

func (c *Consumer) Close() error {
	return c.nc.Drain()
}
