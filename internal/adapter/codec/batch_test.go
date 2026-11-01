package codec_test

import (
	"testing"
	"time"

	"github.com/danialv/telemetry/internal/adapter/codec"
	"github.com/danialv/telemetry/internal/domain"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	s1, err := domain.NewSample("server-0001", time.Unix(1730000001, 0), domain.RawReadings{
		CPU: 12.5, Memory: 64.25, DiskSpace: 80.0, DiskBusy: 5.5, NetRx: 11.2, NetTx: 9.8, Fan: 45.0,
	})
	if err != nil {
		t.Fatal(err)
	}
	s2, err := domain.NewSample("server-0002", time.Unix(1730000002, 0), domain.RawReadings{
		CPU: 99.5, Memory: 88.0, DiskSpace: 40.0, DiskBusy: 75.25, NetRx: 50.0, NetTx: 60.0, Fan: 92.0,
	})
	if err != nil {
		t.Fatal(err)
	}

	wire := codec.EncodeSamples([]domain.Sample{s1, s2})
	decoded, err := codec.DecodeSamples(wire)
	if err != nil {
		t.Fatalf("unexpected decode error: %v", err)
	}
	if len(decoded) != 2 {
		t.Fatalf("expected 2 samples, got %d", len(decoded))
	}
	if decoded[0] != s1 || decoded[1] != s2 {
		t.Fatalf("round-trip mismatch: got %+v, want %+v / %+v", decoded, s1, s2)
	}
}
