package domain_test

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/danialv/telemetry/internal/domain"
)

func TestNewSampleValidation(t *testing.T) {
	validRaw := domain.RawReadings{
		CPU:       42.5,
		Memory:    70.0,
		DiskSpace: 55.2,
		DiskBusy:  12.0,
		NetRx:     8.5,
		NetTx:     9.1,
		Fan:       60.0,
	}
	now := time.Unix(1730000000, 500_000_000)

	sample, err := domain.NewSample("srv-01", now, validRaw)
	if err != nil {
		t.Fatalf("expected valid sample, got %v", err)
	}
	if sample.Timestamp.Nanosecond() != 0 {
		t.Fatalf("expected timestamp truncated to second, got %v", sample.Timestamp)
	}

	if _, err := domain.NewSample("  ", now, validRaw); !errors.Is(err, domain.ErrInvalidDeviceID) {
		t.Fatalf("expected ErrInvalidDeviceID, got %v", err)
	}

	outOfBounds := validRaw
	outOfBounds.CPU = 100.01
	if _, err := domain.NewSample("srv-01", now, outOfBounds); !errors.Is(err, domain.ErrPercentageOutOfRange) {
		t.Fatalf("expected ErrPercentageOutOfRange, got %v", err)
	}

	nanRaw := validRaw
	nanRaw.Fan = float32(math.NaN())
	if _, err := domain.NewSample("srv-01", now, nanRaw); !errors.Is(err, domain.ErrPercentageOutOfRange) {
		t.Fatalf("expected ErrPercentageOutOfRange for NaN, got %v", err)
	}
}

func TestTimeRangeResolutionSelection(t *testing.T) {
	base := time.Unix(1730000000, 0).UTC()

	cases := []struct {
		span time.Duration
		want domain.Resolution
	}{
		{5 * time.Minute, domain.Resolution1s},
		{time.Hour, domain.Resolution1s},
		{6 * time.Hour, domain.Resolution1m},
		{7 * 24 * time.Hour, domain.Resolution1m},
		{30 * 24 * time.Hour, domain.Resolution1h},
		{365 * 24 * time.Hour, domain.Resolution1h},
	}

	for _, tc := range cases {
		tr, err := domain.NewTimeRange(base, base.Add(tc.span))
		if err != nil {
			t.Fatalf("span %v unexpected error: %v", tc.span, err)
		}
		if got := tr.SelectResolution(); got != tc.want {
			t.Fatalf("span %v got resolution %s, want %s", tc.span, got, tc.want)
		}
	}

	if _, err := domain.NewTimeRange(base, base.Add(30*time.Second)); !errors.Is(err, domain.ErrRangeTooShort) {
		t.Fatalf("expected ErrRangeTooShort, got %v", err)
	}

	if _, err := domain.NewTimeRange(base, base.Add(370*24*time.Hour)); !errors.Is(err, domain.ErrRangeTooLong) {
		t.Fatalf("expected ErrRangeTooLong, got %v", err)
	}
}
