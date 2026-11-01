package domain

import (
	"math"
	"strings"
	"time"
)

const maxDeviceIDLength = 128

type DeviceID string

func NewDeviceID(raw string) (DeviceID, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || len(trimmed) > maxDeviceIDLength {
		return "", ErrInvalidDeviceID
	}
	for i := 0; i < len(trimmed); i++ {
		ch := trimmed[i]
		if ch < 0x21 || ch > 0x7e {
			return "", ErrInvalidDeviceID
		}
	}
	return DeviceID(trimmed), nil
}

func (id DeviceID) String() string {
	return string(id)
}

type Percentage float32

func NewPercentage(value float32) (Percentage, error) {
	v64 := float64(value)
	if math.IsNaN(v64) || math.IsInf(v64, 0) || value < 0 || value > 100 {
		return 0, ErrPercentageOutOfRange
	}
	return Percentage(value), nil
}

func (p Percentage) Float32() float32 {
	return float32(p)
}

type RawReadings struct {
	CPU       float32
	Memory    float32
	DiskSpace float32
	DiskBusy  float32
	NetRx     float32
	NetTx     float32
	Fan       float32
}

type Readings struct {
	CPU       Percentage
	Memory    Percentage
	DiskSpace Percentage
	DiskBusy  Percentage
	NetRx     Percentage
	NetTx     Percentage
	Fan       Percentage
}

func NewReadings(raw RawReadings) (Readings, error) {
	cpu, err := NewPercentage(raw.CPU)
	if err != nil {
		return Readings{}, err
	}
	memory, err := NewPercentage(raw.Memory)
	if err != nil {
		return Readings{}, err
	}
	diskSpace, err := NewPercentage(raw.DiskSpace)
	if err != nil {
		return Readings{}, err
	}
	diskBusy, err := NewPercentage(raw.DiskBusy)
	if err != nil {
		return Readings{}, err
	}
	netRx, err := NewPercentage(raw.NetRx)
	if err != nil {
		return Readings{}, err
	}
	netTx, err := NewPercentage(raw.NetTx)
	if err != nil {
		return Readings{}, err
	}
	fan, err := NewPercentage(raw.Fan)
	if err != nil {
		return Readings{}, err
	}
	return Readings{
		CPU:       cpu,
		Memory:    memory,
		DiskSpace: diskSpace,
		DiskBusy:  diskBusy,
		NetRx:     netRx,
		NetTx:     netTx,
		Fan:       fan,
	}, nil
}

func (r Readings) ValueOf(sensor Sensor) Percentage {
	switch sensor {
	case SensorCPU:
		return r.CPU
	case SensorMemory:
		return r.Memory
	case SensorDiskSpace:
		return r.DiskSpace
	case SensorDiskBusy:
		return r.DiskBusy
	case SensorNetRx:
		return r.NetRx
	case SensorNetTx:
		return r.NetTx
	case SensorFan:
		return r.Fan
	default:
		return 0
	}
}

type Sample struct {
	DeviceID  DeviceID
	Timestamp time.Time
	Readings  Readings
}

func NewSample(deviceID string, timestamp time.Time, raw RawReadings) (Sample, error) {
	id, err := NewDeviceID(deviceID)
	if err != nil {
		return Sample{}, err
	}
	if timestamp.IsZero() || timestamp.Unix() <= 0 {
		return Sample{}, ErrInvalidTimestamp
	}
	readings, err := NewReadings(raw)
	if err != nil {
		return Sample{}, err
	}
	return Sample{
		DeviceID:  id,
		Timestamp: timestamp.UTC().Truncate(time.Second),
		Readings:  readings,
	}, nil
}
