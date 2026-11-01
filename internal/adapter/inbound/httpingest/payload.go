package httpingest

import (
	"bytes"
	"encoding/json"
	"errors"
	"time"

	"github.com/danialv/telemetry/internal/domain"
)

type sampleDTO struct {
	DeviceID  string   `json:"device_id"`
	Timestamp int64    `json:"timestamp"`
	CPU       *float32 `json:"cpu"`
	Memory    *float32 `json:"memory"`
	DiskSpace *float32 `json:"disk_space"`
	DiskBusy  *float32 `json:"disk_busy"`
	NetRx     *float32 `json:"net_rx"`
	NetTx     *float32 `json:"net_tx"`
	Fan       *float32 `json:"fan"`
}

type batchEnvelopeDTO struct {
	DeviceID string      `json:"device_id"`
	Samples  []sampleDTO `json:"samples"`
}

func decodeSamplesPayload(payload []byte) ([]domain.Sample, error) {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 {
		return nil, domain.ErrEmptyBatch
	}

	var defaultDeviceID string
	var dtos []sampleDTO

	if trimmed[0] == '[' {
		if err := json.Unmarshal(trimmed, &dtos); err != nil {
			return nil, errors.New("malformed json array")
		}
	} else {
		var probe struct {
			Samples json.RawMessage `json:"samples"`
		}
		if err := json.Unmarshal(trimmed, &probe); err != nil {
			return nil, errors.New("malformed json object")
		}
		if len(probe.Samples) > 0 {
			var env batchEnvelopeDTO
			if err := json.Unmarshal(trimmed, &env); err != nil {
				return nil, errors.New("malformed batch payload")
			}
			defaultDeviceID = env.DeviceID
			dtos = env.Samples
		} else {
			var single sampleDTO
			if err := json.Unmarshal(trimmed, &single); err != nil {
				return nil, errors.New("malformed sample payload")
			}
			dtos = []sampleDTO{single}
		}
	}

	if len(dtos) == 0 {
		return nil, domain.ErrEmptyBatch
	}

	nowUTC := time.Now().UTC()
	samples := make([]domain.Sample, 0, len(dtos))
	for _, dto := range dtos {
		deviceID := dto.DeviceID
		if deviceID == "" {
			deviceID = defaultDeviceID
		}
		if dto.CPU == nil || dto.Memory == nil || dto.DiskSpace == nil ||
			dto.DiskBusy == nil || dto.NetRx == nil || dto.NetTx == nil || dto.Fan == nil {
			return nil, errors.New("all 7 sensor fields are required")
		}
		ts := nowUTC
		if dto.Timestamp > 1_000_000_000_000 {
			ts = time.UnixMilli(dto.Timestamp).UTC()
		} else if dto.Timestamp != 0 {
			ts = time.Unix(dto.Timestamp, 0).UTC()
		}
		sample, err := domain.NewSample(deviceID, ts, domain.RawReadings{
			CPU:       *dto.CPU,
			Memory:    *dto.Memory,
			DiskSpace: *dto.DiskSpace,
			DiskBusy:  *dto.DiskBusy,
			NetRx:     *dto.NetRx,
			NetTx:     *dto.NetTx,
			Fan:       *dto.Fan,
		})
		if err != nil {
			return nil, err
		}
		samples = append(samples, sample)
	}

	return samples, nil
}
