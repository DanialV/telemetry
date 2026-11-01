package codec

import (
	"encoding/binary"
	"errors"
	"math"
	"time"

	"github.com/danialv/telemetry/internal/domain"
)

var (
	ErrCorruptFrame       = errors.New("corrupt batch frame")
	ErrUnsupportedVersion = errors.New("unsupported batch frame version")
)

const (
	frameMagic   = "TLMB"
	frameVersion = uint8(1)
	headerBytes  = 9
	readingBytes = 8 + 7*4
)

func EncodeSamples(samples []domain.Sample) []byte {
	total := headerBytes
	for i := range samples {
		total += 1 + len(samples[i].DeviceID) + readingBytes
	}

	buf := make([]byte, total)
	copy(buf[0:4], frameMagic)
	buf[4] = frameVersion
	binary.LittleEndian.PutUint32(buf[5:9], uint32(len(samples)))

	offset := headerBytes
	for i := range samples {
		s := samples[i]
		idLen := len(s.DeviceID)
		buf[offset] = uint8(idLen)
		offset++
		copy(buf[offset:offset+idLen], s.DeviceID)
		offset += idLen

		binary.LittleEndian.PutUint64(buf[offset:offset+8], uint64(s.Timestamp.Unix()))
		offset += 8

		offset = putFloat32(buf, offset, s.Readings.CPU.Float32())
		offset = putFloat32(buf, offset, s.Readings.Memory.Float32())
		offset = putFloat32(buf, offset, s.Readings.DiskSpace.Float32())
		offset = putFloat32(buf, offset, s.Readings.DiskBusy.Float32())
		offset = putFloat32(buf, offset, s.Readings.NetRx.Float32())
		offset = putFloat32(buf, offset, s.Readings.NetTx.Float32())
		offset = putFloat32(buf, offset, s.Readings.Fan.Float32())
	}

	return buf
}

func DecodeSamples(data []byte) ([]domain.Sample, error) {
	if len(data) < headerBytes || string(data[0:4]) != frameMagic {
		return nil, ErrCorruptFrame
	}
	if data[4] != frameVersion {
		return nil, ErrUnsupportedVersion
	}

	count := int(binary.LittleEndian.Uint32(data[5:9]))
	if count < 0 || count > 1_000_000 {
		return nil, ErrCorruptFrame
	}

	samples := make([]domain.Sample, 0, count)
	offset := headerBytes

	for i := 0; i < count; i++ {
		if offset >= len(data) {
			return nil, ErrCorruptFrame
		}
		idLen := int(data[offset])
		offset++
		if idLen == 0 || offset+idLen+readingBytes > len(data) {
			return nil, ErrCorruptFrame
		}

		deviceID := string(data[offset : offset+idLen])
		offset += idLen

		unixSec := int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
		offset += 8

		var raw domain.RawReadings
		raw.CPU, offset = readFloat32(data, offset)
		raw.Memory, offset = readFloat32(data, offset)
		raw.DiskSpace, offset = readFloat32(data, offset)
		raw.DiskBusy, offset = readFloat32(data, offset)
		raw.NetRx, offset = readFloat32(data, offset)
		raw.NetTx, offset = readFloat32(data, offset)
		raw.Fan, offset = readFloat32(data, offset)

		sample, err := domain.NewSample(deviceID, time.Unix(unixSec, 0).UTC(), raw)
		if err != nil {
			return nil, err
		}
		samples = append(samples, sample)
	}

	if offset != len(data) {
		return nil, ErrCorruptFrame
	}
	return samples, nil
}

func putFloat32(buf []byte, offset int, val float32) int {
	binary.LittleEndian.PutUint32(buf[offset:offset+4], math.Float32bits(val))
	return offset + 4
}

func readFloat32(buf []byte, offset int) (float32, int) {
	bits := binary.LittleEndian.Uint32(buf[offset : offset+4])
	return math.Float32frombits(bits), offset + 4
}
