package domain

import "strings"

type Sensor string

const (
	SensorCPU       Sensor = "cpu"
	SensorMemory    Sensor = "memory"
	SensorDiskSpace Sensor = "disk_space"
	SensorDiskBusy  Sensor = "disk_busy"
	SensorNetRx     Sensor = "net_rx"
	SensorNetTx     Sensor = "net_tx"
	SensorFan       Sensor = "fan"
)

func AllSensors() []Sensor {
	return []Sensor{
		SensorCPU,
		SensorMemory,
		SensorDiskSpace,
		SensorDiskBusy,
		SensorNetRx,
		SensorNetTx,
		SensorFan,
	}
}

func ParseSensor(raw string) (Sensor, error) {
	switch Sensor(strings.TrimSpace(strings.ToLower(raw))) {
	case SensorCPU:
		return SensorCPU, nil
	case SensorMemory:
		return SensorMemory, nil
	case SensorDiskSpace:
		return SensorDiskSpace, nil
	case SensorDiskBusy:
		return SensorDiskBusy, nil
	case SensorNetRx:
		return SensorNetRx, nil
	case SensorNetTx:
		return SensorNetTx, nil
	case SensorFan:
		return SensorFan, nil
	default:
		return "", ErrUnsupportedSensor
	}
}

func ParseSensors(raw []string) ([]Sensor, error) {
	if len(raw) == 0 {
		return nil, ErrNoSensorsSelected
	}
	seen := make(map[Sensor]struct{}, len(raw))
	result := make([]Sensor, 0, len(raw))
	for _, item := range raw {
		sensor, err := ParseSensor(item)
		if err != nil {
			return nil, err
		}
		if _, exists := seen[sensor]; exists {
			continue
		}
		seen[sensor] = struct{}{}
		result = append(result, sensor)
	}
	if len(result) == 0 {
		return nil, ErrNoSensorsSelected
	}
	return result, nil
}
