package domain

import "errors"

var (
	ErrInvalidDeviceID      = errors.New("invalid device id")
	ErrInvalidTimestamp     = errors.New("invalid timestamp")
	ErrPercentageOutOfRange = errors.New("sensor percentage must be between 0 and 100")
	ErrEmptyBatch           = errors.New("sample batch cannot be empty")
	ErrUnsupportedSensor    = errors.New("unsupported sensor")
	ErrNoSensorsSelected    = errors.New("at least one sensor must be selected")
	ErrInvalidTimeRange     = errors.New("end time must be after start time")
	ErrRangeTooShort        = errors.New("time range must be at least 1 minute")
	ErrRangeTooLong         = errors.New("time range cannot exceed 366 days")
	ErrBackpressure         = errors.New("ingestion buffer is saturated")
)
