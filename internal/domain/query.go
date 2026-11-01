package domain

import "time"

type Resolution string

const (
	Resolution1s Resolution = "1s"
	Resolution1m Resolution = "1m"
	Resolution1h Resolution = "1h"
)

func (r Resolution) Step() time.Duration {
	switch r {
	case Resolution1s:
		return time.Second
	case Resolution1m:
		return time.Minute
	case Resolution1h:
		return time.Hour
	default:
		return time.Second
	}
}

const (
	minQueryDuration = time.Minute
	maxQueryDuration = 366 * 24 * time.Hour
	rawCutoff        = time.Hour
	minuteCutoff     = 7 * 24 * time.Hour
)

type TimeRange struct {
	From time.Time
	To   time.Time
}

func NewTimeRange(from, to time.Time) (TimeRange, error) {
	fromUTC := from.UTC().Truncate(time.Second)
	toUTC := to.UTC().Truncate(time.Second)
	if !toUTC.After(fromUTC) {
		return TimeRange{}, ErrInvalidTimeRange
	}
	span := toUTC.Sub(fromUTC)
	if span < minQueryDuration {
		return TimeRange{}, ErrRangeTooShort
	}
	if span > maxQueryDuration {
		return TimeRange{}, ErrRangeTooLong
	}
	return TimeRange{From: fromUTC, To: toUTC}, nil
}

func (tr TimeRange) Duration() time.Duration {
	return tr.To.Sub(tr.From)
}

func (tr TimeRange) SelectResolution() Resolution {
	span := tr.Duration()
	switch {
	case span <= rawCutoff:
		return Resolution1s
	case span <= minuteCutoff:
		return Resolution1m
	default:
		return Resolution1h
	}
}

type SeriesQuery struct {
	DeviceID   DeviceID
	Sensors    []Sensor
	Range      TimeRange
	Resolution Resolution
}

func NewSeriesQuery(deviceID string, sensors []string, from, to time.Time) (SeriesQuery, error) {
	id, err := NewDeviceID(deviceID)
	if err != nil {
		return SeriesQuery{}, err
	}
	parsedSensors, err := ParseSensors(sensors)
	if err != nil {
		return SeriesQuery{}, err
	}
	tr, err := NewTimeRange(from, to)
	if err != nil {
		return SeriesQuery{}, err
	}
	return SeriesQuery{
		DeviceID:   id,
		Sensors:    parsedSensors,
		Range:      tr,
		Resolution: tr.SelectResolution(),
	}, nil
}

type SensorSeries struct {
	Sensor Sensor
	Avg    []float32
	Min    []float32
	Max    []float32
}

type SeriesResult struct {
	DeviceID   DeviceID
	Resolution Resolution
	From       time.Time
	To         time.Time
	Timestamps []time.Time
	Series     []SensorSeries
}

type DeviceSearch struct {
	Query string
	Limit int
}

func NewDeviceSearch(query string, limit int) DeviceSearch {
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}
	return DeviceSearch{
		Query: query,
		Limit: limit,
	}
}

type DeviceSummary struct {
	ID       DeviceID
	LastSeen time.Time
}
