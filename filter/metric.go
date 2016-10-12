package filter

import (
	"fmt"
	"log"
	"strconv"
	"sync/atomic"
	"time"
	"unicode"
)

// LogParseErrors flag to log parse errors
var LogParseErrors bool

// MatchedMetric represent parsed and matched metric data
type MatchedMetric struct {
	Metric             string
	Patterns           []string
	Value              float64
	Timestamp          int64
	RetentionTimestamp int64
	Retention          int
}

var (
	totalReceived   int64
	validReceived   int64
	matchedReceived int64
)

// ParseMetricFromString parses metric from string
func ParseMetricFromString(line []byte) ([]byte, float64, int64, error) {
	var parts [3][]byte
	partIndex := 0
	partOffset := 0
	for i, b := range line {
		r := rune(b)
		if r > unicode.MaxASCII || !strconv.IsPrint(r) {
			return nil, 0, 0, fmt.Errorf("non-ascii or non-printable chars in metric name: '%s'", line)
		}
		if b == ' ' {
			parts[partIndex] = line[partOffset:i]
			partOffset = i + 1
			partIndex++
		}
		if partIndex > 2 {
			return nil, 0, 0, fmt.Errorf("too many space-separated items: '%s'", line)
		}
	}

	if partIndex < 2 {
		return nil, 0, 0, fmt.Errorf("too few space-separated items: '%s'", line)
	}

	parts[partIndex] = line[partOffset:]

	metric := parts[0]
	if len(metric) < 1 {
		return nil, 0, 0, fmt.Errorf("metric name is empty: '%s'", line)
	}

	valueString := string(parts[1])
	value, err := strconv.ParseFloat(valueString, 64)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("cannot parse value: '%s' (%s)", line, err)
	}

	timestampString := string(parts[2])
	timestamp, err := strconv.ParseInt(timestampString, 10, 64)
	if err != nil || timestamp == 0 {
		return nil, 0, 0, fmt.Errorf("cannot parse timestamp: '%s' (%s)", line, err)
	}

	return metric, value, timestamp, nil
}

// ProcessIncomingMetric process "metric value timestamp" raw line
func (t *PatternStorage) ProcessIncomingMetric(lineBytes []byte) *MatchedMetric {
	count := atomic.AddInt64(&totalReceived, 1)

	metric, value, timestamp, err := ParseMetricFromString(lineBytes)
	if err != nil {
		if LogParseErrors {
			log.Printf("cannot parse input: %s", err)
		}

		return nil
	}

	atomic.AddInt64(&validReceived, 1)

	matchingStart := time.Now()
	matched := t.MatchPattern(metric)
	if count%10 == 0 {
		MatchingTimer.UpdateSince(matchingStart)
	}
	if len(matched) > 0 {
		atomic.AddInt64(&matchedReceived, 1)
		return &MatchedMetric{string(metric), matched, value, timestamp, timestamp, 60}
	}
	return nil
}
