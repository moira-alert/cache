package filter

import (
	"log"
	"strconv"
	"sync/atomic"
	"time"
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

// ProcessIncomingMetric process "metric value [timestamp]" raw line
func (t *PatternStorage) ProcessIncomingMetric(lineBytes []byte) *MatchedMetric {

	count := atomic.AddInt64(&totalReceived, 1)

	var parts [3][]byte
	partIndex := 0
	partOffset := 0
	for i, b := range lineBytes {
		if !strconv.IsPrint(rune(b)) {
			if i+1 < len(lineBytes) {
				copy(lineBytes[i:], lineBytes[i+1:])
			}
			lineBytes = lineBytes[:len(lineBytes)-1]
			if i < len(lineBytes) {
				b = lineBytes[i]
			} else {
				break
			}
		}
		if b == ' ' {
			parts[partIndex] = lineBytes[partOffset:i]
			partOffset = i + 1
			partIndex++
		}
		if partIndex > 2 {
			break
		}
	}

	if partIndex == 0 {
		return nil
	}

	if partIndex <= 2 {
		parts[partIndex] = lineBytes[partOffset:]
	}

	metric := parts[0]

	valueString := string(parts[1])

	value, err := strconv.ParseFloat(valueString, 64)
	if err != nil {
		if LogParseErrors {
			log.Printf("Can not parse value [%s] in line [%s]: %s", valueString, string(lineBytes), err.Error())
		}
		return nil
	}

	timestamp := time.Now().Unix()

	timestampString := string(parts[2])
	if partIndex >= 2 {
		parsed, err := strconv.ParseInt(timestampString, 10, 64)
		if err != nil || parsed == 0 {
			if LogParseErrors {
				log.Printf("Can not parse timestamp [%s] in line [%s]: %s. Use current timestamp", timestampString, string(lineBytes), err.Error())
			}
		} else {
			timestamp = parsed
		}
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
