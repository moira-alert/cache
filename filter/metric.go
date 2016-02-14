package filter

import (
	"log"
	"strconv"
	"sync/atomic"
	"time"
)

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
		if !strconv.IsPrint(rune(b)) { // Strip invalid characters from the input
			if len(lineBytes) > i { // Dont try and remove rune if at end of bytes
				copy(lineBytes[i:], lineBytes[i + 1:])
			}
			lineBytes = lineBytes[:len(lineBytes) - 1]git
		}
		if b == ' '{
			parts[partIndex] = lineBytes[partOffset:i]
			partOffset = i + 1
			partIndex ++
		}
		if partIndex > 2 {
			return nil
		}
	}
	
	if partIndex == 0{
		return nil
	}
	
	parts[partIndex] = lineBytes[partOffset:]

	metric := parts[0]
	valueString := string(parts[1])

	value, err := strconv.ParseFloat(valueString, 64)
	if err != nil {
		log.Printf("Can not parse value [%s] in line [%s]: %s", valueString, string(lineBytes), err)
		return nil
	}

	var timestamp int64
	timestampString := string(parts[2])
	if partIndex == 2 {
		parsed, err := strconv.ParseInt(timestampString, 10, 64)
		if err != nil || parsed == 0 {
			log.Printf("Can not parse timestamp [%s] in line [%s]: %s. Use current timestamp", timestampString, string(lineBytes), err)
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
