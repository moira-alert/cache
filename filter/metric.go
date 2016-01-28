package filter

import (
	"log"
	"strconv"
	"strings"
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
func (t *PatternStorage) ProcessIncomingMetric(rawLine string) *MatchedMetric {
	count := atomic.AddInt64(&totalReceived, 1)

	elems := strings.Split(strings.TrimSpace(rawLine), " ")
	if len(elems) != 2 && len(elems) != 3 {
		// log.Printf("Line [%s] must be of 2 or 3 words. Got %d", rawLine, len(elems))
		return nil
	}

	runes := make([]rune, 0, len(elems[0]))
	for _, r := range elems[0] {
		if !strconv.IsPrint(r) {
			continue
		}
		runes = append(runes, r)
	}
	metric := string(runes)

	value, err := strconv.ParseFloat(elems[1], 64)
	if err != nil {
		log.Printf("Can not parse value [%s] in line [%s]: %s", elems[1], rawLine, err)
		return nil
	}

	ts := time.Now().Unix()
	if len(elems) == 3 {
		tsf, err := strconv.ParseFloat(elems[2], 64)
		if err != nil || tsf == 0 {
			log.Printf("Can not parse timestamp [%s] in line [%s]: %s. Use current timestamp", elems[2], rawLine, err)
		} else {
			ts = int64(tsf)
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
		return &MatchedMetric{metric, matched, value, ts, ts, 60}
	}
	return nil
}
