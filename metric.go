package main

import (
	"log"
	"strconv"
	"strings"
	"time"
)

type matchedMetric struct {
	metric    string
	patterns  []string
	value     float64
	timestamp int
	retention int
}

func processIncomingMetric(rawLine string) *matchedMetric {
	totalMetricsReceived.Mark(1)

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
		log.Printf("Can not parse value [%s] in line [%s]: %s", elems[1], rawLine, err.Error())
		return nil
	}

	ts := time.Now().Unix()
	if len(elems) == 3 {
		tsf, err := strconv.ParseFloat(elems[2], 64)
		if err != nil || tsf == 0 {
			log.Printf("Can not parse timestamp [%s] in line [%s]: %s. Use current timestamp", elems[2], rawLine, err.Error())
		} else {
			ts = int64(tsf)
		}
	}

	validMetricsReceived.Mark(1)

	timer := time.Now()
	matched := patterns.matchPattern(metric)
	matchingTimer.UpdateSince(timer)
	if len(matched) > 0 {
		matchingMetricsReceived.Mark(1)
		return &matchedMetric{metric, matched, value, int(ts), 60}
	}
	return nil
}
