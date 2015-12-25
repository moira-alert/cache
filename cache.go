package main

import (
	"bufio"
	"os"
	"regexp"
	"strings"
)

type retentionMatcher struct {
	pattern   *regexp.Regexp
	retention int
}

type cacheStorage struct {
	retentions []retentionMatcher
}

func newCacheStorage() (*cacheStorage, error) {
	retentionConfigFile, err := os.Open(retentionConfigFileName)
	if err != nil {
		return nil, err
	}

	storage := &cacheStorage{}
	if err := storage.buildRetentions(bufio.NewScanner(retentionConfigFile)); err != nil {
		return nil, err
	}

	return storage, nil
}

func (cs *cacheStorage) buildRetentions(retentionScanner *bufio.Scanner) error {
	cs.retentions = make([]retentionMatcher, 0, 100)

	for retentionScanner.Scan() {
		line := retentionScanner.Text()
		if strings.HasPrefix(line, "#") || strings.Count(line, "=") != 1 {
			continue
		}

		pattern, err := regexp.Compile(strings.TrimSpace(strings.Split(line, "=")[1]))
		if err != nil {
			return err
		}

		retentionScanner.Scan()
		line = retentionScanner.Text()
		retentions := strings.TrimSpace(strings.Split(line, "=")[1])
		retention, err := rawRetentionToSeconds(retentions[0:strings.Index(retentions, ":")])
		if err != nil {
			return err
		}

		cs.retentions = append(cs.retentions, retentionMatcher{
			pattern:   pattern,
			retention: retention,
		})
	}
	return retentionScanner.Err()
}

func (cs *cacheStorage) savePoints(buffer []*matchedMetric) error {

	for _, m := range buffer {
		for _, matcher := range cs.retentions {
			if matcher.pattern.MatchString(m.metric) {
				m.retention = matcher.retention
				break
			}
		}
		m.timestamp = roundToNearestRetention(m.timestamp, m.retention)

	}

	if err := db.SaveMetrics(buffer); err != nil {
		return err
	}

	return nil
}
