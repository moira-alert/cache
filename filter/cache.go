package filter

import (
	"bufio"
	"os"
	"regexp"
	"strings"
	"time"
)

var cache *CacheStorage

type retentionMatcher struct {
	pattern   *regexp.Regexp
	retention int
}

type CacheStorage struct {
	retentions []retentionMatcher
}

// NewCacheStorage create new CacheStorage
func NewCacheStorage(retentionConfigFile *os.File) (*CacheStorage, error) {

	storage := &CacheStorage{}
	if err := storage.BuildRetentions(bufio.NewScanner(retentionConfigFile)); err != nil {
		return nil, err
	}

	return storage, nil
}

// BuildRetentions build cache storage retention matchers
func (cs *CacheStorage) BuildRetentions(retentionScanner *bufio.Scanner) error {
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

// Save buffered save matched metrics
func (cs *CacheStorage) Save(ch chan *MatchedMetric, save func([]*MatchedMetric)) {
	buffer := make([]*MatchedMetric, 0, 10)
	timeout := time.NewTimer(time.Second)
	for {
		select {
		case m, ok := <-ch:
			if !ok {
				return
			}
			buffer = append(buffer, m)
			if len(buffer) < 10 {
				continue
			}
			break
		case <-timeout.C:
			break
		}
		if len(buffer) == 0 {
			continue
		}
		timer := time.Now()
		save(buffer)
		SavingTimer.UpdateSince(timer)
		buffer = make([]*MatchedMetric, 0, 10)
	}
}

// SavePoints saving matched metrics to DB
func (cs *CacheStorage) SavePoints(buffer []*MatchedMetric, db *DbConnector) error {

	for _, m := range buffer {
		for _, matcher := range cs.retentions {
			if matcher.pattern.MatchString(m.Metric) {
				m.Retention = matcher.retention
				break
			}
		}
		m.Timestamp = roundToNearestRetention(m.Timestamp, m.Retention)

	}

	if err := db.saveMetrics(buffer); err != nil {
		return err
	}

	return nil
}
