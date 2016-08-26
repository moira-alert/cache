package filter

import (
	"bufio"
	"regexp"
	"strings"
	"time"
)

var cache *CacheStorage

type retentionMatcher struct {
	pattern   *regexp.Regexp
	retention int
}

// CacheStorage struct to store retention matchers
type CacheStorage struct {
	retentions      []retentionMatcher
	retentionsCache map[string]*retentionCacheItem
	metricsCache    map[string]*MatchedMetric
}

// NewCacheStorage create new CacheStorage
func NewCacheStorage(retentionScanner *bufio.Scanner) (*CacheStorage, error) {

	storage := &CacheStorage{
		retentionsCache: make(map[string]*retentionCacheItem),
		metricsCache:    make(map[string]*MatchedMetric),
	}
	if err := storage.buildRetentions(retentionScanner); err != nil {
		return nil, err
	}

	return storage, nil
}

func (cs *CacheStorage) buildRetentions(retentionScanner *bufio.Scanner) error {
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

// ProcessMatchedMetrics make buffer of metrics and save it
func (cs *CacheStorage) ProcessMatchedMetrics(ch chan *MatchedMetric, save func([]*MatchedMetric)) {
	buffer := make([]*MatchedMetric, 0, 10)
	for {
		select {
		case m, ok := <-ch:
			if !ok {
				return
			}

			if cs.ProcessMatchedMetric(m) {
				buffer = append(buffer, m)
			}

			if len(buffer) < 10 {
				continue
			}
			break
		case <-time.After(time.Second):
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

// ProcessMatchedMetric calculate retention and filter cached values
func (cs *CacheStorage) ProcessMatchedMetric(m *MatchedMetric) bool {
	m.Retention = cs.GetRetention(m)
	m.RetentionTimestamp = roundToNearestRetention(m.Timestamp, int64(m.Retention))

	if ex, ok := cs.metricsCache[m.Metric]; ok && ex.RetentionTimestamp == m.RetentionTimestamp && ex.Value == m.Value {
		return false
	}
	cs.metricsCache[m.Metric] = m

	return true
}

// SavePoints saving matched metrics to DB
func (cs *CacheStorage) SavePoints(buffer []*MatchedMetric, db *DbConnector) error {

	if err := db.saveMetrics(buffer); err != nil {
		return err
	}

	return nil
}
