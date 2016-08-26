package filter

var defaultRetention = 60

type retentionCacheItem struct {
    value int
    timestamp int64
}

// GetRetention returns first matched retention for metric
func (cs *CacheStorage) GetRetention(m *MatchedMetric) int {
    if item, ok := cs.retentionsCache[m.Metric]; ok && item.timestamp + 60 > m.Timestamp {
        return item.value
    }
	for _, matcher := range cs.retentions {
		if matcher.pattern.MatchString(m.Metric) {
            cs.retentionsCache[m.Metric] = &retentionCacheItem{
                value: matcher.retention,
                timestamp: m.Timestamp,
            }
			return matcher.retention
		}
	}
	return defaultRetention
}
