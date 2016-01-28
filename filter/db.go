package filter

import (
	"fmt"

	"github.com/garyburd/redigo/redis"
)

// DbConnector is DB layer client
type DbConnector struct {
	Pool  *redis.Pool
	cache map[string]*MatchedMetric
}

// NewDbConnector return db connector
func NewDbConnector(pool *redis.Pool) *DbConnector {
	return &DbConnector{
		Pool: pool,
		cache: make(map[string]*MatchedMetric),
	}
}

// GetMetricDbKey returns string redis key for metric
func GetMetricDbKey(metric string) string {
	return fmt.Sprintf("moira-metric-data:%s", metric)
}

// GetMetricRetentionDbKey returns string redis key for metric retention
func GetMetricRetentionDbKey(metric string) string {
	return fmt.Sprintf("moira-metric-retention:%s", metric)
}

func (connector *DbConnector) getPatterns() ([]string, error) {
	c := connector.Pool.Get()
	defer c.Close()
	return redis.Strings(c.Do("SMEMBERS", "moira-pattern-list"))
}

func (connector *DbConnector) saveMetrics(buffer []*MatchedMetric) error {

	c := connector.Pool.Get()
	defer c.Close()

	for _, m := range buffer {

		if ex, ok := connector.cache[m.Metric]; ok && ex.RetentionTimestamp == m.RetentionTimestamp && ex.Value == m.Value {
			continue
		}
		connector.cache[m.Metric] = m
		metricKey := GetMetricDbKey(m.Metric)
		metricRetentionKey := GetMetricRetentionDbKey(m.Metric)

		metricValue := fmt.Sprintf("%v %v", m.Timestamp, m.Value)

		c.Send("ZADD", metricKey, m.RetentionTimestamp, metricValue)
		c.Send("SET", metricRetentionKey, m.Retention)

		for _, pattern := range m.Patterns {
			event, err := makeEvent(pattern, m.Metric)
			if err != nil {
				continue
			}
			c.Send("PUBLISH", "metric-event", event)
		}
	}
	return c.Flush()
}
