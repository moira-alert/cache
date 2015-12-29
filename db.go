package main

import (
	"fmt"
	"time"

	"github.com/garyburd/redigo/redis"
)

type dbConnector struct {
	pool *redis.Pool
}

func getMetricDbKey(metric string) string{
	return fmt.Sprintf("moira-metric:%s", metric)
}

func getMetricRetentionDbKey(metric string) string{
	return fmt.Sprintf("moira-metric-retention:%s", metric)
}

func (connector *dbConnector) getPatterns() ([]string, error) {
	c := connector.pool.Get()
	defer c.Close()
	return redis.Strings(c.Do("SMEMBERS", "moira-pattern-list"))
}

func (connector *dbConnector) saveMetrics(buffer []*matchedMetric) error {

	c := connector.pool.Get()
	defer c.Close()

	for _, m := range buffer {

		metricKey := getMetricDbKey(m.metric)
		metricRetentionKey := getMetricRetentionDbKey(m.metric)
		
		metricValue := fmt.Sprintf("%v %v", m.value, m.timestamp)

		c.Send("ZADD", metricKey, m.timestamp, metricValue)
		c.Send("SET", metricRetentionKey, m.retention)

		for _, pattern := range m.patterns {
			event, err := makeEvent(pattern, m.metric)
			if err != nil {
				continue
			}
			c.Send("PUBLISH", "metric-event", event)
		}
	}
	return c.Flush()
}

func newDbConnector(redisURI string) *dbConnector {
	return &dbConnector{
		pool: newRedisPool(redisURI),
	}
}

func newRedisPool(redisURI string, dbID ...int) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     10,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", redisURI)
			if err != nil {
				return nil, err
			}
			if len(dbID) > 0 {
				c.Do("SELECT", dbID[0])
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}
