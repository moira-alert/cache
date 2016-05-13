package perftests

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"
	"math/rand"

	"github.com/garyburd/redigo/redis"
	"github.com/gmlexx/redigomock"
	"github.com/moira-alert/cache/filter"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var (
	db       *filter.DbConnector
	cache    *filter.CacheStorage
	patterns *filter.PatternStorage

	testMetricsLines []string
)

func TestCache(t *testing.T) {
	flag.Parse()

	RegisterFailHandler(Fail)
	RunSpecs(t, "Cache Suite")
}

var _ = Describe("Cache", func() {
	testRetentions := `
	# comment
	[simple]
	pattern = ^Simple\.
	retentions = 60s:2d,10m:30d,100m:90d

	[rare]
	pattern = suf$
	retentions = 20m:30d,8h:1y

	[hourly]
	pattern = hourly$
	retentions = 1h:1d

	[daily]
	pattern = daily$
	retentions = 1d:1w

	[weekly]
	pattern = weekly$
	retentions = 1w:1y

	[yearly]
	pattern = yearly$
	retentions = 1y:100y

	[default]
	pattern = .*
	retentions = 120:7d
	`

	BeforeEach(func() {
		c := redigomock.NewFakeRedis()
		db = &filter.DbConnector{
			Pool: &redis.Pool{
				MaxIdle:     3,
				IdleTimeout: 240 * time.Second,
				Dial: func() (redis.Conn, error) {
					return c, nil
				},
			},
		}
		patternsTxt, err := os.Open("patterns.txt")
		Expect(err).ShouldNot(HaveOccurred())
		patternsReader := bufio.NewReader(patternsTxt)
		for {
			pattern, _, err := patternsReader.ReadLine()
			if err != nil {
				break
			}
			c.Do("SADD", "moira-pattern-list", string(pattern))
		}
		if err != nil && err != io.EOF {
			fmt.Printf("Error reading patterns: %s", err.Error())
		}


		filter.InitGraphiteMetrics()

		patterns = filter.NewPatternStorage()
		patterns.DoRefresh(db)
		cache = &filter.CacheStorage{}
		cache.BuildRetentions(bufio.NewScanner(strings.NewReader(testRetentions)))

		testMetricsLines = generateMetrics(patterns, 100000)
	})

	Context("Without saving", func() {
		Measure("metrics processing", func(b Benchmarker) {
			runtime := b.Time("runtime", func() {
				for _, line := range testMetricsLines {
					patterns.ProcessIncomingMetric([]byte(line))
				}
			})
			Expect(runtime.Seconds()).Should(BeNumerically("<", 1), "metrics processing shouldn't take too long.")
			b.RecordValue("metrics per sec", float64(len(testMetricsLines)) / runtime.Seconds())
			filter.UpdateProcessingMetrics()
			b.RecordValue("matched", float64(filter.MatchingMetricsReceived.Count()))
		}, 10)
	})
})

func generateMetrics(patterns *filter.PatternStorage, count int) []string{

	result := make([]string, 0, count)

	timestamp := time.Now()

	i := 0

	for i < count {
		parts := make([]string, 0, 16)

		node := patterns.PatternTree.Children[rand.Intn(len(patterns.PatternTree.Children))]
		matched := rand.Float64() < 0.02
		level := float64(0)
		for {
			part := node.Part
			if len(node.InnerParts) > 0 {
				part = node.InnerParts[rand.Intn(len(node.InnerParts))]
			}
			if !matched && rand.Float64() < 0.2 + level {
				part = RandStringBytes(len(part))
			}
			parts = append(parts, strings.Replace(part, "*", "XXXXXXXXX", -1))
			if len(node.Children) == 0 {
				break
			}
			level += 0.7
			node = node.Children[rand.Intn(len(node.Children))]
		}
		value := rand.Float32()
		ts := fmt.Sprintf("%d", timestamp.Unix())
		v := fmt.Sprintf("%f", value)
		path := strings.Join(parts, ".")
		metric := strings.Join([]string{path, v, ts}, " ")
		result = append(result, metric)
		i ++
		timestamp = timestamp.Add(time.Microsecond)
	}

	return result
}

const letterBytes = "abcdefghijklmnopqrstuvwxyz"

func RandStringBytes(n int) string {
    b := make([]byte, n)
    for i := range b {
        b[i] = letterBytes[rand.Intn(len(letterBytes))]
    }
    return string(b)
}
