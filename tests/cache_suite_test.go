package tests

import (
	"bufio"
	"flag"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/gmlexx/redigomock"
	"github.com/moira-alert/cache/filter"
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
)

var (
	tcReport = flag.Bool("teamcity", false, "enable TeamCity reporting format")
	db       *filter.DbConnector
	cache    *filter.CacheStorage
	patterns *filter.PatternStorage
)

func TestCache(t *testing.T) {
	flag.Parse()

	RegisterFailHandler(Fail)
	if *tcReport {
		RunSpecsWithCustomReporters(t, "Cache Suite", []Reporter{reporters.NewTeamCityReporter(os.Stdout)})
	} else {
		RunSpecs(t, "Cache Suite")
	}
}

var _ = Describe("Cache", func() {
	testPatterns := []string{
		"Simple.matching.pattern",
		"Star.single.*",
		"Star.*.double.any*",
		"Bracket.{one,two,three}.pattern",
		"Bracket.pr{one,two,three}suf",
		"Complex.matching.pattern",
		"Complex.*.*",
		"Complex.*{one,two,three}suf*.pattern",
		"Question.?at_begin",
		"Question.at_the_end?",
	}

	invalidRawMetrics := []string{
		"No.value.no.timestamp",
		"Invalid.value 12g5 1234567890",
		"\n\t",
	}

	nonMatchingMetrics := []string{
		"Simple.notmatching.pattern",
		"Star.nothing",
		"Bracket.one.nothing",
		"Bracket.nothing.pattern",
		"Complex.prefixonesuffix",
		"Too.many.parts 1 2 3 4",
	}

	matchingMetrics := []string{
		"Simple.matching.pattern",
		"Star.single.anything",
		"Star.anything.double.anything",
		"Bracket.one.pattern",
		"Bracket.two.pattern",
		"Bracket.three.pattern",
		"Bracket.pronesuf",
		"Bracket.prtwosuf",
		"Bracket.prthreesuf",
		"Complex.matching.pattern",
		"Complex.anything.pattern",
		"Complex.prefixonesuffix.pattern",
		"Complex.prefixtwofix.pattern",
		"Complex.anything.pattern",
		"Question.1at_begin",
		"Question.at_the_end2",
	}

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
		db = filter.NewDbConnector(&redis.Pool{
				MaxIdle:     3,
				IdleTimeout: 240 * time.Second,
				Dial: func() (redis.Conn, error) {
					return c, nil
				},
			})
		for _, pattern := range testPatterns {
			c.Do("SADD", "moira-pattern-list", pattern)
		}

		filter.InitGraphiteMetrics()

		patterns = filter.NewPatternStorage()
		patterns.DoRefresh(db)
		cache = &filter.CacheStorage{}
		cache.BuildRetentions(bufio.NewScanner(strings.NewReader(testRetentions)))
	})

	Context("When invalid metric arrives", func() {
		BeforeEach(func() {
			for _, metric := range invalidRawMetrics {
				process(metric)
			}
		})

		It("should be properly counted", func() {
			filter.UpdateProcessingMetrics()
			Expect(int(filter.TotalMetricsReceived.Count())).To(Equal(len(invalidRawMetrics)))
			Expect(int(filter.ValidMetricsReceived.Count())).To(Equal(0))
			Expect(int(filter.MatchingMetricsReceived.Count())).To(Equal(0))
		})
	})

	Context("When valid non-matching metric arrives", func() {

		Context("When metric arrives without timestamp", func() {
			BeforeEach(func() {
				for _, metric := range nonMatchingMetrics {
					process(metric + " 12")
				}
			})
			assertNonMatchedMetrics(nonMatchingMetrics)
		})

		Context("When metric arrives with timestamp", func() {
			BeforeEach(func() {
				for _, metric := range nonMatchingMetrics {
					process(metric + " 12 1234567890")
				}
			})
			assertNonMatchedMetrics(nonMatchingMetrics)
		})
	})

	Context("When valid matching metric arrives", func() {
		Context("When metric name is pure", func() {
			BeforeEach(func() {
				for _, metric := range matchingMetrics {
					process(metric + " 12 1234567890")
				}
			})
			assertMatchedMetrics(matchingMetrics)
		})
		Context("When metric name contains non-printable characters", func() {
			BeforeEach(func() {
				for _, metric := range matchingMetrics {
					process("\000" + metric + "\r 12 1234567890 \r")
				}
			})
			assertMatchedMetrics(matchingMetrics)
		})
		Context("When value has dot", func() {
			BeforeEach(func() {
				for _, metric := range matchingMetrics {
					process(metric + " 12.000000 1234567890")
				}
			})
			assertMatchedMetrics(matchingMetrics)
		})
	})
})

func assertMatchedMetrics(matchingMetrics []string) {
	It("should be properly counted", func() {
		filter.UpdateProcessingMetrics()
		Expect(int(filter.TotalMetricsReceived.Count())).To(Equal(len(matchingMetrics)))
		Expect(int(filter.ValidMetricsReceived.Count())).To(Equal(len(matchingMetrics)))
		Expect(int(filter.MatchingMetricsReceived.Count())).To(Equal(len(matchingMetrics)))
	})

	It("should appear in cache", func() {
		c := db.Pool.Get()
		defer c.Close()

		for _, metric := range matchingMetrics {
			dbKey := filter.GetMetricDbKey(metric)
			count, err := redis.Int(c.Do("ZCOUNT", dbKey, "-inf", "+inf"))
			Expect(err).ShouldNot(HaveOccurred())
			Expect(count).To(Equal(1))
		}
	})

	It("should have correct retention", func() {
		c := db.Pool.Get()
		defer c.Close()

		for _, metric := range matchingMetrics {
			retention := 120
			if strings.HasPrefix(metric, "Simple") {
				retention = 60
			} else if strings.HasSuffix(metric, "suf") {
				retention = 1200
			}
			dbKey := filter.GetMetricRetentionDbKey(metric)
			result, err := redis.Int(c.Do("GET", dbKey))
			Expect(err).ShouldNot(HaveOccurred())
			Expect(result).To(Equal(retention))
		}
	})

	It("should have timestamp rounded to nearest retention", func() {
		c := db.Pool.Get()
		defer c.Close()

		for _, metric := range matchingMetrics {
			timestamp := "1234567920"
			if strings.HasPrefix(metric, "Simple") {
				timestamp = "1234567920"
			} else if strings.HasSuffix(metric, "suf") {
				timestamp = "1234568400"
			}
			dbKey := filter.GetMetricDbKey(metric)
			values, err := redis.Strings(c.Do("ZRANGE", dbKey, 0, -1, "WITHSCORES"))
			Expect(err).ShouldNot(HaveOccurred())
			Expect(len(values)).To(Equal(2))
			Expect(values[1]).To(Equal(timestamp))
		}
	})

}

func process(metric string) {
	if m := patterns.ProcessIncomingMetric([]byte(metric)); m != nil {
		buffer := []*filter.MatchedMetric{m}
		cache.SavePoints(buffer, db)
	}
}

func assertNonMatchedMetrics(nonMatchingMetrics []string) {

	It("should be properly counted", func() {
		filter.UpdateProcessingMetrics()
		Expect(int(filter.TotalMetricsReceived.Count())).To(Equal(len(nonMatchingMetrics)))
		Expect(int(filter.ValidMetricsReceived.Count())).To(Equal(len(nonMatchingMetrics)))
		Expect(int(filter.MatchingMetricsReceived.Count())).To(Equal(0))
	})

	It("should not appear in cache", func() {
		c := db.Pool.Get()
		defer c.Close()

		for _, metric := range nonMatchingMetrics {
			metricDbKey := filter.GetMetricDbKey(metric)
			count, err := redis.Int(c.Do("ZCOUNT", metricDbKey, "-inf", "+inf"))
			Expect(err).ShouldNot(HaveOccurred())
			Expect(count).To(Equal(0))
			retentionDbKey := filter.GetMetricRetentionDbKey(metric)
			result, err := c.Do("GET", retentionDbKey)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(result).To(BeNil())
		}
	})

}
