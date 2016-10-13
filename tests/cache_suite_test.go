package tests

import (
	"bufio"
	"flag"
	"fmt"
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

var _ = Describe("Cache unit tests", func() {
	Describe("ParseMetricFromString", func() {
		Context("Given invalid metric strings", func() {
			invalidMetrics := []string{
				"Invalid.value 12g5 1234567890",
				"No.value.two.spaces  1234567890",
				"No.timestamp.space.in.the.end 123 ",
				"No.timestamp 123",
				" 123 1234567890",
				"Non-ascii.こんにちは 12 1234567890",
				"Non-printable.\000 12 1234567890",
				"",
				"\n",
				"Too.many.parts 1 2 3 4 12 1234567890",
				"Space.in.the.end 12 1234567890 ",
				" Space.in.the.beginning 12 1234567890",
				"\tNon-printable.in.the.beginning 12 1234567890",
				"\rNon-printable.in.the.beginning 12 1234567890",
				"Newline.in.the.end 12 1234567890\n",
			}

			It("should return errors", func() {
				for _, invalidMetric := range invalidMetrics {
					_, _, _, err := filter.ParseMetricFromString([]byte(invalidMetric))
					Expect(err).To(HaveOccurred(), "failed metric: '%s'", invalidMetric)
				}
			})
		})

		Context("Given valid metric strings", func() {
			type m struct {
				raw       string
				metric    string
				value     float64
				timestamp int64
			}
			validMetrics := []m{
				m{"One.two.three 123 1234567890", "One.two.three", 123, 1234567890},
				m{"One.two.three 1.23e2 1234567890", "One.two.three", 123, 1234567890},
				m{"One.two.three -123 1234567890", "One.two.three", -123, 1234567890},
				m{"One.two.three +123 1234567890", "One.two.three", 123, 1234567890},
				m{"One.two.three 123. 1234567890", "One.two.three", 123, 1234567890},
				m{"One.two.three 123.0 1234567890", "One.two.three", 123, 1234567890},
				m{"One.two.three .123 1234567890", "One.two.three", 0.123, 1234567890},
			}
			It("should return parsed values", func() {
				for _, validMetric := range validMetrics {
					metric, value, timestamp, err := filter.ParseMetricFromString([]byte(validMetric.raw))
					Expect(err).NotTo(HaveOccurred(), "failed metric: '%s'", validMetric)
					Expect(metric).To(Equal([]byte(validMetric.metric)), "failed metric: '%s'", validMetric)
					Expect(value).To(Equal(validMetric.value), "failed metric: '%s'", validMetric)
					Expect(timestamp).To(Equal(validMetric.timestamp), "failed metric: '%s'", validMetric)
				}
			})
		})
	})
})

var _ = Describe("Cache functional tests", func() {
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

	nonMatchingMetrics := []string{
		"Simple.notmatching.pattern",
		"Star.nothing",
		"Bracket.one.nothing",
		"Bracket.nothing.pattern",
		"Complex.prefixonesuffix",
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
		var err error
		cache, err = filter.NewCacheStorage(bufio.NewScanner(strings.NewReader(testRetentions)))
		if err != nil {
			Fail(fmt.Sprintf("Can not create new cache storage %s", err))
		}
	})

	Context("When invalid metric arrives", func() {
		BeforeEach(func() {
			process("Invalid.metric")
		})

		It("should be properly counted", func() {
			filter.UpdateProcessingMetrics()
			Expect(int(filter.TotalMetricsReceived.Count())).To(Equal(1))
			Expect(int(filter.ValidMetricsReceived.Count())).To(Equal(0))
			Expect(int(filter.MatchingMetricsReceived.Count())).To(Equal(0))
		})
	})

	Context("When valid non-matching metric arrives", func() {
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
			Expect(err).ShouldNot(HaveOccurred(), "failed metric: '%s'", metric)
			Expect(count).To(Equal(1), "failed metric: '%s'", metric)
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
			Expect(err).ShouldNot(HaveOccurred(), "failed metric: '%s'", metric)
			Expect(result).To(Equal(retention), "failed metric: '%s'", metric)
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
			Expect(err).ShouldNot(HaveOccurred(), "failed metric: '%s'", metric)
			Expect(len(values)).To(Equal(2), "failed metric: '%s'", metric)
			Expect(values[1]).To(Equal(timestamp), "failed metric: '%s'", metric)
		}
	})

}

func process(metric string) {
	if m := patterns.ProcessIncomingMetric([]byte(metric)); m != nil {
		buffer := make(map[string]*filter.MatchedMetric)
		cache.EnrichMatchedMetric(buffer, m)
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
			Expect(err).ShouldNot(HaveOccurred(), "failed metric: '%s'", metric)
			Expect(count).To(Equal(0), "failed metric: '%s'", metric)
			retentionDbKey := filter.GetMetricRetentionDbKey(metric)
			result, err := c.Do("GET", retentionDbKey)
			Expect(err).ShouldNot(HaveOccurred(), "failed metric: '%s'", metric)
			Expect(result).To(BeNil(), "failed metric: '%s'", metric)
		}
	})

}
