package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	"github.com/rcrowley/go-metrics"
)

type dbMock struct {
	patterns   map[string]bool
	metrics    map[string]string
	retentions map[string]int
}

var testDb *dbMock

func (db *dbMock) GetPatterns() ([]string, error) {
	keys := make([]string, 0, len(db.patterns))
	for k := range db.patterns {
		keys = append(keys, k)
	}
	return keys, nil
}

func (db *dbMock) SaveMetrics(buffer []*matchedMetric) error {
	for _, m := range buffer {
		db.metrics[m.metric] = fmt.Sprintf("%v %v", m.value, m.timestamp)
		db.retentions[m.metric] = m.retention
	}
	return nil
}

var tcReport = flag.Bool("teamcity", false, "enable TeamCity reporting format")

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
	}

	invalidRawMetrics := []string{
		"No.value.no.timestamp",
		"Too.many.parts 1 2 3 4",
		"Invalid.value 12g5 1234567890",
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

	BeforeSuite(func() {
		testDb = &dbMock{make(map[string]bool), make(map[string]string), make(map[string]int)}
		db = testDb
		for _, pattern := range testPatterns {
			testDb.patterns[pattern] = true
		}
		patterns = newPatternStorage()
		patterns.doRefresh()
		cache = &cacheStorage{}
		cache.buildRetentions(bufio.NewScanner(strings.NewReader(testRetentions)))
	})

	BeforeEach(func() {
		totalMetricsReceived = metrics.NewRegisteredMeter("received.total", metrics.DefaultRegistry)
		validMetricsReceived = metrics.NewRegisteredMeter("received.valid", metrics.DefaultRegistry)
		matchingMetricsReceived = metrics.NewRegisteredMeter("received.matching", metrics.DefaultRegistry)
		matchingTimer = metrics.NewRegisteredTimer("time.match", metrics.DefaultRegistry)
		savingTimer = metrics.NewRegisteredTimer("time.save", metrics.DefaultRegistry)
	})

	Context("When invalid metric arrives", func() {
		BeforeEach(func() {
			for _, metric := range invalidRawMetrics {
				process(metric)
			}
		})

		It("should be properly counted", func() {
			Expect(int(totalMetricsReceived.Count())).To(Equal(len(invalidRawMetrics)))
			Expect(int(validMetricsReceived.Count())).To(Equal(0))
			Expect(int(matchingMetricsReceived.Count())).To(Equal(0))
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
					process("\000" + metric + " 12 1234567890")
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
		Expect(int(totalMetricsReceived.Count())).To(Equal(len(matchingMetrics)))
		Expect(int(validMetricsReceived.Count())).To(Equal(len(matchingMetrics)))
		Expect(int(matchingMetricsReceived.Count())).To(Equal(len(matchingMetrics)))
	})

	It("should appear in cache", func() {
		for _, metric := range matchingMetrics {
			_, exists := testDb.metrics[metric]
			Expect(exists).To(Equal(true))
		}
	})

	It("should have correct retention", func() {
		for _, metric := range matchingMetrics {
			retention := 120
			if strings.HasPrefix(metric, "Simple") {
				retention = 60
			} else if strings.HasSuffix(metric, "suf") {
				retention = 1200
			}
			Expect(testDb.retentions[metric]).To(Equal(retention))
		}
	})

	It("should have timestamp rounded to nearest retention", func() {
		for _, metric := range matchingMetrics {
			value := "12 1234567920"
			if strings.HasPrefix(metric, "Simple") {
				value = "12 1234567920"
			} else if strings.HasSuffix(metric, "suf") {
				value = "12 1234568400"
			}
			Expect(testDb.metrics[metric]).To(Equal(value))
		}
	})

}

func process(metric string) {
	if m := processIncomingMetric(metric); m != nil {
		buffer := []*matchedMetric{m}
		cache.savePoints(buffer)
	}
}

func assertNonMatchedMetrics(nonMatchingMetrics []string) {

	It("should be properly counted", func() {
		Expect(int(totalMetricsReceived.Count())).To(Equal(len(nonMatchingMetrics)))
		Expect(int(validMetricsReceived.Count())).To(Equal(len(nonMatchingMetrics)))
		Expect(int(matchingMetricsReceived.Count())).To(Equal(0))
	})

	It("should not appear in cache", func() {
		for _, metric := range nonMatchingMetrics {
			_, exists := testDb.metrics[metric]
			Expect(exists).To(Equal(false))
			_, exists = testDb.retentions[metric]
			Expect(exists).To(Equal(false))
		}
	})

}
