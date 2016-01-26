package filter

import (
	"sync/atomic"
	"github.com/rcrowley/go-metrics"
)

var (
	TotalMetricsReceived    metrics.Meter
	ValidMetricsReceived    metrics.Meter
	MatchingMetricsReceived metrics.Meter
	MatchingTimer           metrics.Timer
	SavingTimer             metrics.Timer
	BuildTreeTimer          metrics.Timer
)

// InitGraphiteMetrics initialize graphite metrics
func InitGraphiteMetrics() {
	TotalMetricsReceived = metrics.NewRegisteredMeter("received.total", metrics.DefaultRegistry)
	ValidMetricsReceived = metrics.NewRegisteredMeter("received.valid", metrics.DefaultRegistry)
	MatchingMetricsReceived = metrics.NewRegisteredMeter("received.matching", metrics.DefaultRegistry)
	MatchingTimer = metrics.NewRegisteredTimer("time.match", metrics.DefaultRegistry)
	SavingTimer = metrics.NewRegisteredTimer("time.save", metrics.DefaultRegistry)
	BuildTreeTimer = metrics.NewRegisteredTimer("time.buildtree", metrics.DefaultRegistry)
	totalReceived = 0
	validReceived = 0
	matchedReceived = 0
}

// UpdateProcessingMetrics update processing metrics on demand
func UpdateProcessingMetrics() {
	TotalMetricsReceived.Mark(atomic.SwapInt64(&totalReceived, int64(0)))
	ValidMetricsReceived.Mark(atomic.SwapInt64(&validReceived, int64(0)))
	MatchingMetricsReceived.Mark(atomic.SwapInt64(&matchedReceived, int64(0)))
}
