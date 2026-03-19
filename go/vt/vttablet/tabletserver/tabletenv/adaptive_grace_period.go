/*
Copyright 2024 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tabletenv

import (
	"sync"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/servenv"
)

const (
	// Adaptive grace period bounds (milliseconds).
	adaptiveGraceDefault = 25
	adaptiveGraceMin     = 10
	adaptiveGraceMax     = 50

	// Multiplier applied to the average MySQL response time to compute
	// the grace period. For example, if MySQL averages 80ms to return
	// errno 3024, the grace period is set to 80 * 1.5 = 120ms.
	adaptiveGraceMultiplier = 1.5

	// Number of 1-second intervals in the sliding window.
	adaptiveGraceWindowSize = 10

	// Minimum number of pushdown kills in the window needed to adjust
	// the grace period based on observed response times.
	adaptiveGraceMinSamples = 2
)

type (
	graceWindow struct {
		pushdown           int64
		fallback           int64
		pushdownDurationMs int64 // total pushdown response time in ms for this interval
	}

	// AdaptiveGracePeriod dynamically adjusts the grace period that vttablet
	// waits for MySQL to return an ERQueryTimeout (3024) from MAX_EXECUTION_TIME
	// before falling back to KILL QUERY.
	//
	// It tracks the average time MySQL takes to respond with errno 3024 over
	// a sliding window and sets the grace period to avg * 1.5, clamped to
	// [50ms, 1000ms]. This ensures vttablet gives MySQL just enough time to
	// handle the kill internally while falling back quickly if it doesn't.
	AdaptiveGracePeriod struct {
		gracePeriod atomic.Int64 // current grace period in milliseconds

		// Per-tick counters, atomically incremented by connections.
		pendingPushdown           atomic.Int64
		pendingFallback           atomic.Int64
		pendingPushdownDurationMs atomic.Int64

		// Sliding window of per-second snapshots.
		mu        sync.Mutex
		window    []graceWindow
		windowPos int

		// Exported metrics visible in /debug/vars.
		currentGauge        *stats.Gauge
		windowPushdownGauge *stats.Gauge
		windowFallbackGauge *stats.Gauge
		responseTimings     *servenv.TimingsWrapper

		done chan struct{}
		wg   sync.WaitGroup
	}
)

// NewAdaptiveGracePeriod creates and starts an adaptive grace period tracker.
func NewAdaptiveGracePeriod(exporter *servenv.Exporter) *AdaptiveGracePeriod {
	a := &AdaptiveGracePeriod{
		window:              make([]graceWindow, adaptiveGraceWindowSize),
		currentGauge:        exporter.NewGauge("KillPushdownGracePeriodMs", "Current adaptive grace period in milliseconds"),
		windowPushdownGauge: exporter.NewGauge("KillPushdownWindowPushdown", "Pushdown kills in the current sliding window"),
		windowFallbackGauge: exporter.NewGauge("KillPushdownWindowFallback", "Fallback kills in the current sliding window"),
		responseTimings:     exporter.NewTimings("KillPushdownResponseTime", "MySQL MAX_EXECUTION_TIME response time", "type"),
		done:                make(chan struct{}),
	}
	a.gracePeriod.Store(adaptiveGraceDefault)
	a.currentGauge.Set(adaptiveGraceDefault)
	a.wg.Add(1)
	go a.run()
	return a
}

// Duration returns the current adaptive grace period.
func (a *AdaptiveGracePeriod) Duration() time.Duration {
	return time.Duration(a.gracePeriod.Load()) * time.Millisecond
}

// RecordPushdownKill records that MySQL handled a kill via MAX_EXECUTION_TIME
// within the grace period (errno 3024). responseTime is the duration between
// context cancellation and MySQL's response.
func (a *AdaptiveGracePeriod) RecordPushdownKill(responseTime time.Duration) {
	a.pendingPushdown.Add(1)
	a.pendingPushdownDurationMs.Add(responseTime.Milliseconds())
	a.responseTimings.Add("Pushdown", responseTime)
}

// RecordFallbackKill records that the grace period expired and vttablet
// fell back to issuing KILL QUERY.
func (a *AdaptiveGracePeriod) RecordFallbackKill() {
	a.pendingFallback.Add(1)
}

// Stop stops the adaptive grace period goroutine.
func (a *AdaptiveGracePeriod) Stop() {
	close(a.done)
	a.wg.Wait()
}

func (a *AdaptiveGracePeriod) run() {
	defer a.wg.Done()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-a.done:
			return
		case <-ticker.C:
			a.tick()
		}
	}
}

func (a *AdaptiveGracePeriod) tick() {
	pushdown := a.pendingPushdown.Swap(0)
	fallback := a.pendingFallback.Swap(0)
	pushdownDurationMs := a.pendingPushdownDurationMs.Swap(0)

	a.mu.Lock()
	defer a.mu.Unlock()

	a.window[a.windowPos] = graceWindow{
		pushdown:           pushdown,
		fallback:           fallback,
		pushdownDurationMs: pushdownDurationMs,
	}
	a.windowPos = (a.windowPos + 1) % len(a.window)

	var totalPushdown, totalFallback, totalPushdownDurationMs int64
	for _, w := range a.window {
		totalPushdown += w.pushdown
		totalFallback += w.fallback
		totalPushdownDurationMs += w.pushdownDurationMs
	}

	a.windowPushdownGauge.Set(totalPushdown)
	a.windowFallbackGauge.Set(totalFallback)

	if totalPushdown < adaptiveGraceMinSamples {
		// Not enough data from successful pushdown kills to estimate
		// MySQL's response time. If we're seeing only fallbacks, reduce
		// the grace period toward the minimum since pushdown isn't working.
		if totalFallback >= adaptiveGraceMinSamples {
			current := a.gracePeriod.Load()
			if current > adaptiveGraceMin {
				newGrace := max(current/2, adaptiveGraceMin)
				a.gracePeriod.Store(newGrace)
				a.currentGauge.Set(newGrace)
			}
		}
		return
	}

	avgMs := totalPushdownDurationMs / totalPushdown

	// Set grace period to avg response time * multiplier, clamped to bounds.
	newGrace := int64(float64(avgMs) * adaptiveGraceMultiplier)
	newGrace = max(newGrace, adaptiveGraceMin)
	newGrace = min(newGrace, adaptiveGraceMax)

	a.gracePeriod.Store(newGrace)
	a.currentGauge.Set(newGrace)
}
