/*
   Copyright 2014 Outbrain Inc.

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

package logic

import (
	"context"
	"sync"
	"time"

	"vitess.io/vitess/go/vt/vtorc/config"
)

var (
	metricTickCallbacks [](func())
	metricTickCancel    context.CancelFunc
	metricTickCtx       context.Context
	metricTickMu        sync.Mutex
)

// initMetricsTick is called to init the metrics ticker.
func initMetricsTick() error {
	metricTickMu.Lock()
	defer metricTickMu.Unlock()

	if metricTickCtx != nil {
		return nil
	}

	metricTickCtx, metricTickCancel = context.WithCancel(context.Background())
	go func() {
		metricsCallbackTicker := time.NewTicker(time.Duration(config.DebugMetricsIntervalSeconds) * time.Second)
		defer metricsCallbackTicker.Stop()
		for {
			select {
			case <-metricTickCtx.Done():
				metricTickMu.Lock()
				defer metricTickMu.Unlock()
				metricTickCancel = nil
				metricTickCtx = nil
				return
			case <-metricsCallbackTicker.C:
				metricTickMu.Lock()
				defer metricTickMu.Unlock()
				for _, f := range metricTickCallbacks {
					go f()
				}
			}
		}
	}()

	return nil
}

// onMetricsTick adds a func to be ran on each metric tick.
func onMetricsTick(f func()) {
	metricTickMu.Lock()
	defer metricTickMu.Unlock()
	metricTickCallbacks = append(metricTickCallbacks, f)
}

// stopAndResetMetricsTicks stops the metric ticker and
// resets the list of scheduled funcs.
func stopAndResetMetricsTicks() {
	metricTickMu.Lock()
	defer metricTickMu.Unlock()
	if metricTickCtx == nil {
		return
	}
	metricTickCancel()
	metricTickCallbacks = [](func()){}
}
