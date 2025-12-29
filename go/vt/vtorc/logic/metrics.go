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
	metricTickMu        sync.Mutex
)

// initMetricsTick is called once in the lifetime of the app, after config has been loaded
func initMetricsTick(ctx context.Context) error {
	go func() {
		metricsCallbackTicker := time.NewTicker(time.Duration(config.DebugMetricsIntervalSeconds) * time.Second)
		defer metricsCallbackTicker.Stop()
		for {
			select {
			case <-ctx.Done():
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

func onMetricsTick(f func()) {
	metricTickMu.Lock()
	defer metricTickMu.Unlock()
	metricTickCallbacks = append(metricTickCallbacks, f)
}

func resetMetricsTicks() {
	metricTickMu.Lock()
	defer metricTickMu.Unlock()
	metricTickCallbacks = [](func()){}
}
