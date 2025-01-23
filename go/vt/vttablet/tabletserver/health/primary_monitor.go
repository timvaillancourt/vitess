/*
Copyright 2025 The Vitess Authors.

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

package health

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/vt/log"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/grpctmclient"
)

var (
	ErrPrimaryMonitorOpen = vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION,
		"primary monitor is already open")
	ErrPrimaryUnreachable = vterrors.New(vtrpcpb.Code_UNAVAILABLE,
		"primary is unreachable")
	PrimaryPingTimeout = time.Second * 5
)

// NewPrimaryMonitor creates a TMClientPrimaryMonitor.
func NewPrimaryMonitor(interval time.Duration) *TMClientPrimaryMonitor {
	return &TMClientPrimaryMonitor{
		interval: interval,
		pingChan: make(chan struct{}, 1),
	}
}

// TMClientPrimaryMonitor monitors the health of a primary by pinging it using grpctmclient.
type TMClientPrimaryMonitor struct {
	cancel    context.CancelFunc
	ctx       context.Context
	interval  time.Duration
	mu        sync.Mutex
	opened    bool
	pingChan  chan struct{}
	primary   *topodatapb.Tablet
	reachable uint32
}

// getPrimary returns the primary to monitor under lock.
func (pm *TMClientPrimaryMonitor) getPrimary() *topodatapb.Tablet {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return pm.primary
}

// SetPrimary sets the primary to monitor under lock.
func (pm *TMClientPrimaryMonitor) SetPrimary(primary *topodatapb.Tablet) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.primary = primary
	if pm.opened {
		pm.pingChan <- struct{}{}
	}
}

// ping performs a Ping RPC to the tmserver of a primary.
func (pm *TMClientPrimaryMonitor) ping(tmc *grpctmclient.Client, primary *topodatapb.Tablet) {
	if tmc == nil {
		return
	}
	if primary == nil {
		return
	}

	ctx, cancel := context.WithTimeout(pm.ctx, PrimaryPingTimeout)
	defer cancel()
	if err := tmc.Ping(ctx, primary); err != nil {
		log.Errorf("Failed to ping primary %s: %+v", topoproto.TabletAliasString(primary.Alias), err)
		return
	}
	atomic.StoreUint32(&pm.reachable, 1)
}

// Open opens the primary health monitor. This causes an initial ping, then periodic/on-demand pings in a loop.
func (pm *TMClientPrimaryMonitor) Open() error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if pm.opened {
		return ErrPrimaryMonitorOpen
	}

	firstPingChan := make(chan struct{}, 1)
	pm.ctx, pm.cancel = context.WithCancel(context.Background())
	go func(ctx context.Context) {
		tmc := grpctmclient.NewClient()
		defer tmc.Close()

		// initial ping
		pm.ping(tmc, pm.primary)
		close(firstPingChan)

		// periodic/on-demand ping
		ticker := time.NewTicker(pm.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-pm.pingChan:
				pm.ping(tmc, pm.getPrimary())
			case <-ticker.C:
				pm.ping(tmc, pm.getPrimary())
			}
		}
	}(pm.ctx)
	<-firstPingChan

	pm.opened = true
	return nil
}

// Close stops background pings and closes the primary health monitor.
func (pm *TMClientPrimaryMonitor) Close() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if pm.cancel != nil {
		pm.cancel()
	}
	atomic.StoreUint32(&pm.reachable, 0)
	pm.opened = false
	pm.primary = nil
}

func (pm *TMClientPrimaryMonitor) IsReachable() error {
	if atomic.LoadUint32(&pm.reachable) != 1 {
		return ErrPrimaryUnreachable
	}
	return nil
}
