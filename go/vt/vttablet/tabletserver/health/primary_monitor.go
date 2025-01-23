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
		interval:    interval,
		pingNowChan: make(chan struct{}, 1),
	}
}

// TMClientPrimaryMonitor monitors the health of a primary by pinging it using grpctmclient.
type TMClientPrimaryMonitor struct {
	cancel      context.CancelFunc
	ctx         context.Context
	interval    time.Duration
	mu          sync.Mutex
	opened      bool
	pingNowChan chan struct{}
	primary     *topodatapb.Tablet
	reachable   uint32
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
		pm.pingNowChan <- struct{}{}
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
	var reachable uint32
	if err := tmc.Ping(ctx, primary); err != nil {
		log.Errorf("Failed to ping primary %s: %+v", topoproto.TabletAliasString(primary.Alias), err)
	} else {
		reachable = 1
	}
	atomic.StoreUint32(&pm.reachable, reachable)
}

// poll pings the primary periodically and on-demand when the address changes.
func (pm *TMClientPrimaryMonitor) poll(firstPingChan chan struct{}) {
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
		case <-pm.ctx.Done():
			return
		case <-pm.pingNowChan:
			pm.ping(tmc, pm.getPrimary())
		case <-ticker.C:
			pm.ping(tmc, pm.getPrimary())
		}
	}
}

// Open opens the primary health monitor. This causes an initial ping, then periodic/on-demand pings in a loop.
func (pm *TMClientPrimaryMonitor) Open() error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if pm.opened {
		return ErrPrimaryMonitorOpen
	}

	pm.ctx, pm.cancel = context.WithCancel(context.Background())
	firstPingChan := make(chan struct{}, 1)
	go pm.poll(firstPingChan)
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

// IsReachable returns nil if the primary is reachable or an error.
func (pm *TMClientPrimaryMonitor) IsReachable() error {
	if atomic.LoadUint32(&pm.reachable) != 1 {
		return ErrPrimaryUnreachable
	}
	return nil
}
