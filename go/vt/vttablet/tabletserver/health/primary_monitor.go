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
	ErrPrimaryHealthMonitorOpen = vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION,
		"primary health pinger is already open")
	ErrPrimaryUnreachable = vterrors.New(vtrpcpb.Code_UNAVAILABLE,
		"primary is unreachable")
	PrimaryPingTimeout = time.Second * 5
)

func NewPrimaryHealthMonitor(interval time.Duration) *tmPrimaryHealthMonitor {
	return &tmPrimaryHealthMonitor{
		interval: interval,
		pingChan: make(chan struct{}, 1),
	}
}

type tmPrimaryHealthMonitor struct {
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
func (phm *tmPrimaryHealthMonitor) getPrimary() *topodatapb.Tablet {
	phm.mu.Lock()
	defer phm.mu.Unlock()
	return phm.primary
}

// SetPrimary sets the primary to monitor under lock.
func (phm *tmPrimaryHealthMonitor) SetPrimary(primary *topodatapb.Tablet) {
	phm.mu.Lock()
	defer phm.mu.Unlock()
	phm.primary = primary
	if phm.opened {
		phm.pingChan <- struct{}{}
	}
}

// ping performs a Ping RPC to the tmserver of a primary.
func (phm *tmPrimaryHealthMonitor) ping(tmc *grpctmclient.Client, primary *topodatapb.Tablet) {
	if tmc == nil {
		return
	}
	if primary == nil {
		return
	}

	ctx, cancel := context.WithTimeout(phm.ctx, PrimaryPingTimeout)
	defer cancel()
	if err := tmc.Ping(ctx, primary); err != nil {
		log.Errorf("Failed to ping primary %s: %+v", topoproto.TabletAliasString(primary.Alias), err)
		return
	}
	atomic.StoreUint32(&phm.reachable, 1)
}

// Open opens the primary health monitor. This causes an initial ping, then periodic/on-demand pings in a loop.
func (phm *tmPrimaryHealthMonitor) Open() error {
	phm.mu.Lock()
	defer phm.mu.Unlock()

	if phm.opened {
		return ErrPrimaryHealthMonitorOpen
	}

	firstPingChan := make(chan struct{}, 1)
	phm.ctx, phm.cancel = context.WithCancel(context.Background())
	go func(ctx context.Context) {
		tmc := grpctmclient.NewClient()
		defer tmc.Close()

		// initial ping
		phm.ping(tmc, phm.primary)
		close(firstPingChan)

		// periodic/on-demand ping
		ticker := time.NewTicker(phm.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-phm.pingChan:
				phm.ping(tmc, phm.getPrimary())
			case <-ticker.C:
				phm.ping(tmc, phm.getPrimary())
			}
		}
	}(phm.ctx)
	<-firstPingChan

	phm.opened = true
	return nil
}

// Close stops background pings and closes the primary health monitor.
func (phm *tmPrimaryHealthMonitor) Close() {
	phm.mu.Lock()
	defer phm.mu.Unlock()
	if phm.cancel != nil {
		phm.cancel()
	}
	atomic.StoreUint32(&phm.reachable, 0)
	phm.opened = false
	phm.primary = nil
}

func (phm *tmPrimaryHealthMonitor) IsReachable() error {
	if atomic.LoadUint32(&phm.reachable) != 1 {
		return ErrPrimaryUnreachable
	}
	return nil
}
