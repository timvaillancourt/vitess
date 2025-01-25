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

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/log"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

var (
	ErrTabletMonitorOpen = vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION,
		"tablet monitor is already open")
	tabletMonitorInterval = time.Millisecond * 3000
	tabletMonitorTimeout  = time.Millisecond * 2500
)

func registerTabletMonitorFlags(fs *pflag.FlagSet) {
	fs.DurationVar(&tabletMonitorInterval, "tablet-monitor-interval", tabletMonitorInterval, "interval for non-primaries to monitor the tablet")
	fs.DurationVar(&tabletMonitorTimeout, "tablet-monitor-timeout", tabletMonitorTimeout, "timeout for pings when monitoring the tablet")
}

// TabletMonitor is an interface for monitoring a tablet.
type TabletMonitor interface {
	Open() error
	Close()
	IsReachable() bool
	SetTablet(*topodatapb.Tablet)
}

// NewTabletMonitor creates a TMClientTabletMonitor.
func NewTabletMonitor(tmc tmclient.TabletManagerClient) TabletMonitor {
	return &TMClientTabletMonitor{
		pingNowChan: make(chan *topodatapb.Tablet, 1),
		tablet:      atomic.Value{},
		tmc:         tmc,
	}
}

// TMClientTabletMonitor monitors the health of a tablet by pinging it using grpctmclient.
type TMClientTabletMonitor struct {
	cancel context.CancelFunc
	ctx    context.Context
	mu     sync.Mutex
	tmc    tmclient.TabletManagerClient

	opened      bool
	pingNowChan chan *topodatapb.Tablet
	reachable   uint32
	tablet      atomic.Value
}

// getTablet returns the tablet to monitor under lock.
func (pm *TMClientTabletMonitor) getTablet() *topodatapb.Tablet {
	if tablet, ok := pm.tablet.Load().(*topodatapb.Tablet); ok {
		return tablet
	}
	return nil
}

// SetTablet sets the tablet to monitor under lock.
func (pm *TMClientTabletMonitor) SetTablet(tablet *topodatapb.Tablet) {
	pm.tablet.Store(tablet)
	if pm.opened {
		pm.pingNowChan <- tablet
	}
}

// ping performs a Ping RPC to the tmserver of a tablet.
func (pm *TMClientTabletMonitor) ping(tablet *topodatapb.Tablet) {
	if pm.tmc == nil {
		return
	}
	if tablet == nil {
		return
	}

	ctx, cancel := context.WithTimeout(pm.ctx, tabletMonitorTimeout)
	defer cancel()

	var reachable uint32
	if err := pm.tmc.Ping(ctx, tablet); err != nil {
		log.Errorf("Failed to ping tablet %s: %+v", topoproto.TabletAliasString(tablet.Alias), err)
	} else {
		reachable = 1
	}
	atomic.StoreUint32(&pm.reachable, reachable)
}

// poll pings the tablet periodically and on-demand when the address changes.
func (pm *TMClientTabletMonitor) poll() {
	ticker := time.NewTicker(tabletMonitorInterval)
	defer ticker.Stop()
	for {
		select {
		case <-pm.ctx.Done():
			return
		case tablet := <-pm.pingNowChan:
			pm.ping(tablet)
		case <-ticker.C:
			pm.ping(pm.getTablet())
		}
	}
}

// Open opens the tablet health monitor. This causes an initial ping, then periodic/on-demand pings in a loop.
func (pm *TMClientTabletMonitor) Open() error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if pm.opened {
		return ErrTabletMonitorOpen
	}

	pm.ctx, pm.cancel = context.WithCancel(context.Background())
	pm.ping(pm.getTablet())
	go pm.poll()
	pm.opened = true
	return nil
}

// Close stops background pings and closes the tablet health monitor.
func (pm *TMClientTabletMonitor) Close() {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if pm.cancel != nil {
		pm.cancel()
	}
	atomic.StoreUint32(&pm.reachable, 0)
	pm.tablet.Store(nil)
	pm.opened = false
}

// IsReachable returns true if the tablet is reachable.
func (pm *TMClientTabletMonitor) IsReachable() bool {
	return atomic.LoadUint32(&pm.reachable) == 1
}
