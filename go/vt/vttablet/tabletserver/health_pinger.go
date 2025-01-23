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

package tabletserver

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
	ErrPrimaryHealthPingerOpen = vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION,
		"primary health pinger is already open")
	ErrPrimaryUnreachable = vterrors.New(vtrpcpb.Code_UNAVAILABLE,
		"primary is unreachable")
	PrimaryPingTimeout = time.Second * 5
)

type PrimaryHealthPinger interface {
	Open() error
	Close()
	IsReachable() error
	SetPrimary(*topodatapb.Tablet)
}

func NewPrimaryHealthPinger(interval time.Duration) PrimaryHealthPinger {
	return &tmPrimaryHealthPinger{
		interval: interval,
	}
}

type tmPrimaryHealthPinger struct {
	cancel    context.CancelFunc
	ctx       context.Context
	interval  time.Duration
	mu        sync.Mutex
	opened    bool
	primary   *topodatapb.Tablet
	reachable uint32
}

func (php *tmPrimaryHealthPinger) getPrimary() *topodatapb.Tablet {
	php.mu.Lock()
	defer php.mu.Unlock()
	return php.primary
}

func (php *tmPrimaryHealthPinger) SetPrimary(primary *topodatapb.Tablet) {
	php.mu.Lock()
	defer php.mu.Unlock()
	php.primary = primary
}

func (php *tmPrimaryHealthPinger) ping(tmc *grpctmclient.Client) {
	primary := php.getPrimary()
	if primary == nil {
		return
	}

	ctx, cancel := context.WithTimeout(php.ctx, PrimaryPingTimeout)
	defer cancel()
	if err := tmc.Ping(ctx, primary); err != nil {
		log.Errorf("Failed to ping primary %s: %+v", topoproto.TabletAliasString(primary.Alias), err)
		return
	}
	atomic.StoreUint32(&php.reachable, 1)
}

func (php *tmPrimaryHealthPinger) Open() error {
	php.mu.Lock()
	defer php.mu.Unlock()

	if php.opened {
		return ErrPrimaryHealthPingerOpen
	}

	php.ctx, php.cancel = context.WithCancel(context.Background())
	go func() {
		tmc := grpctmclient.NewClient()
		defer tmc.Close()

		// initial ping
		php.ping(tmc)

		// periodic ping
		ticker := time.NewTicker(php.interval)
		defer ticker.Stop()
		for {
			select {
			case <-php.ctx.Done():
				return
			case <-ticker.C:
				php.ping(tmc)
			}
		}
	}()
	return nil
}

func (php *tmPrimaryHealthPinger) Close() {
	php.mu.Lock()
	defer php.mu.Unlock()
	if php.cancel != nil {
		php.cancel()
	}
	atomic.StoreUint32(&php.reachable, 0)
	php.opened = false
}

func (php *tmPrimaryHealthPinger) IsReachable() error {
	if atomic.LoadUint32(&php.reachable) != 1 {
		return ErrPrimaryUnreachable
	}
	return nil
}
