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

package logic

import (
	"context"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

type TabletManager struct {
	tmclient.TabletManagerClient
}

func NewTabletManager(tmc tmclient.TabletManagerClient) *TabletManager {
	return &TabletManager{tmc}
}

// tabletUndoDemotePrimary calls the said RPC for the given tablet.
func (tm *TabletManager) tabletUndoDemotePrimary(ctx context.Context, tablet *topodatapb.Tablet, semiSync bool) error {
	tmcCtx, tmcCancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer tmcCancel()
	return tm.UndoDemotePrimary(tmcCtx, tablet, semiSync)
}

// setReadOnly calls the said RPC for the given tablet
func (tm *TabletManager) setReadOnly(ctx context.Context, tablet *topodatapb.Tablet) error {
	tmcCtx, tmcCancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer tmcCancel()
	return tm.SetReadOnly(tmcCtx, tablet)
}

// changeTabletType calls the said RPC for the given tablet with the given parameters.
func (tm *TabletManager) changeTabletType(ctx context.Context, tablet *topodatapb.Tablet, tabletType topodatapb.TabletType, semiSync bool) error {
	tmcCtx, tmcCancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer tmcCancel()
	return tm.ChangeType(tmcCtx, tablet, tabletType, semiSync)
}

// resetReplicationParameters resets the replication parameters on the given tablet.
func (tm *TabletManager) resetReplicationParameters(ctx context.Context, tablet *topodatapb.Tablet) error {
	tmcCtx, tmcCancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer tmcCancel()
	return tm.ResetReplicationParameters(tmcCtx, tablet)
}

// setReplicationSource calls the said RPC with the parameters provided
func (tm *TabletManager) setReplicationSource(ctx context.Context, replica *topodatapb.Tablet, primary *topodatapb.Tablet, semiSync bool, heartbeatInterval float64) error {
	tmcCtx, tmcCancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer tmcCancel()
	return tm.SetReplicationSource(tmcCtx, replica, primary.Alias, 0, "", true, semiSync, heartbeatInterval)
}
