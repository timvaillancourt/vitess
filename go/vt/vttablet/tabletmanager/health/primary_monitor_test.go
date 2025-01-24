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
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/testutils"
)

func TestPrimaryHealthMonitor(t *testing.T) {
	s, listener := testutils.InitTestTMServer(t)
	defer listener.Close()

	interval := time.Millisecond * 5
	phm := NewPrimaryHealthMonitor(interval)
	require.NotNil(t, phm)

	// open
	require.NoError(t, phm.Open())
	require.True(t, phm.opened)
	require.Nil(t, phm.primary)
	require.ErrorIs(t, phm.IsReachable(), ErrPrimaryUnreachable)

	// set primary
	primary := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "test",
			Uid:  123,
		},
		Hostname: host,
		PortMap: map[string]int32{
			"grpc": port,
		},
	}
	phm.SetPrimary(primary)
	require.Equal(t, primary, phm.primary)

	// test if reachable
	time.Sleep(interval * 3)
	require.NoError(t, phm.IsReachable())

	// close
	phm.Close()
	require.Error(t, phm.IsReachable())
	require.False(t, phm.opened)
	require.Nil(t, phm.primary)

	// set primary before re-open, test immediately reachable
	phm.SetPrimary(primary)
	require.NoError(t, phm.Open())
	require.NoError(t, phm.IsReachable())
}
