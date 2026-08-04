/*
Copyright 2026 The Vitess Authors.

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

package grpctmserver

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmrpctest"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func TestStartReplicationRejectsUnknownMode(t *testing.T) {
	s := &server{tm: tmrpctest.NewFakeRPCTM(t)}

	_, err := s.StartReplication(t.Context(), &tabletmanagerdatapb.StartReplicationRequest{
		StartReplicationMode: replicationdatapb.StartReplicationMode(100),
	})
	require.ErrorContains(t, err, "unsupported start replication mode")
	require.Equal(t, vtrpcpb.Code_INVALID_ARGUMENT, vterrors.Code(err))
}
