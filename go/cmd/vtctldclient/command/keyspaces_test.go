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

package command

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/constants/sidecar"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vtctl/vtctldclient"
	"vitess.io/vitess/go/vt/vterrors"
)

type createKeyspaceRecordingClient struct {
	vtctldclient.VtctldClient
	request *vtctldatapb.CreateKeyspaceRequest
	err     error
}

func (c *createKeyspaceRecordingClient) CreateKeyspace(_ context.Context, req *vtctldatapb.CreateKeyspaceRequest, _ ...grpc.CallOption) (*vtctldatapb.CreateKeyspaceResponse, error) {
	c.request = req
	if c.err != nil {
		return nil, c.err
	}
	return &vtctldatapb.CreateKeyspaceResponse{
		Keyspace: &vtctldatapb.Keyspace{
			Name: req.Name,
			Keyspace: &topodatapb.Keyspace{
				KeyspaceType:     req.Type,
				DurabilityPolicy: req.DurabilityPolicy,
			},
		},
	}, nil
}

func TestCreateKeyspaceDurabilityWarning(t *testing.T) {
	snapshotArgs := []string{"--type=SNAPSHOT", "--base-keyspace=base", "--snapshot-timestamp=2020-01-01T00:00:00Z"}
	rpcErr := vterrors.New(vtrpcpb.Code_UNAVAILABLE, "test RPC failure")
	tests := []struct {
		name       string
		args       []string
		policy     string
		warning    bool
		snapshot   bool
		force      bool
		rpcError   error
		validation string
	}{
		{name: "omitted policy", policy: "none", warning: true},
		{name: "explicit none", args: []string{"--durability-policy=none"}, policy: "none"},
		{name: "explicit semi_sync", args: []string{"--durability-policy=semi_sync"}, policy: "semi_sync"},
		{name: "explicit cross_cell", args: []string{"--durability-policy=cross_cell"}, policy: "cross_cell"},
		{name: "custom policy", args: []string{"--durability-policy=custom"}, policy: "custom"},
		{name: "explicit empty policy", args: []string{"--durability-policy="}, warning: true},
		{name: "snapshot", args: snapshotArgs, policy: "none", snapshot: true},
		{name: "snapshot explicit none", args: append(append([]string{}, snapshotArgs...), "--durability-policy=none"), policy: "none", snapshot: true},
		{name: "snapshot semi_sync", args: append(append([]string{}, snapshotArgs...), "--durability-policy=semi_sync"), validation: "--durability-policy cannot be specified"},
		{name: "snapshot empty policy", args: append(append([]string{}, snapshotArgs...), "--durability-policy="), validation: "--durability-policy cannot be specified"},
		{name: "snapshot missing base", args: []string{"--type=SNAPSHOT"}, validation: "--base-keyspace is required"},
		{name: "invalid sidecar", args: []string{"--sidecar-db-name="}, validation: "--sidecar-db-name cannot be empty"},
		{name: "force", args: []string{"--force"}, policy: "none", warning: true, force: true},
		{name: "RPC failure", policy: "none", warning: true, rpcError: rpcErr},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldOptions, oldClient, oldCtx := createKeyspaceOptions, client, commandCtx
			changed := make(map[*pflag.Flag]bool)
			t.Cleanup(func() {
				createKeyspaceOptions, client, commandCtx = oldOptions, oldClient, oldCtx
				for flag, wasChanged := range changed {
					flag.Changed = wasChanged
				}
			})
			CreateKeyspace.Flags().VisitAll(func(flag *pflag.Flag) {
				changed[flag] = flag.Changed
				require.NoError(t, flag.Value.Set(flag.DefValue))
				flag.Changed = false
			})

			var stderr bytes.Buffer
			cmd := &cobra.Command{}
			cmd.Flags().AddFlagSet(CreateKeyspace.Flags())
			cmd.SetErr(&stderr)
			args := append([]string{"ks"}, tt.args...)
			require.NoError(t, cmd.ParseFlags(args))
			recorder := &createKeyspaceRecordingClient{err: tt.rpcError}
			client, commandCtx = recorder, t.Context()

			stdout, err := os.CreateTemp(t.TempDir(), "stdout")
			require.NoError(t, err)
			oldStdout := os.Stdout
			os.Stdout = stdout
			t.Cleanup(func() {
				os.Stdout = oldStdout
				require.NoError(t, stdout.Close())
			})

			err = commandCreateKeyspace(cmd, args)
			if tt.validation != "" {
				require.ErrorContains(t, err, tt.validation)
				assert.Nil(t, recorder.request)
			} else {
				if tt.rpcError != nil {
					require.ErrorIs(t, err, tt.rpcError)
				} else {
					require.NoError(t, err)
				}
				require.NotNil(t, recorder.request)
				assert.Equal(t, "ks", recorder.request.Name)
				assert.Equal(t, tt.policy, recorder.request.DurabilityPolicy)
				assert.Equal(t, sidecar.DefaultName, recorder.request.SidecarDbName)
				assert.Equal(t, tt.force, recorder.request.Force)
				if tt.snapshot {
					assert.Equal(t, topodatapb.KeyspaceType_SNAPSHOT, recorder.request.Type)
					assert.Equal(t, "base", recorder.request.BaseKeyspace)
					require.NotNil(t, recorder.request.SnapshotTime)
					assert.EqualValues(t, 1577836800, recorder.request.SnapshotTime.Seconds)
				} else {
					assert.Equal(t, topodatapb.KeyspaceType_NORMAL, recorder.request.Type)
					assert.Nil(t, recorder.request.SnapshotTime)
				}
			}

			if tt.warning {
				assert.Contains(t, stderr.String(), "Warning:")
				assert.Contains(t, stderr.String(), "v25")
				assert.Contains(t, stderr.String(), "v26")
				assert.Contains(t, stderr.String(), "acknowledged writes")
				assert.Contains(t, stderr.String(), "--durability-policy=none")
				assert.Contains(t, stderr.String(), "--durability-policy=semi_sync")
				assert.Contains(t, stderr.String(), "Existing keyspaces are unaffected")
			} else {
				assert.Empty(t, stderr.String())
			}
			_, err = stdout.Seek(0, io.SeekStart)
			require.NoError(t, err)
			output, err := io.ReadAll(stdout)
			require.NoError(t, err)
			if tt.validation == "" && tt.rpcError == nil {
				assert.Contains(t, string(output), "Successfully created keyspace ks. Result:")
				assert.NotContains(t, string(output), "Warning:")
			} else {
				assert.Empty(t, output)
			}
		})
	}
}
