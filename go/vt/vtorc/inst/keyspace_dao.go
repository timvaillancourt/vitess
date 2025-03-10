/*
Copyright 2022 The Vitess Authors.

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

package inst

import (
	"errors"

	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/vt/external/golib/sqlutils"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/db"
)

// ErrKeyspaceNotFound is a fixed error message used when a keyspace is not found in the database.
var ErrKeyspaceNotFound = errors.New("keyspace not found")

// ReadKeyspace reads the vitess keyspace record.
func ReadKeyspace(keyspaceName string) (*topo.KeyspaceInfo, error) {
	if err := topo.ValidateKeyspaceName(keyspaceName); err != nil {
		return nil, err
	}

	query := `select
			keyspace_type,
			durability_policy,
			vtorc_config
		from
			vitess_keyspace
		where
			keyspace = ?`
	args := sqlutils.Args(keyspaceName)

	keyspace := &topo.KeyspaceInfo{
		Keyspace: &topodatapb.Keyspace{
			VtorcConfig: &topodatapb.VtorcConfig{},
		},
	}
	err := db.QueryVTOrc(query, args, func(row sqlutils.RowMap) error {
		keyspace.KeyspaceType = topodatapb.KeyspaceType(row.GetInt32("keyspace_type"))
		keyspace.DurabilityPolicy = row.GetString("durability_policy")
		keyspace.SetKeyspaceName(keyspaceName)

		opts := prototext.UnmarshalOptions{DiscardUnknown: true}
		return opts.Unmarshal([]byte(row.GetString("vtorc_config")), keyspace.Keyspace.VtorcConfig)
	})
	if err != nil {
		return nil, err
	}
	if keyspace.KeyspaceName() == "" {
		return nil, ErrKeyspaceNotFound
	}
	return keyspace, nil
}

// SaveKeyspace saves the keyspace record against the keyspace name.
func SaveKeyspace(keyspace *topo.KeyspaceInfo) error {
	if keyspace.Keyspace.VtorcConfig == nil {
		keyspace.Keyspace.VtorcConfig = config.DefaultKeyspaceVtorcConfig
	}
	vtorcConfigProto, err := prototext.Marshal(keyspace.Keyspace.VtorcConfig)
	if err != nil {
		return err
	}
	_, err = db.ExecVTOrc(`
		replace	into vitess_keyspace (
			keyspace, keyspace_type, durability_policy, vtorc_config
		) values (
			?, ?, ?, ?
		)`,
		keyspace.KeyspaceName(),
		int(keyspace.KeyspaceType),
		keyspace.GetDurabilityPolicy(),
		vtorcConfigProto,
	)
	return err
}

// GetDurabilityPolicy gets the durability policy for the given keyspace.
func GetDurabilityPolicy(keyspace string) (policy.Durabler, error) {
	ki, err := ReadKeyspace(keyspace)
	if err != nil {
		return nil, err
	}
	return policy.GetDurabilityPolicy(ki.DurabilityPolicy)
}
