/*
   Copyright 2014 Outbrain Inc.

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

package db

import (
	"context"
	"database/sql"

	"vitess.io/vitess/go/vt/external/golib/sqlutils"
	"vitess.io/vitess/go/vt/log"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/inst"
)

type AcknowledgeRecoveriesOpts struct {
	Owner           string
	Comment         string
	MarkEndRecovery bool
}

type AuditOperationOpts struct {
	// TODO: define
}

type ReadRecoveriesOpts struct {
	// TODO: define
}

type ReadReplicaInstancesOpts struct {
	PrimaryHost                    string
	PrimaryPort                    int
	IncludeBinlogServerSubReplicas bool
}

type DB interface {
	// Discovery
	DeleteDiscoveredTablets(ctx context.Context) error
	GetShardPrimary(ctx context.Context, keyspace string, shard string) (*topodatapb.Tablet, error)
	GetTabletAliasesByCell(ctx context.Context) ([]*topodatapb.TabletAlias, error)
	GetTabletAliasesByKeyspaceShard(ctx context.Context) ([]*topodatapb.TabletAlias, error)

	// Detection
	AttemptFailureDetectionRegistration(ctx context.Context, analysisEntry *inst.ReplicationAnalysis) (bool, error)
	ClearActiveFailureDetections(ctx context.Context) error

	// Analysis
	AuditInstanceAnalysisInChangelog(ctx context.Context, tabletAlias *topodatapb.TabletAlias, analysisCode AnalysisCode) error
	ExpireAuditData(ctx context.Context) error
	ExpireInstanceAnalysisChangelog(ctx context.Context) error
	GetReplicationAnalysis(ctx context.Context, keyspace string, shard string, hints *inst.ReplicationAnalysisHints) ([]*inst.ReplicationAnalysis, error)

	// Audit
	AuditOperation(ctx context.Context, opts *AuditOperationOpts) error

	// Instance
	ExpireStaleInstanceBinlogCoordinates(ctx context.Context) error
	ForgetInstance(ctx context.Context, tabletAlias *topodatapb.TabletAlias) error
	ForgetLongUnseenInstances(ctx context.Context) error
	GetKeyspaceShardName(ctx context.Context, tabletAlias *topodatapb.TabletAlias) (string, string, error)
	LegacyReadInstanceClusterAttributes(ctx context.Context, primaryHost string, primaryPort int) (*inst.Instance, error)
	ReadInstanceClusterAttributes(ctx context.Context, primaryAlias *topodatapb.TabletAlias) (*inst.Instance, error)
	ReadInstance(ctx context.Context, tabletAlias *topodatapb.TabletAlias) (*inst.Instance, bool, error)
	ReadReplicaInstances(ctx context.Context, opts *ReadReplicaInstancesOpts) ([]*inst.Instance, error)
	ReadProblemInstances(ctx context.Context, keyspace string, shard string) ([]*inst.Instance, error)
	ReadOutdatedInstanceKeys(ctx context.Context) ([]*topodatapb.TabletAlias, error)
	ReadInstancesWithErrantGTIDs(ctx context.Context, keyspace string, shard string) ([]*inst.Instance, error)
	RecordStaleInstanceBinlogCoordinates(ctx context.Context, tabletAlias *topodatapb.TabletAlias, binlogCoordinates *inst.BinlogCoordinates) error
	SnapshotTopologies(ctx context.Context) error
	UpdateInstanceLastAttemptedCheck(ctx context.Context, tabletAlias *topodatapb.TabletAlias)
	UpdateInstanceLastChecked(ctx context.Context, tabletAlias *topodatapb.TabletAlias, partialSuccess bool) error
	WriteInstances(ctx context.Context, instances []*inst.Instance, instanceWasActuallyFound, updateLastSeen bool) error

	// Keyspace
	ReadKeyspace(ctx context.Context, keyspace string) (*topo.KeyspaceInfo, error)
	SaveKeyspace(ctx context.Context, keyspace *topo.KeyspaceInfo) error
	GetDurabilityPolicy(ctx context.Context, keyspace string) (reparentutil.Durabler, error)

	// Shard
	ReadShardPrimaryInformation(ctx context.Context, keyspaceName, shardName string) (*topodatapb.TabletAlias, string, error)
	SaveShard(ctx context.Context, shard *topo.ShardInfo) error

	// Tablet
	ReadTablet(ctx context.Context, tabletAlias *topodatapb.TabletAlias) (*topodatapb.Tablet, error)
	SaveTablet(ctx context.Context, tablet *topodatapb.Tablet) error

	// Recovery
	AcknowledgeRecoveries(ctx context.Context, opts *AcknowledgeRecoveriesOpts) (int64, error)
	ClearActiveRecoveries(ctx context.Context) error
	DisableRecovery(ctx context.Context) error
	EnableRecovery(ctx context.Context) error
	ExpireBlockedRecoveries(ctx context.Context) error
	ExpireRecoveries(ctx context.Context) error
	ExpireRecoverySteps(ctx context.Context) error
	IsRecoveryDisabled(ctx context.Context) (bool, error)
	ReadRecoveries(ctx context.Context, opts *ReadRecoveriesOpts) ([]*logic.TopologyRecovery, error)
	RegisterBlockedRecoveries(ctx context.Context, analysisEntry *inst.ReplicationAnalysis, blockingRecoveries []*logic.TopologyRecovery) error
	WriteResolveRecovery(ctx context.Context, topologyRecovery *logic.TopologyRecovery) error
	WriteTopologyRecovery(ctx context.Context, topologyRecovery *logic.TopologyRecovery) (*logic.TopologyRecovery, error)
	WriteTopologyRecoveryStep(ctx context.Context, topologyRecoveryStep *logic.TopologyRecoveryStep) error
}

type vtorcDB struct {
	db *sql.DB
}

// OpenTopology returns the DB instance for the vtorc backed database
func OpenVTOrc() (*vtorcDB, error) {
	var fromCache bool
	db, fromCache, err := sqlutils.GetSQLiteDB(config.Config.SQLite3DataFile)
	if err == nil && !fromCache {
		log.Infof("Connected to vtorc backend: sqlite on %v", config.Config.SQLite3DataFile)
		if err := initVTOrcDB(db); err != nil {
			log.Fatalf("Cannot initiate vtorc: %+v", err)
		}
	}
	if db != nil {
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
	}
	return &vtorcDB{db}, err
}

// registerVTOrcDeployment updates the vtorc_db_deployments table upon successful deployment
func (vdb *vtorcDB) registerVTOrcDeployment() error {
	query := `REPLACE INTO vtorc_db_deployments (
		deployed_version,
		deployed_timestamp
	) VALUES (
		?,
		DATETIME('now')
	)`
	if _, err := ExecVTOrc(query, ""); err != nil {
		log.Fatalf("Unable to write to vtorc_db_deployments: %+v", err)
	}
	return nil
}

// deployStatements will issue given sql queries that are not already known to be deployed.
// This iterates both lists (to-run and already-deployed) and also verifies no contradictions.
func (vdb *vtorcDB) deployStatements(db *sql.DB, queries []string) error {
	tx, err := vdb.db.Begin()
	if err != nil {
		return err
	}
	for _, query := range queries {
		if _, err := tx.Exec(query); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// ClearVTOrcDatabase is used to clear the VTOrc database. This function is meant to be used by tests to clear the
// database to get a clean slate without starting a new one.
func ClearVTOrcDatabase() {
	db, _, _ := sqlutils.GetSQLiteDB(config.Config.SQLite3DataFile)
	if db != nil {
		if err := initVTOrcDB(db); err != nil {
			log.Fatalf("Cannot re-initiate vtorc: %+v", err)
		}
	}
}

// initVTOrcDB attempts to create/upgrade the vtorc backend database. It is created once in the
// application's lifetime.
func (vdb *vtorcDB) initVTOrcDB() error {
	log.Info("Initializing vtorc")
	log.Info("Migrating database schema")
	if err := vdb.deployStatements(); err != nil {
		return err
	}
	if err := vdb.registerVTOrcDeployment(); err != nil {
		return err
	}
	if _, err := vdb.ExecVTOrc(`PRAGMA journal_mode = WAL`); err != nil {
		return err
	}
	if _, err := vdb.ExecVTOrc(`PRAGMA synchronous = NORMAL`); err != nil {
		return err
	}
	return nil
}

// ExecVTOrc will execute given query on the vtorc backend database.
func (vdb *vtorcDB) ExecVTOrc(query string, args ...any) (sql.Result, error) {
	return sqlutils.ExecNoPrepare(vdb.db, query, args...)
}

// QueryVTOrcRowsMap
func (vdb *vtorcDB) QueryVTOrcRowsMap(query string, onRow func(sqlutils.RowMap) error) error {
	return sqlutils.QueryRowsMap(db, query, onRow)
}

// QueryVTOrc
func (vdb *vtorcDB) QueryVTOrc(query string, argsArray []any, onRow func(sqlutils.RowMap) error) error {
	if err = sqlutils.QueryRowsMap(db, query, onRow, argsArray...); err != nil {
		log.Warning(err.Error())
	}
	return err
}
