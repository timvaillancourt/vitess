/*
Copyright 2021 The Vitess Authors.

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

package onlineddl

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"

	"vitess.io/vitess/go/test/endtoend/cluster"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	ThrottledAppsTimeout = 60 * time.Second
)

var (
	testsStartupTime time.Time
)

func init() {
	testsStartupTime = time.Now()
}

// VtgateExecQuery runs a query on VTGate using given query params
func VtgateExecQuery(t *testing.T, vtParams *mysql.ConnParams, query string, expectError string) *sqltypes.Result {
	t.Helper()

	ctx := context.Background()
	conn, err := mysql.Connect(ctx, vtParams)
	require.Nil(t, err)
	defer conn.Close()

	qr, err := conn.ExecuteFetch(query, -1, true)
	if expectError == "" {
		require.NoError(t, err)
	} else {
		require.Error(t, err, "error should not be nil")
		assert.Contains(t, err.Error(), expectError, "Unexpected error")
	}
	return qr
}

// VtgateExecQueryInTransaction runs a query on VTGate using given query params, inside a transaction
func VtgateExecQueryInTransaction(t *testing.T, vtParams *mysql.ConnParams, query string, expectError string) *sqltypes.Result {
	t.Helper()

	ctx := context.Background()
	conn, err := mysql.Connect(ctx, vtParams)
	require.Nil(t, err)
	defer conn.Close()

	_, err = conn.ExecuteFetch("begin", -1, true)
	require.NoError(t, err)
	qr, err := conn.ExecuteFetch(query, -1, true)
	if expectError == "" {
		require.NoError(t, err)
	} else {
		require.Error(t, err, "error should not be nil")
		assert.Contains(t, err.Error(), expectError, "Unexpected error")
	}
	_, err = conn.ExecuteFetch("commit", -1, true)
	require.NoError(t, err)
	return qr
}

// VtgateExecDDL executes a DDL query with given strategy
func VtgateExecDDL(t *testing.T, vtParams *mysql.ConnParams, ddlStrategy string, query string, expectError string) *sqltypes.Result {
	t.Helper()

	ctx := context.Background()
	conn, err := mysql.Connect(ctx, vtParams)
	require.Nil(t, err)
	defer conn.Close()

	setSession := fmt.Sprintf("set @@ddl_strategy='%s'", ddlStrategy)
	_, err = conn.ExecuteFetch(setSession, 1000, true)
	assert.NoError(t, err)

	qr, err := conn.ExecuteFetch(query, 1000, true)
	if expectError == "" {
		require.NoError(t, err)
	} else {
		require.Error(t, err, "error should not be nil")
		assert.Contains(t, err.Error(), expectError, "Unexpected error")
	}
	return qr
}

// CheckRetryMigration attempts to retry a migration, and expects success/failure by counting affected rows
func CheckRetryMigration(t *testing.T, vtParams *mysql.ConnParams, shards []cluster.Shard, uuid string, expectRetryPossible bool) {
	query, err := sqlparser.ParseAndBind("alter vitess_migration %a retry",
		sqltypes.StringBindVariable(uuid),
	)
	require.NoError(t, err)
	r := VtgateExecQuery(t, vtParams, query, "")

	if expectRetryPossible {
		assert.Equal(t, len(shards), int(r.RowsAffected))
	} else {
		assert.Equal(t, int(0), int(r.RowsAffected))
	}
}

// CheckRetryPartialMigration attempts to retry a migration where a subset of shards failed
func CheckRetryPartialMigration(t *testing.T, vtParams *mysql.ConnParams, uuid string, expectAtLeastRowsAffected uint64) {
	query, err := sqlparser.ParseAndBind("alter vitess_migration %a retry",
		sqltypes.StringBindVariable(uuid),
	)
	require.NoError(t, err)
	r := VtgateExecQuery(t, vtParams, query, "")

	assert.GreaterOrEqual(t, expectAtLeastRowsAffected, r.RowsAffected)
}

// CheckCancelMigration attempts to cancel a migration, and expects success/failure by counting affected rows
func CheckCancelMigration(t *testing.T, vtParams *mysql.ConnParams, shards []cluster.Shard, uuid string, expectCancelPossible bool) {
	query, err := sqlparser.ParseAndBind("alter vitess_migration %a cancel",
		sqltypes.StringBindVariable(uuid),
	)
	require.NoError(t, err)
	r := VtgateExecQuery(t, vtParams, query, "")

	if expectCancelPossible {
		assert.Equal(t, len(shards), int(r.RowsAffected))
	} else {
		assert.Equal(t, int(0), int(r.RowsAffected))
	}
}

// CheckCleanupMigration attempts to cleanup a migration, and expects success by counting affected rows
func CheckCleanupMigration(t *testing.T, vtParams *mysql.ConnParams, shards []cluster.Shard, uuid string) {
	query, err := sqlparser.ParseAndBind("alter vitess_migration %a cleanup",
		sqltypes.StringBindVariable(uuid),
	)
	require.NoError(t, err)
	r := VtgateExecQuery(t, vtParams, query, "")

	assert.Equal(t, len(shards), int(r.RowsAffected))
}

// CheckCompleteMigration attempts to complete a migration, and expects success by counting affected rows
func CheckCompleteMigration(t *testing.T, vtParams *mysql.ConnParams, shards []cluster.Shard, uuid string, expectCompletePossible bool) {
	query, err := sqlparser.ParseAndBind("alter vitess_migration %a complete",
		sqltypes.StringBindVariable(uuid),
	)
	require.NoError(t, err)
	r := VtgateExecQuery(t, vtParams, query, "")

	if expectCompletePossible {
		assert.Equal(t, len(shards), int(r.RowsAffected))
	} else {
		assert.Equal(t, int(0), int(r.RowsAffected))
	}
}

// CheckCompleteMigrationShards attempts to complete a migration for specific shards, and expects success by counting affected rows
func CheckCompleteMigrationShards(t *testing.T, vtParams *mysql.ConnParams, shards []cluster.Shard, uuid string, completeShards string, expectCompletePossible bool) {
	query, err := sqlparser.ParseAndBind("alter vitess_migration %a complete vitess_shards %a",
		sqltypes.StringBindVariable(uuid),
		sqltypes.StringBindVariable(completeShards),
	)
	require.NoError(t, err)
	r := VtgateExecQuery(t, vtParams, query, "")

	if expectCompletePossible {
		assert.Equal(t, len(shards), int(r.RowsAffected))
	} else {
		assert.Equal(t, int(0), int(r.RowsAffected))
	}
}

// CheckPostponeCompleteMigration attempts to postpone an existing migration, and expects success by counting affected rows
func CheckPostponeCompleteMigration(t *testing.T, vtParams *mysql.ConnParams, shards []cluster.Shard, uuid string, expectPotponePossible bool) {
	query, err := sqlparser.ParseAndBind("alter vitess_migration %a postpone complete",
		sqltypes.StringBindVariable(uuid),
	)
	require.NoError(t, err)
	r := VtgateExecQuery(t, vtParams, query, "")

	if expectPotponePossible {
		assert.Equal(t, len(shards), int(r.RowsAffected))
	} else {
		assert.Equal(t, int(0), int(r.RowsAffected))
	}
}

// CheckLaunchMigration attempts to launch a migration, and expects success by counting affected rows
func CheckLaunchMigration(t *testing.T, vtParams *mysql.ConnParams, shards []cluster.Shard, uuid string, launchShards string, expectLaunchPossible bool) {
	query, err := sqlparser.ParseAndBind("alter vitess_migration %a launch vitess_shards %a",
		sqltypes.StringBindVariable(uuid),
		sqltypes.StringBindVariable(launchShards),
	)
	require.NoError(t, err)
	r := VtgateExecQuery(t, vtParams, query, "")

	if expectLaunchPossible {
		assert.Equal(t, len(shards), int(r.RowsAffected))
	} else {
		assert.Equal(t, int(0), int(r.RowsAffected))
	}
}

// CheckCompleteAllMigrations completes all pending migrations and expect number of affected rows
// A negative value for expectCount indicates "don't care, no need to check"
func CheckCompleteAllMigrations(t *testing.T, vtParams *mysql.ConnParams, expectCount int) {
	completeQuery := "alter vitess_migration complete all"
	r := VtgateExecQuery(t, vtParams, completeQuery, "")

	if expectCount >= 0 {
		assert.Equal(t, expectCount, int(r.RowsAffected))
	}
}

// CheckPostponeCompleteAllMigrations postpones all pending migrations and expect number of affected rows
// A negative value for expectCount indicates "don't care, no need to check"
func CheckPostponeCompleteAllMigrations(t *testing.T, vtParams *mysql.ConnParams, expectCount int) {
	completeQuery := "alter vitess_migration postpone complete all"
	r := VtgateExecQuery(t, vtParams, completeQuery, "")

	if expectCount >= 0 {
		assert.Equal(t, expectCount, int(r.RowsAffected))
	}
}

// CheckCancelAllMigrations cancels all pending migrations and expect number of affected rows
// A negative value for expectCount indicates "don't care, no need to check"
func CheckCancelAllMigrations(t *testing.T, vtParams *mysql.ConnParams, expectCount int) {
	cancelQuery := "alter vitess_migration cancel all"
	r := VtgateExecQuery(t, vtParams, cancelQuery, "")

	if expectCount >= 0 {
		assert.Equal(t, expectCount, int(r.RowsAffected))
	}
}

// CheckCleanupAllMigrations cleans up all applicable migrations and expect number of affected rows
// A negative value for expectCount indicates "don't care, no need to check"
func CheckCleanupAllMigrations(t *testing.T, vtParams *mysql.ConnParams, expectCount int) uint64 {
	cleanupQuery := "alter vitess_migration cleanup all"
	r := VtgateExecQuery(t, vtParams, cleanupQuery, "")

	if expectCount >= 0 {
		assert.Equal(t, expectCount, int(r.RowsAffected))
	}
	return r.RowsAffected
}

// CheckLaunchAllMigrations launches all queued posponed migrations and expect number of affected rows
// A negative value for expectCount indicates "don't care, no need to check"
func CheckLaunchAllMigrations(t *testing.T, vtParams *mysql.ConnParams, expectCount int) {
	completeQuery := "alter vitess_migration launch all"
	r := VtgateExecQuery(t, vtParams, completeQuery, "")

	if expectCount >= 0 {
		assert.Equal(t, expectCount, int(r.RowsAffected))
	}
}

// CheckForceMigrationCutOver marks a migration for forced cut-over, and expects success by counting affected rows.
func CheckForceMigrationCutOver(t *testing.T, vtParams *mysql.ConnParams, shards []cluster.Shard, uuid string, expectPossible bool) {
	query, err := sqlparser.ParseAndBind("alter vitess_migration %a force_cutover",
		sqltypes.StringBindVariable(uuid),
	)
	require.NoError(t, err)
	r := VtgateExecQuery(t, vtParams, query, "")

	if expectPossible {
		assert.Equal(t, len(shards), int(r.RowsAffected))
	} else {
		assert.Equal(t, int(0), int(r.RowsAffected))
	}
}

// CheckSetMigrationCutOverThreshold sets the cut-over threshold for a given migration.
func CheckSetMigrationCutOverThreshold(t *testing.T, vtParams *mysql.ConnParams, shards []cluster.Shard, uuid string, threshold time.Duration, expectError string) {
	query, err := sqlparser.ParseAndBind("alter vitess_migration %a cutover_threshold %a",
		sqltypes.StringBindVariable(uuid),
		sqltypes.StringBindVariable(threshold.String()),
	)
	require.NoError(t, err)
	_ = VtgateExecQuery(t, vtParams, query, expectError)
}

// CheckMigrationStatus verifies that the migration indicated by given UUID has the given expected status
func CheckMigrationStatus(t *testing.T, vtParams *mysql.ConnParams, shards []cluster.Shard, uuid string, expectStatuses ...schema.OnlineDDLStatus) bool {
	ksName := shards[0].PrimaryTablet().VttabletProcess.Keyspace
	query, err := sqlparser.ParseAndBind(fmt.Sprintf("show vitess_migrations from %s like %%a", ksName),
		sqltypes.StringBindVariable(uuid),
	)
	require.NoError(t, err)

	r := VtgateExecQuery(t, vtParams, query, "")
	fmt.Printf("# output for `%s`:\n", query)
	PrintQueryResult(os.Stdout, r)

	count := 0
	for _, row := range r.Named().Rows {
		if row["migration_uuid"].ToString() != uuid {
			continue
		}
		for _, expectStatus := range expectStatuses {
			if row["migration_status"].ToString() == string(expectStatus) {
				count++
				break
			}
		}
	}
	return assert.Equal(t, len(shards), count)
}

// WaitForMigrationStatus waits for a migration to reach either provided statuses (returns immediately), or eventually time out
func WaitForMigrationStatus(t *testing.T, vtParams *mysql.ConnParams, shards []cluster.Shard, uuid string, timeout time.Duration, expectStatuses ...schema.OnlineDDLStatus) schema.OnlineDDLStatus {
	shardNames := map[string]bool{}
	for _, shard := range shards {
		shardNames[shard.Name] = true
	}
	query, err := sqlparser.ParseAndBind("show vitess_migrations like %a",
		sqltypes.StringBindVariable(uuid),
	)
	require.NoError(t, err)

	statusesMap := map[string]bool{}
	for _, status := range expectStatuses {
		statusesMap[string(status)] = true
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	lastKnownStatus := ""
	for {
		countMatchedShards := 0
		r := VtgateExecQuery(t, vtParams, query, "")
		for _, row := range r.Named().Rows {
			shardName := row["shard"].ToString()
			if !shardNames[shardName] {
				// irrelevant shard
				continue
			}
			lastKnownStatus = row["migration_status"].ToString()
			if row["migration_uuid"].ToString() == uuid && statusesMap[lastKnownStatus] {
				countMatchedShards++
			}
		}
		if countMatchedShards == len(shards) {
			return schema.OnlineDDLStatus(lastKnownStatus)
		}
		select {
		case <-ctx.Done():
			return schema.OnlineDDLStatus(lastKnownStatus)
		case <-ticker.C:
		}
	}
}

// CheckMigrationArtifacts verifies given migration exists, and checks if it has artifacts
func CheckMigrationArtifacts(t *testing.T, vtParams *mysql.ConnParams, shards []cluster.Shard, uuid string, expectArtifacts bool) {
	r := ReadMigrations(t, vtParams, uuid)

	assert.Equal(t, len(shards), len(r.Named().Rows))
	for _, row := range r.Named().Rows {
		hasArtifacts := (row["artifacts"].ToString() != "")
		assert.Equal(t, expectArtifacts, hasArtifacts)
	}
}

// ReadMigrations reads migration entries
func ReadMigrations(t *testing.T, vtParams *mysql.ConnParams, like string) *sqltypes.Result {
	query, err := sqlparser.ParseAndBind("show vitess_migrations like %a",
		sqltypes.StringBindVariable(like),
	)
	require.NoError(t, err)

	return VtgateExecQuery(t, vtParams, query, "")
}

// ReadMigrationLogs reads migration logs for a given migration, on all shards
func ReadMigrationLogs(t *testing.T, vtParams *mysql.ConnParams, uuid string) (logs []string) {
	query, err := sqlparser.ParseAndBind("show vitess_migration %a logs",
		sqltypes.StringBindVariable(uuid),
	)
	require.NoError(t, err)

	r := VtgateExecQuery(t, vtParams, query, "")
	for _, row := range r.Named().Rows {
		migrationLog := row["migration_log"].ToString()
		logs = append(logs, migrationLog)
	}
	return logs
}

// ThrottleAllMigrations fully throttles online-ddl apps
func ThrottleAllMigrations(t *testing.T, vtParams *mysql.ConnParams) {
	query := "alter vitess_migration throttle all expire '24h' ratio 1"
	_ = VtgateExecQuery(t, vtParams, query, "")
}

// UnthrottleAllMigrations cancels migration throttling
func UnthrottleAllMigrations(t *testing.T, vtParams *mysql.ConnParams) {
	query := "alter vitess_migration unthrottle all"
	_ = VtgateExecQuery(t, vtParams, query, "")
}

// CheckThrottledApps checks for existence or non-existence of an app in the throttled apps list
func CheckThrottledApps(t *testing.T, vtParams *mysql.ConnParams, throttlerApp throttlerapp.Name, expectFind bool) bool {

	ctx, cancel := context.WithTimeout(context.Background(), ThrottledAppsTimeout)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		query := "show vitess_throttled_apps"
		r := VtgateExecQuery(t, vtParams, query, "")

		appFound := false
		for _, row := range r.Named().Rows {
			if throttlerApp.Equals(row.AsString("app", "")) {
				appFound = true
			}
		}
		if appFound == expectFind {
			// we're all good
			return true
		}

		select {
		case <-ctx.Done():
			assert.Fail(t, "CheckThrottledApps timed out", "waiting for '%v' to be in throttled status '%v'", throttlerApp.String(), expectFind)
			return false
		case <-ticker.C:
		}
	}
}

// WaitForThrottledTimestamp waits for a migration to have a non-empty last_throttled_timestamp
func WaitForThrottledTimestamp(t *testing.T, vtParams *mysql.ConnParams, uuid string, timeout time.Duration) (
	row sqltypes.RowNamedValues,
	startedTimestamp string,
	lastThrottledTimestamp string,
) {
	startTime := time.Now()
	for time.Since(startTime) < timeout {
		rs := ReadMigrations(t, vtParams, uuid)
		require.NotNil(t, rs)
		for _, row = range rs.Named().Rows {
			startedTimestamp = row.AsString("started_timestamp", "")
			require.NotEmpty(t, startedTimestamp)
			lastThrottledTimestamp = row.AsString("last_throttled_timestamp", "")
			if lastThrottledTimestamp != "" {
				// good. This is what we've been waiting for.
				return row, startedTimestamp, lastThrottledTimestamp
			}
		}
		time.Sleep(1 * time.Second)
	}
	t.Error("timeout waiting for last_throttled_timestamp to have nonempty value")
	return
}

// ValidateSequentialMigrationIDs validates that schem_migrations.id column, which is an AUTO_INCREMENT, does
// not have gaps
func ValidateSequentialMigrationIDs(t *testing.T, vtParams *mysql.ConnParams, shards []cluster.Shard) {
	r := VtgateExecQuery(t, vtParams, "show vitess_migrations", "")
	shardMin := map[string]uint64{}
	shardMax := map[string]uint64{}
	shardCount := map[string]uint64{}

	for _, row := range r.Named().Rows {
		id := row.AsUint64("id", 0)
		require.NotZero(t, id)

		shard := row.AsString("shard", "")
		require.NotEmpty(t, shard)

		if _, ok := shardMin[shard]; !ok {
			shardMin[shard] = id
			shardMax[shard] = id
		}
		if id < shardMin[shard] {
			shardMin[shard] = id
		}
		if id > shardMax[shard] {
			shardMax[shard] = id
		}
		shardCount[shard]++
	}
	require.NotEmpty(t, shards)
	assert.Equal(t, len(shards), len(shardMin))
	assert.Equal(t, len(shards), len(shardMax))
	assert.Equal(t, len(shards), len(shardCount))
	for shard, count := range shardCount {
		assert.NotZero(t, count)
		assert.Equalf(t, count, shardMax[shard]-shardMin[shard]+1, "mismatch: shared=%v, count=%v, min=%v, max=%v", shard, count, shardMin[shard], shardMax[shard])
	}
}

// ValidateCompletedTimestamp ensures that any migration in `cancelled`, `completed`, `failed` statuses
// has a non-nil and valid `completed_timestamp` value.
func ValidateCompletedTimestamp(t *testing.T, vtParams *mysql.ConnParams) {
	require.False(t, testsStartupTime.IsZero())
	r := VtgateExecQuery(t, vtParams, "show vitess_migrations", "")

	completedTimestampNumValidations := 0
	for _, row := range r.Named().Rows {
		migrationStatus := row.AsString("migration_status", "")
		require.NotEmpty(t, migrationStatus)
		switch migrationStatus {
		case string(schema.OnlineDDLStatusComplete),
			string(schema.OnlineDDLStatusFailed),
			string(schema.OnlineDDLStatusCancelled):
			{
				assert.False(t, row["completed_timestamp"].IsNull())
				// Also make sure the timestamp is "real", and that it is recent.
				timestamp := row.AsString("completed_timestamp", "")
				completedTime, err := time.Parse(sqltypes.TimestampFormat, timestamp)
				assert.NoError(t, err)
				assert.Greater(t, completedTime.Unix(), testsStartupTime.Unix())
				completedTimestampNumValidations++
			}
		}
	}
	assert.NotZero(t, completedTimestampNumValidations)
}
