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

package misc

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
)

func start(t *testing.T) (utils.MySQLCompare, func()) {
	mcmp, err := utils.NewMySQLCompare(t, vtParams, mysqlParams)
	require.NoError(t, err)

	deleteAll := func() {
		tables := []string{"t1", "uks.unsharded"}
		for _, table := range tables {
			_, _ = mcmp.ExecAndIgnore("delete from " + table)
		}
	}

	deleteAll()

	return mcmp, func() {
		deleteAll()
		mcmp.Close()
	}
}

func TestQueryTimeoutWithDual(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	_, err := utils.ExecAllowError(t, mcmp.VtConn, "select sleep(0.04) from dual")
	assert.NoError(t, err)
	_, err = utils.ExecAllowError(t, mcmp.VtConn, "select sleep(0.24) from dual")
	assert.Error(t, err)
	_, err = utils.ExecAllowError(t, mcmp.VtConn, "set @@session.query_timeout=20")
	require.NoError(t, err)
	_, err = utils.ExecAllowError(t, mcmp.VtConn, "select sleep(0.04) from dual")
	assert.Error(t, err)
	_, err = utils.ExecAllowError(t, mcmp.VtConn, "select sleep(0.01) from dual")
	assert.NoError(t, err)
	_, err = utils.ExecAllowError(t, mcmp.VtConn, "select /*vt+ QUERY_TIMEOUT_MS=500 */ sleep(0.24) from dual")
	assert.NoError(t, err)
	_, err = utils.ExecAllowError(t, mcmp.VtConn, "select /*vt+ QUERY_TIMEOUT_MS=10 */ sleep(0.04) from dual")
	assert.Error(t, err)
	_, err = utils.ExecAllowError(t, mcmp.VtConn, "select /*vt+ QUERY_TIMEOUT_MS=15 */ sleep(0.001) from dual")
	assert.NoError(t, err)
	// infinite query timeout overriding all defaults
	utils.SkipIfBinaryIsBelowVersion(t, 21, "vttablet")
	_, err = utils.ExecAllowError(t, mcmp.VtConn, "select /*vt+ QUERY_TIMEOUT_MS=0 */ sleep(5) from dual")
	assert.NoError(t, err)
}

func TestQueryTimeoutWithTables(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	// unsharded
	utils.Exec(t, mcmp.VtConn, "insert /*vt+ QUERY_TIMEOUT_MS=1000 */ into uks.unsharded(id1) values (1),(2),(3),(4),(5)")
	for range 12 {
		utils.Exec(t, mcmp.VtConn, "insert /*vt+ QUERY_TIMEOUT_MS=2000 */ into uks.unsharded(id1) select id1+5 from uks.unsharded")
	}

	utils.Exec(t, mcmp.VtConn, "select count(*) from uks.unsharded where id1 > 31")
	utils.Exec(t, mcmp.VtConn, "select /*vt+ QUERY_TIMEOUT_MS=100 */ count(*) from uks.unsharded where id1 > 31")

	// the query usually takes more than 5ms to return. So this should fail.
	_, err := utils.ExecAllowError(t, mcmp.VtConn, "select /*vt+ QUERY_TIMEOUT_MS=1 */ count(*) from uks.unsharded where id1 > 31")
	require.Error(t, err)
	assert.ErrorContains(t, err, "context deadline exceeded")
	assert.ErrorContains(t, err, "(errno 1317) (sqlstate 70100)")

	// sharded
	utils.Exec(t, mcmp.VtConn, "insert /*vt+ QUERY_TIMEOUT_MS=1000 */ into ks_misc.t1(id1, id2) values (1,2),(2,4),(3,6),(4,8),(5,10)")

	// sleep take in seconds, so 0.1 is 100ms
	utils.Exec(t, mcmp.VtConn, "select /*vt+ QUERY_TIMEOUT_MS=500 */ sleep(0.1) from t1 where id1 = 1")
	_, err = utils.ExecAllowError(t, mcmp.VtConn, "select /*vt+ QUERY_TIMEOUT_MS=20 */ sleep(0.1) from t1 where id1 = 1")
	require.Error(t, err)
	assert.ErrorContains(t, err, "DeadlineExceeded")
	assert.ErrorContains(t, err, "(errno 1317) (sqlstate 70100)")
}

// TestQueryTimeoutWithShardTargeting tests the query timeout with shard targeting.
func TestQueryTimeoutWithShardTargeting(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	// shard targeting to -80 shard.
	utils.Exec(t, mcmp.VtConn, "use `ks_misc/-80`")

	// insert some data
	// Fix: Increased query timeout from 100ms to 1000ms to prevent timeout errors on slow runners.
	utils.Exec(t, mcmp.VtConn, "insert /*vt+ QUERY_TIMEOUT_MS=1000 */ into t1(id1, id2) values (1,2),(3,4),(4,5),(5,6)")

	queries := []string{
		"insert /*vt+ QUERY_TIMEOUT_MS=1 */ into t1(id1, id2) values (6,sleep(5))",
		"update /*vt+ QUERY_TIMEOUT_MS=1 */ t1 set id2 = sleep(5)",
		"delete /*vt+ QUERY_TIMEOUT_MS=1 */ from t1 where id2 = sleep(5)",
		"select /*vt+ QUERY_TIMEOUT_MS=1 */ 1 from t1 where id2 = 5 and sleep(100)",
	}

	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			_, err := utils.ExecAllowError(t, mcmp.VtConn, query)
			// the error message can be different based on VTGate or VTTABLET or grpc error.
			assert.ErrorContains(t, err, "(errno 1317) (sqlstate 70100)")
		})
	}
}

func TestQueryTimeoutWithoutVTGateDefault(t *testing.T) {
	utils.SkipIfBinaryIsBelowVersion(t, 21, "vttablet")
	// disable query timeout
	origVtGateExtraArgs := clusterInstance.VtGateExtraArgs
	clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs,
		"--query-timeout", "0")
	require.NoError(t,
		clusterInstance.RestartVtgate())
	defer func() {
		clusterInstance.VtGateExtraArgs = origVtGateExtraArgs
		require.NoError(t, clusterInstance.RestartVtgate())
		vtParams = clusterInstance.GetVTParams(keyspaceName)
	}()

	// update vtgate params
	vtParams = clusterInstance.GetVTParams(keyspaceName)

	mcmp, closer := start(t)
	defer closer()

	// tablet query timeout of 2s
	_, err := utils.ExecAllowError(t, mcmp.VtConn, "select sleep(5) from dual")
	assert.Error(t, err)

	// infinite timeout using query hint
	utils.Exec(t, mcmp.VtConn, "select /*vt+ QUERY_TIMEOUT_MS=0 */ sleep(5) from dual")

	// checking again without query hint, tablet query timeout of 2s should be applied
	_, err = utils.ExecAllowError(t, mcmp.VtConn, "select sleep(5) from dual")
	assert.Error(t, err)

	// set timeout of 20ms
	utils.Exec(t, mcmp.VtConn, "set query_timeout=20")

	// query timeout of 20ms should be applied
	_, err = utils.ExecAllowError(t, mcmp.VtConn, "select sleep(1) from dual")
	assert.Error(t, err)

	// infinite timeout using query hint will override session timeout.
	utils.Exec(t, mcmp.VtConn, "select /*vt+ QUERY_TIMEOUT_MS=0 */ sleep(5) from dual")

	// open second session
	conn2, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn2.Close()

	// tablet query timeout of 2s should be applied, as session timeout is not set on this connection.
	utils.Exec(t, conn2, "select sleep(1) from dual")
	_, err = utils.ExecAllowError(t, conn2, "select sleep(5) from dual")
	assert.Error(t, err)

	// reset session on first connection, tablet query timeout of 2s should be applied.
	utils.Exec(t, mcmp.VtConn, "set query_timeout=0")
	_, err = utils.ExecAllowError(t, mcmp.VtConn, "select sleep(5) from dual")
	assert.Error(t, err)
}

// TestOverallQueryTimeout tests that the query timeout is applied to the overall execution of a query
// and not just individual routes.
func TestOverallQueryTimeout(t *testing.T) {
	utils.SkipIfBinaryIsBelowVersion(t, 21, "vtgate")
	utils.SkipIfBinaryIsBelowVersion(t, 21, "vttablet")
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into t1(id1, id2) values (2,2),(3,3)")

	// After inserting the rows above, if we run the following query, we will end up doing join on vtgate
	// that issues one select query on the left side and 2 on the right side. The queries on the right side
	// take 2 and 3 seconds each to run. If we have an overall timeout for 4 seconds, then it should fail.
	_, err := utils.ExecAllowError(t, mcmp.VtConn, "select /*vt+ QUERY_TIMEOUT_MS=4000 */ sleep(u2.id2), u1.id2 from t1 u1 join t1 u2 where u1.id2 = u2.id1")
	assert.Error(t, err)
	// We can get two different error messages based on whether it is coming from vttablet or vtgate
	deadLineExceeded := "DeadlineExceeded desc"
	if !strings.Contains(err.Error(), "Query execution was interrupted, maximum statement execution time exceeded") {
		assert.ErrorContains(t, err, deadLineExceeded)
	}

	// Let's also check that setting the session variable also works.
	utils.Exec(t, mcmp.VtConn, "set query_timeout=4000")
	_, err = utils.ExecAllowError(t, mcmp.VtConn, "select sleep(u2.id2), u1.id2 from t1 u1 join t1 u2 where u1.id2 = u2.id1")
	assert.Error(t, err)
	if !strings.Contains(err.Error(), "Query execution was interrupted, maximum statement execution time exceeded") {
		assert.ErrorContains(t, err, deadLineExceeded)
	}

	// Increasing the timeout should pass the query.
	utils.Exec(t, mcmp.VtConn, "set query_timeout=10000")
	_ = utils.Exec(t, mcmp.VtConn, "select sleep(u2.id2), u1.id2 from t1 u1 join t1 u2 where u1.id2 = u2.id1")
}

// getKillCounter returns the value of a specific KillCounters label from vttablet debug/vars.
// The Kills map keys may be prefixed with the tablet name (e.g., "tablet-0000001234.QueriesPushdown")
// when the TabletServer uses a named exporter.
func getKillCounter(t *testing.T, tablet *cluster.VttabletProcess, label string) float64 {
	t.Helper()
	vars := tablet.GetVars()
	require.Contains(t, vars, "Kills")
	kills := vars["Kills"].(map[string]any)

	// First try the exact label, then look for a suffixed match.
	if v, ok := kills[label]; ok {
		return v.(float64)
	}
	for k, v := range kills {
		if strings.HasSuffix(k, "."+label) {
			return v.(float64)
		}
	}
	t.Fatalf("Kill counter label %q not found in %v", label, kills)
	return 0
}

// getMySQLStatusValue returns a MySQL global status variable value as an integer.
func getMySQLStatusValue(t *testing.T, tablet *cluster.VttabletProcess, keyspace, statusVar string) int64 {
	t.Helper()
	result, err := tablet.QueryTablet(
		fmt.Sprintf("SHOW GLOBAL STATUS LIKE '%s'", statusVar),
		keyspace, false,
	)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1, "expected exactly one row for status variable %s", statusVar)
	val, err := strconv.ParseInt(result.Rows[0][1].ToString(), 10, 64)
	require.NoError(t, err)
	return val
}

func TestQueryKillSelectPushdown(t *testing.T) {
	// Get the unsharded keyspace primary tablet.
	primaryTablet := clusterInstance.Keyspaces[0].Shards[0].PrimaryTablet()

	// Restart VTGate with --query-timeout-select-pushdown enabled.
	origVtGateExtraArgs := clusterInstance.VtGateExtraArgs
	clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs, "--query-timeout-select-pushdown")
	require.NoError(t, clusterInstance.RestartVtgate())
	defer func() {
		clusterInstance.VtGateExtraArgs = origVtGateExtraArgs
		require.NoError(t, clusterInstance.RestartVtgate())
		vtParams = clusterInstance.GetVTParams(keyspaceName)
	}()
	vtParams = clusterInstance.GetVTParams(keyspaceName)

	// Connect to VTGate targeting the unsharded keyspace.
	conn, err := mysql.Connect(context.Background(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	utils.Exec(t, conn, "use uks")

	// Insert enough data for a cross join that takes >100ms to scan.
	// MAX_EXECUTION_TIME is checked between row reads, so row-scanning queries
	// reliably respect it (unlike SLEEP or BENCHMARK which run in tight loops).
	utils.Exec(t, conn, "insert /*vt+ QUERY_TIMEOUT_MS=1000 */ into unsharded(id1) values (1),(2),(3),(4),(5)")
	for range 12 {
		utils.Exec(t, conn, "insert /*vt+ QUERY_TIMEOUT_MS=2000 */ into unsharded(id1) select id1+5 from unsharded")
	}
	defer func() {
		utils.Exec(t, conn, "delete /*vt+ QUERY_TIMEOUT_MS=5000 */ from unsharded")
	}()

	// Record initial counters.
	pushdownBefore := getKillCounter(t, primaryTablet.VttabletProcess, "QueriesPushdown")
	queriesBefore := getKillCounter(t, primaryTablet.VttabletProcess, "Queries")
	mysqlExceededBefore := getMySQLStatusValue(t, primaryTablet.VttabletProcess, uks, "Max_execution_time_exceeded")

	// Cross join with a non-optimizable expression forces MySQL to scan all N*N rows.
	// MySQL checks MAX_EXECUTION_TIME between row reads.
	_, err = utils.ExecAllowError(t, conn, "select /*vt+ QUERY_TIMEOUT_MS=100 */ sum(a.id1 + b.id1) from unsharded a join unsharded b")
	require.Error(t, err)

	// The client should always see errno 1317 (ERQueryInterrupted), regardless of whether
	// VTGate's context or MySQL's MAX_EXECUTION_TIME fired first. The errno 3024
	// (ERQueryTimeout) from MAX_EXECUTION_TIME is mapped to 1317 by vttablet so that
	// clients see consistent behavior.
	assert.ErrorContains(t, err, "errno 1317")

	// Wait for vttablet to process MySQL's MAX_EXECUTION_TIME response. VTGate returns
	// the error to the client before vttablet finishes handling the MySQL response, so
	// we need to poll the counter until it's incremented.
	assert.Eventually(t, func() bool {
		return getKillCounter(t, primaryTablet.VttabletProcess, "QueriesPushdown") > pushdownBefore
	}, 5*time.Second, 100*time.Millisecond, "QueriesPushdown kill counter should have incremented")

	// Verify the old Queries kill counter was NOT incremented (no fallback KILL QUERY).
	queriesAfter := getKillCounter(t, primaryTablet.VttabletProcess, "Queries")
	assert.Equal(t, queriesBefore, queriesAfter, "Queries kill counter should not have incremented")

	// Verify MySQL's Max_execution_time_exceeded status variable was incremented.
	mysqlExceededAfter := getMySQLStatusValue(t, primaryTablet.VttabletProcess, uks, "Max_execution_time_exceeded")
	assert.Greater(t, mysqlExceededAfter, mysqlExceededBefore, "MySQL Max_execution_time_exceeded should have incremented")
}
