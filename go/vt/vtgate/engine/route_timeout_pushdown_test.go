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

package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type timeoutPushdownVCursor struct {
	*loggingVCursor
	queryTimeoutSelectPushdown bool
	queryTimeout               int64
	maxExecutionTimeHint       int64
}

func (vc *timeoutPushdownVCursor) GetQueryTimeoutSelectPushdown() bool {
	return vc.queryTimeoutSelectPushdown
}

func (vc *timeoutPushdownVCursor) Session() SessionActions {
	return vc
}

func (vc *timeoutPushdownVCursor) GetQueryTimeout() int64 {
	return vc.queryTimeout
}

func (vc *timeoutPushdownVCursor) SetMaxExecutionTimeHint(ms int64) {
	vc.maxExecutionTimeHint = ms
}

func newTimeoutPushdownVCursor(pushdown bool, queryTimeout int64) *timeoutPushdownVCursor {
	return &timeoutPushdownVCursor{
		loggingVCursor: &loggingVCursor{
			shards:  []string{"-20"},
			results: []*sqltypes.Result{defaultSelectResult},
		},
		queryTimeoutSelectPushdown: pushdown,
		queryTimeout:               queryTimeout,
	}
}

func TestInjectMaxExecutionTimeHint(t *testing.T) {
	testcases := []struct {
		name         string
		query        string
		queryTimeout int    // route-level timeout (from QUERY_TIMEOUT_MS)
		sessionMs    int64  // session-level query timeout
		pushdown     bool   // flag enabled
		wantQuery    string // expected query after injection
		wantHintMs   int64  // expected hint value
	}{
		{
			name:       "pushdown disabled",
			query:      "select * from t",
			sessionMs:  1000,
			pushdown:   false,
			wantQuery:  "select * from t",
			wantHintMs: 0,
		},
		{
			name:       "no timeout available",
			query:      "select * from t",
			pushdown:   true,
			wantQuery:  "select * from t",
			wantHintMs: 0,
		},
		{
			name:         "route timeout injects hint",
			query:        "select * from t",
			queryTimeout: 5000,
			pushdown:     true,
			wantQuery:    "select /*+ MAX_EXECUTION_TIME(5000) */ * from t",
			wantHintMs:   5000,
		},
		{
			name:       "session timeout injects hint",
			query:      "select * from t",
			sessionMs:  3000,
			pushdown:   true,
			wantQuery:  "select /*+ MAX_EXECUTION_TIME(3000) */ * from t",
			wantHintMs: 3000,
		},
		{
			name:         "route timeout takes priority over session",
			query:        "select * from t",
			queryTimeout: 2000,
			sessionMs:    5000,
			pushdown:     true,
			wantQuery:    "select /*+ MAX_EXECUTION_TIME(2000) */ * from t",
			wantHintMs:   2000,
		},
		{
			name:       "existing MAX_EXECUTION_TIME is overridden",
			query:      "select /*+ MAX_EXECUTION_TIME(100) */ * from t",
			sessionMs:  3000,
			pushdown:   true,
			wantQuery:  "select /*+ MAX_EXECUTION_TIME(3000) */ * from t",
			wantHintMs: 3000,
		},
		{
			name:       "works alongside other hints",
			query:      "select /*+ SET_VAR(sort_buffer_size=16M) */ * from t",
			sessionMs:  1500,
			pushdown:   true,
			wantQuery:  "select /*+ SET_VAR(sort_buffer_size=16M) MAX_EXECUTION_TIME(1500) */ * from t",
			wantHintMs: 1500,
		},
		{
			name:       "non-select is unchanged",
			query:      "update t set a = 1",
			sessionMs:  1000,
			pushdown:   true,
			wantQuery:  "update t set a = 1",
			wantHintMs: 0,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			route := NewRoute(
				Unsharded,
				&vindexes.Keyspace{Name: "ks", Sharded: false},
				tc.query,
				"",
			)
			route.QueryTimeout = tc.queryTimeout

			vc := newTimeoutPushdownVCursor(tc.pushdown, tc.sessionMs)

			var query string
			var hintMs int64
			if vc.GetQueryTimeoutSelectPushdown() {
				query, hintMs = route.injectMaxExecutionTimeHint(vc)
			} else {
				query = route.Query
			}

			assert.Equal(t, tc.wantQuery, query)
			assert.Equal(t, tc.wantHintMs, hintMs)
		})
	}
}

func TestExecuteShardsWithTimeoutPushdown(t *testing.T) {
	route := NewRoute(
		Unsharded,
		&vindexes.Keyspace{Name: "ks", Sharded: false},
		"select * from t",
		"select * from t",
	)
	route.QueryTimeout = 5000

	vc := newTimeoutPushdownVCursor(true, 0)

	result, err := route.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Verify the hint was set during execution via callback
	var hintDuringExec int64
	vc.onExecuteMultiShardFn = func(_ context.Context, _ Primitive, _ []*srvtopo.ResolvedShard, _ []*querypb.BoundQuery, _, _ bool) {
		hintDuringExec = vc.maxExecutionTimeHint
	}
	vc.Rewind()
	result, err = route.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, int64(5000), hintDuringExec)
	// Reset to 0 after execution by defer
	assert.Equal(t, int64(0), vc.maxExecutionTimeHint)
}

func TestStreamExecuteShardsWithTimeoutPushdown(t *testing.T) {
	route := NewRoute(
		Unsharded,
		&vindexes.Keyspace{Name: "ks", Sharded: false},
		"select * from t",
		"select * from t",
	)
	route.QueryTimeout = 3000

	vc := newTimeoutPushdownVCursor(true, 0)

	_, err := wrapStreamExecute(route, vc, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)

	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`StreamExecuteMulti select /*+ MAX_EXECUTION_TIME(3000) */ * from t ks.-20: {} `,
	})
	// maxExecutionTimeHint is reset to 0 by defer after execution
	assert.Equal(t, int64(0), vc.maxExecutionTimeHint)
}

func TestExecuteShardsWithoutTimeoutPushdown(t *testing.T) {
	route := NewRoute(
		Unsharded,
		&vindexes.Keyspace{Name: "ks", Sharded: false},
		"select * from t",
		"select * from t",
	)
	route.QueryTimeout = 5000

	vc := newTimeoutPushdownVCursor(false, 0)

	result, err := route.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	require.NotNil(t, result)

	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.-20: select * from t {} false false`,
	})
	assert.Equal(t, int64(0), vc.maxExecutionTimeHint)
}
