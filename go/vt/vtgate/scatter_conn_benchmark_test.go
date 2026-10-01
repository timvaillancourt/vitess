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

package vtgate

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/srvtopo"
	econtext "vitess.io/vitess/go/vt/vtgate/executorcontext"
	"vitess.io/vitess/go/vt/vttablet/queryservice"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

// benchmarkScatterGateway returns a fixed result without RPCs, SQL parsing or
// query-history recording, so those costs do not hide scatter/session overhead.
type benchmarkScatterGateway struct {
	srvtopo.Gateway
	result        *sqltypes.Result
	transactionID int64
}

func (g *benchmarkScatterGateway) Execute(_ context.Context, _ queryservice.Session, _ *querypb.Target, _ string, _ map[string]*querypb.BindVariable, transactionID, _ int64, _ *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	if transactionID != g.transactionID {
		return nil, fmt.Errorf("unexpected transaction ID: got %d, want %d", transactionID, g.transactionID)
	}
	return g.result, nil
}

func (g *benchmarkScatterGateway) QueryServiceByAlias(context.Context, *topodatapb.TabletAlias, *querypb.Target) (queryservice.QueryService, error) {
	return g, nil
}

// BenchmarkScatterExecuteMultiShard measures fan-out and result merging with no
// transaction, or with an established transaction on every destination shard.
func BenchmarkScatterExecuteMultiShard(b *testing.B) {
	for _, shardCount := range []int{1, 2, 8, 32} {
		for _, inTransaction := range []bool{false, true} {
			b.Run(fmt.Sprintf("shards=%d/transaction=%v", shardCount, inTransaction), func(b *testing.B) {
				ctx := b.Context()
				txConn := NewTxConn(nil, &StaticConfig{TxMode: vtgatepb.TransactionMode_MULTI})
				scatter := NewScatterConn("", txConn, nil)
				gateway := &benchmarkScatterGateway{
					result: sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "1"),
				}
				wantResult := sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "1")
				session := econtext.NewSafeSession(&vtgatepb.Session{
					InTransaction: inTransaction,
					Autocommit:    !inTransaction,
					Options:       &querypb.ExecuteOptions{IncludedFields: querypb.ExecuteOptions_ALL},
				})
				if inTransaction {
					gateway.transactionID = 1
				}
				shards := make([]*srvtopo.ResolvedShard, shardCount)
				queries := make([]*querypb.BoundQuery, shardCount)
				for i := range shards {
					target := &querypb.Target{Keyspace: "ks", Shard: strconv.Itoa(i), TabletType: topodatapb.TabletType_PRIMARY}
					shards[i] = &srvtopo.ResolvedShard{Target: target, Gateway: gateway}
					queries[i] = &querypb.BoundQuery{Sql: "select id from user where id = :id", BindVariables: map[string]*querypb.BindVariable{"id": sqltypes.Int64BindVariable(1)}}
					if inTransaction {
						session.ShardSessions = append(session.ShardSessions, &vtgatepb.Session_ShardSession{
							Target:        target,
							TransactionId: gateway.transactionID,
							TabletAlias:   &topodatapb.TabletAlias{Cell: "cell", Uid: uint32(i + 1)},
						})
					}
				}
				execute := func() (*sqltypes.Result, []error) {
					return scatter.ExecuteMultiShard(ctx, nil, shards, queries, session, false, false, nullResultsObserver{}, false)
				}
				result, errs := execute()
				require.Empty(b, errs)
				require.Len(b, result.Rows, shardCount)
				b.ReportAllocs()
				for b.Loop() {
					result, errs = execute()
					if len(errs) != 0 {
						b.Fatal(errs)
					}
				}
				require.Len(b, result.Rows, shardCount)
				require.Equal(b, wantResult.Fields, result.Fields)
				for _, row := range result.Rows {
					require.Equal(b, wantResult.Rows[0], row)
				}
				if inTransaction {
					require.Len(b, session.ShardSessions, shardCount)
				} else {
					require.Empty(b, session.ShardSessions)
				}
			})
		}
	}
}
