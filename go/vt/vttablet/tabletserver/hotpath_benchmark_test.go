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

package tabletserver

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// BenchmarkExecuteWarmSelect includes connection pooling and result decoding
// over a local fake-MySQL socket, but no gRPC or real MySQL execution.
func BenchmarkExecuteWarmSelect(b *testing.B) {
	// Fixture logs can split benchmark result lines when using -count.
	previousLogger := log.SwapLogger(nil)
	b.Cleanup(func() { log.SwapLogger(previousLogger) })

	for _, rowCount := range []int{1, 256} {
		b.Run(fmt.Sprintf("rows=%d", rowCount), func(b *testing.B) {
			ctx := b.Context()
			cfg := tabletenv.NewDefaultConfig()
			cfg.Consolidator = tabletenv.Disable
			cfg.QueryCacheDoorkeeper = false
			db, tsv := setupTabletServerTestCustom(b, ctx, cfg, "", vtenv.NewTestEnv())
			b.Cleanup(func() {
				tsv.StopService()
				db.Close()
				tsv.TopoServer().Close()
			})
			const query = "select pk, name_string from test_table where pk >= :id"
			finalSQL := fmt.Sprintf("select pk, name_string from test_table where pk >= 1 limit %d", tsv.MaxResultSize()+1)
			want := &sqltypes.Result{Fields: sqltypes.MakeTestFields("pk|name_string", "int32|varchar")}
			for i := range rowCount {
				want.Rows = append(want.Rows, []sqltypes.Value{sqltypes.NewInt32(int32(i + 1)), sqltypes.NewVarChar("a short result value")})
			}
			db.AddQuery(finalSQL, want)
			// Keep the fake's query history bounded during long benchmark runs.
			db.SetBeforeFunc(finalSQL, db.ResetQueryLog)
			target := tsv.sm.Target()
			bindVars := map[string]*querypb.BindVariable{"id": sqltypes.Int64BindVariable(1)}
			options := &querypb.ExecuteOptions{IncludedFields: querypb.ExecuteOptions_ALL}
			result, err := tsv.Execute(ctx, nil, target, query, bindVars, 0, 0, options)
			require.NoError(b, err)
			require.Equal(b, want.Rows, result.Rows)
			hits := tsv.qe.plans.Metrics.Hits()
			misses := tsv.qe.plans.Metrics.Misses()
			calls := db.GetQueryCalledNum(finalSQL)

			b.ReportAllocs()
			for b.Loop() {
				result, err = tsv.Execute(ctx, nil, target, query, bindVars, 0, 0, options)
				if err != nil {
					b.Fatal(err)
				}
			}
			require.Equal(b, want.Rows, result.Rows)
			require.Len(b, result.Fields, len(want.Fields))
			for i, field := range result.Fields {
				require.Equal(b, want.Fields[i].Name, field.Name)
				require.Equal(b, want.Fields[i].Type, field.Type)
			}
			require.Equal(b, int64(b.N), tsv.qe.plans.Metrics.Hits()-hits)
			require.Equal(b, misses, tsv.qe.plans.Metrics.Misses())
			require.Equal(b, b.N, db.GetQueryCalledNum(finalSQL)-calls)
		})
	}
}

// BenchmarkGenerateFinalSQL isolates per-request bind expansion and margin
// comments. Parsing and planning are excluded; annotation has separate controls.
func BenchmarkGenerateFinalSQL(b *testing.B) {
	type testCase struct {
		name     string
		query    string
		bindVars map[string]*querypb.BindVariable
		want     string
	}
	cases := []testCase{
		{
			name:     "scalar",
			query:    "select pk from test_table where pk = :id",
			bindVars: map[string]*querypb.BindVariable{"id": sqltypes.Int64BindVariable(1)},
			want:     "select pk from test_table where pk = 1",
		},
		{
			name:     "escaped_text",
			query:    "select pk from test_table where name_string = :name",
			bindVars: map[string]*querypb.BindVariable{"name": sqltypes.StringBindVariable("O'Reilly\\path")},
			want:     "select pk from test_table where name_string = 'O\\'Reilly\\\\path'",
		},
		{
			name:     "no_binds",
			query:    "select pk from test_table where pk = 1",
			bindVars: map[string]*querypb.BindVariable{"unused": sqltypes.Int64BindVariable(1)},
			want:     "select pk from test_table where pk = 1",
		},
		{
			name:     "null",
			query:    "select :value from test_table",
			bindVars: map[string]*querypb.BindVariable{"value": sqltypes.NullBindVariable},
			want:     "select null from test_table",
		},
		{
			name:  "repeated_numeric",
			query: "select :id, :amount from test_table where pk = :id",
			bindVars: map[string]*querypb.BindVariable{
				"id":     sqltypes.Int64BindVariable(-9223372036854775808),
				"amount": sqltypes.DecimalBindVariable("12.50"),
				"unused": sqltypes.StringBindVariable(strings.Repeat("x", 4096)),
			},
			want: "select -9223372036854775808, 12.50 from test_table where pk = -9223372036854775808",
		},
	}
	for _, count := range []int{1, 16, 64, 256, 1024} {
		values := make([]int64, count)
		literals := make([]string, count)
		for i := range values {
			values[i] = int64(i + 1)
			literals[i] = strconv.Itoa(i + 1)
		}
		bind, err := sqltypes.BuildBindVariable(values)
		require.NoError(b, err)
		cases = append(cases, testCase{
			name:     fmt.Sprintf("in=%d", count),
			query:    "select pk from test_table where pk in ::ids",
			bindVars: map[string]*querypb.BindVariable{"ids": bind},
			want:     "select pk from test_table where pk in (" + strings.Join(literals, ", ") + ")",
		})
	}
	type variant struct {
		name         string
		comments     sqlparser.MarginComments
		consolidator querypb.ExecuteOptions_Consolidator
		annotate     bool
	}
	comments := sqlparser.MarginComments{Leading: "/* app */ ", Trailing: " /* trace */"}
	for _, tc := range cases {
		variants := []variant{
			{name: "comments=false/default"},
			{name: "comments=true/default", comments: comments},
			{name: "comments=false/disabled", consolidator: querypb.ExecuteOptions_CONSOLIDATOR_DISABLED},
			{name: "comments=true/disabled", comments: comments, consolidator: querypb.ExecuteOptions_CONSOLIDATOR_DISABLED},
		}
		if tc.name == "scalar" {
			variants = append(variants,
				variant{name: "comments=true/enabled", comments: comments, consolidator: querypb.ExecuteOptions_CONSOLIDATOR_ENABLED},
				variant{name: "leading/disabled", comments: sqlparser.MarginComments{Leading: comments.Leading}, consolidator: querypb.ExecuteOptions_CONSOLIDATOR_DISABLED},
				variant{name: "trailing/disabled", comments: sqlparser.MarginComments{Trailing: comments.Trailing}, consolidator: querypb.ExecuteOptions_CONSOLIDATOR_DISABLED},
				variant{name: "large_comments/disabled", comments: sqlparser.MarginComments{Leading: "/* " + strings.Repeat("trace", 1024) + " */ "}, consolidator: querypb.ExecuteOptions_CONSOLIDATOR_DISABLED},
				variant{name: "annotation/no_comments", annotate: true, consolidator: querypb.ExecuteOptions_CONSOLIDATOR_DISABLED},
				variant{name: "annotation/comments", comments: comments, annotate: true, consolidator: querypb.ExecuteOptions_CONSOLIDATOR_DISABLED},
			)
		}
		for _, v := range variants {
			b.Run(tc.name+"/"+v.name, func(b *testing.B) {
				stmt, err := sqlparser.NewTestParser().Parse(tc.query)
				require.NoError(b, err)
				parsed := sqlparser.NewParsedQuery(stmt)
				qre := &QueryExecutor{
					ctx: b.Context(),
					tsv: &TabletServer{
						config: &tabletenv.TabletConfig{AnnotateQueries: v.annotate},
						sm:     &stateManager{target: &querypb.Target{TabletType: topodatapb.TabletType_PRIMARY}},
					},
				}
				if v.consolidator != querypb.ExecuteOptions_CONSOLIDATOR_UNSPECIFIED {
					qre.options = &querypb.ExecuteOptions{Consolidator: v.consolidator}
				}
				wantLeading := v.comments.Leading
				if v.annotate {
					wantLeading = "/* @PRIMARY */ " + wantLeading
				}
				var query, withoutComments string
				b.ReportAllocs()
				for b.Loop() {
					// Annotation prepends to Leading, so each iteration needs fresh request state.
					qre.marginComments = v.comments
					query, withoutComments, err = qre.generateFinalSQL(parsed, tc.bindVars)
					if err != nil {
						b.Fatal(err)
					}
				}
				require.Equal(b, tc.want, withoutComments)
				require.Equal(b, wantLeading, qre.marginComments.Leading)
				require.Equal(b, wantLeading+tc.want+v.comments.Trailing, query)
			})
		}
	}
}
