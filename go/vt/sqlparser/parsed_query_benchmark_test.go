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

package sqlparser

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func BenchmarkParsedQueryTuple(b *testing.B) {
	const query = "select id from t where id in ::ids and active = 1"
	type fixture struct {
		name       string
		query      string
		count      int
		typ        querypb.Type
		literal    string
		mixedIndex int
		mixed      *querypb.Value
		prefix     string
		capacity   int
		appendSQL  bool
	}
	var fixtures []fixture
	for _, count := range []int{1, 16, 32, 64, 256, 1024} {
		fixtures = append(fixtures, fixture{name: fmt.Sprintf("narrow/count=%d", count), query: query, count: count, typ: sqltypes.Int64, literal: "1"})
	}
	for _, kind := range []struct {
		name    string
		typ     querypb.Type
		literal string
	}{
		{name: "negative", typ: sqltypes.Int64, literal: "-123456"},
		{name: "min_int64", typ: sqltypes.Int64, literal: "-9223372036854775808"},
		{name: "max_uint64", typ: sqltypes.Uint64, literal: "18446744073709551615"},
	} {
		for _, count := range []int{1, 16, 256} {
			fixtures = append(fixtures, fixture{name: fmt.Sprintf("%s/count=%d", kind.name, count), query: query, count: count, typ: kind.typ, literal: kind.literal})
		}
	}
	for _, shape := range []struct{ name, query string }{
		{name: "long_prefix", query: "select " + strings.Repeat("id, ", 128) + "id from t where id in ::ids"},
		{name: "long_suffix", query: query + strings.Repeat(" and active = 1", 128)},
		{name: "multiple_tuples", query: query + " or other_id in ::ids"},
	} {
		fixtures = append(fixtures, fixture{name: shape.name, query: shape.query, count: 256, typ: sqltypes.Int64, literal: "123"})
	}
	for _, mixed := range []struct {
		name  string
		value *querypb.Value
	}{
		{name: "text", value: &querypb.Value{Type: sqltypes.VarChar, Value: []byte("O'Reilly\\path")}},
		{name: "null", value: &querypb.Value{Type: sqltypes.Null}},
	} {
		for _, index := range []int{0, 255} {
			fixtures = append(fixtures, fixture{name: fmt.Sprintf("mixed_%s/index=%d", mixed.name, index), query: query, count: 256, typ: sqltypes.Int64, literal: "1", mixedIndex: index, mixed: mixed.value})
		}
	}
	stmt, err := NewTestParser().Parse(query)
	require.NoError(b, err)
	parsed := NewParsedQuery(stmt)
	for _, capacity := range []int{64, 256, 4096} {
		const prefix = "/* outer */ "
		var probe strings.Builder
		probe.Grow(capacity)
		lengthAtTuple := len(prefix) + parsed.BindLocations()[0].Offset
		// Bracket the count at which a proposed capacity-relative sizing hint
		// would engage (len + 2*count > 2*cap). No such hint is in the tree yet,
		// since Append does no sizing at all, so these are baseline controls.
		// Note this is not where the builder itself first grows, which every
		// count here is well past.
		boundary := probe.Cap() - lengthAtTuple/2 - lengthAtTuple%2
		for _, count := range []int{16, 256, boundary - 1, boundary, boundary + 1} {
			fixtures = append(fixtures, fixture{
				name: fmt.Sprintf("append/capacity=%d/count=%d", capacity, count), query: query,
				count: count, typ: sqltypes.Int64, literal: "1", prefix: prefix, capacity: capacity, appendSQL: true,
			})
		}
	}
	for _, f := range fixtures {
		b.Run(f.name, func(b *testing.B) {
			stmt, err := NewTestParser().Parse(f.query)
			require.NoError(b, err)
			parsed := NewParsedQuery(stmt)
			values := make([]*querypb.Value, f.count)
			literals := make([]string, f.count)
			for i := range values {
				values[i] = &querypb.Value{Type: f.typ, Value: []byte(f.literal)}
				literals[i] = f.literal
			}
			if f.mixed != nil {
				values[f.mixedIndex] = f.mixed
				var reference strings.Builder
				sqltypes.ProtoToValue(f.mixed).EncodeSQL(&reference)
				literals[f.mixedIndex] = reference.String()
			}
			bindVars := map[string]*querypb.BindVariable{"ids": {Type: sqltypes.Tuple, Values: values}}
			want := f.prefix + strings.ReplaceAll(parsed.Query, "::ids", "("+strings.Join(literals, ", ")+")")
			var got string
			b.ReportAllocs()
			if f.appendSQL {
				for b.Loop() {
					var buf strings.Builder
					buf.Grow(f.capacity)
					buf.WriteString(f.prefix)
					err = parsed.Append(&buf, bindVars, nil)
					if err != nil {
						b.Fatal(err)
					}
					got = buf.String()
				}
			} else {
				for b.Loop() {
					got, err = parsed.GenerateQuery(bindVars, nil)
					if err != nil {
						b.Fatal(err)
					}
				}
			}
			require.Equal(b, want, got)
		})
	}
}
