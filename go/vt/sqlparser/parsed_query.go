/*
Copyright 2019 The Vitess Authors.

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
	"encoding/json"
	"fmt"
	"strings"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// ParsedQuery represents a parsed query where
// bind locations are precomputed for fast substitutions.
type ParsedQuery struct {
	Query         string
	bindLocations []BindLocation
	truncateUILen int
}

type BindLocation struct {
	Offset, Length int
}

// NewParsedQuery returns a ParsedQuery of the ast.
func NewParsedQuery(node SQLNode) *ParsedQuery {
	buf := NewTrackedBuffer(nil)
	buf.Myprintf("%v", node)
	return buf.ParsedQuery()
}

// bindValueOverhead is the room the size estimates leave per bound value
// beyond the value's own bytes: the quotes, a `_binary` introducer, the
// tuple parens and ", " separators.
const bindValueOverhead = 16

// bindLargeValue is the value length from which Append sizes the builder
// for the whole query instead of letting it grow. Below it the builder's
// own doubling covers the value in a few small allocations, and the common
// shapes (an IN list of integers, a row of short strings) never pay for a
// pass over the map. Measured on arm64, sizing at 64-byte values cost the
// short-string shape 7%, and pre-growing by a per-placeholder pad cost it
// 3% by moving its append chain onto larger size classes; at 256 with no
// pad both shapes are unchanged.
const bindLargeValue = 256

// sizeHint estimates the generated query's length from the query text and
// the bind variables its placeholders resolve to, so the builder can be
// sized once. It looks each placeholder up rather than ranging the map,
// because the map is whatever the caller sent, not what this query uses:
// vtgate hands a join's right side the whole left-side map on every row,
// and a range would size the builder for binds the query never writes.
// Append only calls this once it has met a large value, so the lookups
// are paid by queries whose substitution already costs far more.
func (pq *ParsedQuery) sizeHint(bindVariables map[string]*querypb.BindVariable) int {
	n := len(pq.Query)
	for _, loc := range pq.bindLocations {
		name := pq.Query[loc.Offset : loc.Offset+loc.Length]
		// Same prefix handling as FetchBindVar: one colon, or two for a list.
		name = strings.TrimPrefix(strings.TrimPrefix(name, ":"), ":")
		if bv, ok := bindVariables[name]; ok {
			n += valueSizeHint(bv)
		}
	}
	return n
}

// valueSizeHint is the room to leave for one bind variable's encoded text:
// its bytes, a sixteenth more for escapes, and the fixed overhead for each
// quoted scalar or tuple element.
func valueSizeHint(bv *querypb.BindVariable) int {
	n := len(bv.Value) + len(bv.Value)/16
	if sqltypes.IsQuoted(bv.Type) {
		n += bindValueOverhead
	}
	for _, v := range bv.Values {
		n += len(v.Value) + len(v.Value)/16 + bindValueOverhead
	}
	return n
}

// GenerateQuery generates a query by substituting the specified
// bindVariables. The extras parameter specifies special parameters
// that can perform custom encoding.
func (pq *ParsedQuery) GenerateQuery(bindVariables map[string]*querypb.BindVariable, extras map[string]Encodable) (string, error) {
	if len(pq.bindLocations) == 0 {
		return pq.Query, nil
	}
	var buf strings.Builder
	buf.Grow(len(pq.Query))
	if err := pq.Append(&buf, bindVariables, extras); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// Append appends the generated query to the provided buffer.
func (pq *ParsedQuery) Append(buf *strings.Builder, bindVariables map[string]*querypb.BindVariable, extras map[string]Encodable) error {
	current := 0
	sized := false
	for _, loc := range pq.bindLocations {
		buf.WriteString(pq.Query[current:loc.Offset])
		name := pq.Query[loc.Offset : loc.Offset+loc.Length]
		if encodable, ok := extras[name[1:]]; ok {
			encodable.EncodeSQL(buf)
		} else {
			supplied, _, err := FetchBindVar(name, bindVariables)
			if err != nil {
				return err
			}
			// The first large value sizes the builder for the whole query,
			// once; Grow is a no-op if it already fits. Growing per value let
			// a query with a few KB of string binds double its way through a
			// dozen allocations, and sizing up front would charge every
			// query a pass over the map that a query of small binds never
			// needs.
			if !sized && (len(supplied.Value) >= bindLargeValue || len(supplied.Values) > 0) {
				if need := pq.sizeHint(bindVariables) - buf.Len(); need > 0 {
					buf.Grow(need)
				}
				sized = true
			}
			EncodeValue(buf, supplied)
		}
		current = loc.Offset + loc.Length
	}
	buf.WriteString(pq.Query[current:])
	return nil
}

func (pq *ParsedQuery) BindLocations() []BindLocation {
	return pq.bindLocations
}

// MarshalJSON is a custom JSON marshaler for ParsedQuery.
func (pq *ParsedQuery) MarshalJSON() ([]byte, error) {
	return json.Marshal(pq.Query)
}

// EncodeValue encodes one bind variable value into the query.
func EncodeValue(buf *strings.Builder, value *querypb.BindVariable) {
	switch value.Type {
	case querypb.Type_TUPLE:
		buf.WriteByte('(')
		for i, bv := range value.Values {
			if i != 0 {
				buf.WriteString(", ")
			}
			sqltypes.ProtoToValue(bv).EncodeSQLStringBuilder(buf)
		}
		buf.WriteByte(')')
	case querypb.Type_ROW_TUPLE:
		for i, bv := range value.Values {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString("row")
			sqltypes.ProtoToValue(bv).EncodeSQLStringBuilder(buf)
		}
	case querypb.Type_RAW:
		v, _ := sqltypes.BindVariableToValue(value)
		buf.Write(v.Raw())
	default:
		v, _ := sqltypes.BindVariableToValue(value)
		v.EncodeSQLStringBuilder(buf)
	}
}

// FetchBindVar resolves the bind variable by fetching it from bindVariables.
func FetchBindVar(name string, bindVariables map[string]*querypb.BindVariable) (val *querypb.BindVariable, isList bool, err error) {
	name = name[1:]
	if name[0] == ':' {
		name = name[1:]
		isList = true
	}
	supplied, ok := bindVariables[name]
	if !ok {
		return nil, false, fmt.Errorf("missing bind var %s", name)
	}

	if isList {
		switch supplied.Type {
		case querypb.Type_TUPLE, querypb.Type_ROW_TUPLE:
		default:
			return nil, false, fmt.Errorf("unexpected list arg type (%v) for key %s", supplied.Type, name)
		}
		if len(supplied.Values) == 0 {
			return nil, false, fmt.Errorf("empty list supplied for %s", name)
		}
		return supplied, true, nil
	}

	if supplied.Type == querypb.Type_TUPLE {
		return nil, false, fmt.Errorf("unexpected arg type (TUPLE) for non-list key %s", name)
	}

	return supplied, false, nil
}

// ParseAndBind is a one step sweep that binds variables to an input query, in order of placeholders.
// It is useful when one doesn't have any parser-variables, just bind variables.
// Example:
//
//	query, err := ParseAndBind("select * from tbl where name=%a", sqltypes.StringBindVariable("it's me"))
func ParseAndBind(in string, binds ...*querypb.BindVariable) (query string, err error) {
	vars := make([]any, len(binds))
	for i, bv := range binds {
		switch bv.Type {
		case querypb.Type_TUPLE:
			vars[i] = fmt.Sprintf("::vars%d", i)
		default:
			vars[i] = fmt.Sprintf(":var%d", i)
		}
	}
	parsed := BuildParsedQuery(in, vars...)

	bindVars := map[string]*querypb.BindVariable{}
	for i, bv := range binds {
		switch bv.Type {
		case querypb.Type_TUPLE:
			bindVars[fmt.Sprintf("vars%d", i)] = binds[i]
		default:
			bindVars[fmt.Sprintf("var%d", i)] = binds[i]
		}
	}
	return parsed.GenerateQuery(bindVars, nil)
}
