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

// bindLargeValue is the encoded size from which Append sizes the builder
// for the whole query instead of letting it grow: a scalar of that many
// bytes, or a tuple whose estimate reaches it. Below it the builder's own
// doubling covers the value in a few small allocations, and the common
// shapes (a short IN list of integers, a row of short strings) never pay
// for a pass over the placeholders. Measured on arm64, sizing at 64-byte
// values cost the short-string shape 7%, and pre-growing by a
// per-placeholder pad cost it 3% by moving its append chain onto larger
// size classes; at 256 with no pad both shapes are unchanged, and a
// three-int IN list got 16% faster once tuples below the estimate stopped
// being sized.
const bindLargeValue = 256

// sizeHint estimates the generated query's length from the query text and
// the bind variables its placeholders resolve to, so the builder can be
// sized once. It looks each placeholder up rather than ranging the map,
// because the map is whatever the caller sent, not what this query uses:
// vtgate hands a join's right side the whole left-side map on every row,
// and a range would size the builder for binds the query never writes.
// Append only calls this once it has met a large value, so the lookups
// are paid by queries whose substitution already costs far more. Custom
// Encodable values are skipped because the interface has no
// side-effect-free size operation; encoding one twice would be a stronger
// contract than it promises.
//
// It reports false at the first placeholder it cannot resolve, rather than
// counting the rest: that query is about to be rejected, and sizing for the
// whole of it first means a request whose one missing bind var comes early
// still allocates for every large value named after it.
func (pq *ParsedQuery) sizeHint(bindVariables map[string]*querypb.BindVariable, extras map[string]Encodable) (int, bool) {
	n := len(pq.Query)
	for _, loc := range pq.bindLocations {
		name := pq.Query[loc.Offset : loc.Offset+loc.Length]
		if _, ok := extras[name[1:]]; ok {
			continue
		}
		bv, _, err := FetchBindVar(name, bindVariables)
		if err != nil {
			return 0, false
		}
		n += valueSizeHint(bv)
	}
	return n, true
}

// valueSizeHint is the best-effort room to leave for one bind variable's
// encoded text: its bytes, a sixteenth more for escapes, and the fixed
// overhead for each quoted scalar or tuple element. Escape-dense values can
// still make the builder grow once.
//
// It reads the same field EncodeValue does and no other. Nothing rejects a
// bind variable that also carries the field its type does not use -- a NULL
// with a Value, a scalar with Values -- and counting those sizes the builder
// for bytes that are never written.
func valueSizeHint(bv *querypb.BindVariable) int {
	switch bv.Type {
	case querypb.Type_TUPLE, querypb.Type_ROW_TUPLE:
		var n int
		for _, v := range bv.Values {
			n += len(v.Value) + len(v.Value)/16 + bindValueOverhead
		}
		return n
	case querypb.Type_NULL_TYPE:
		// EncodeValue writes the literal and never looks at Value.
		return len(sqltypes.NullStr)
	}
	n := len(bv.Value) + len(bv.Value)/16
	if sqltypes.IsQuoted(bv.Type) {
		n += bindValueOverhead
	}
	return n
}

// bindLargeTupleLen is the element count from which a tuple's estimate
// reaches bindLargeValue on the per-element overhead alone, rounded up so it
// stays a sufficient condition whatever the two constants are.
const bindLargeTupleLen = (bindLargeValue + bindValueOverhead - 1) / bindValueOverhead

// isLargeBind reports whether bv is worth sizing the builder for: a scalar
// of bindLargeValue bytes or more, or a tuple whose estimate reaches that.
// The count check spares long IN lists the pass over their values; short
// ones stay on the builder's own doubling. It switches on the type for the
// same reason valueSizeHint does: the field a type does not encode says
// nothing about how much room the query needs.
func isLargeBind(bv *querypb.BindVariable) bool {
	switch bv.Type {
	case querypb.Type_TUPLE, querypb.Type_ROW_TUPLE:
		return len(bv.Values) >= bindLargeTupleLen || valueSizeHint(bv) >= bindLargeValue
	case querypb.Type_NULL_TYPE:
		return false
	}
	return len(bv.Value) >= bindLargeValue
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
	queryStart := buf.Len()
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
			// The first large bind sizes the builder for the whole query,
			// once; Grow is a no-op if it already fits. Growing per value let
			// a query with a few KB of string binds double its way through a
			// dozen allocations, and sizing up front would charge every
			// query a pass over the placeholders that a query of small binds
			// never needs.
			if !sized && isLargeBind(supplied) {
				// Sized once either way: if the estimate stopped at a
				// placeholder with no bind var, retrying it for the next
				// large value walks the list again for a query that is
				// going to be rejected.
				if hint, ok := pq.sizeHint(bindVariables, extras); ok {
					if need := hint - (buf.Len() - queryStart); need > 0 {
						buf.Grow(need)
					}
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
