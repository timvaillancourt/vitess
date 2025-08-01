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

// FormatImpossibleQuery creates an impossible query in a TrackedBuffer.
// An impossible query is a modified version of a query where all selects have where clauses that are
// impossible for mysql to resolve. This is used in the vtgate and vttablet:
//
// - In the vtgate it's used for joins: if the first query returns no result, then vtgate uses the impossible
// query just to fetch field info from vttablet
// - In the vttablet, it's just an optimization: the field info is fetched once form MySQL, cached and reused
// for subsequent queries
func FormatImpossibleQuery(buf *TrackedBuffer, node SQLNode) {
	switch node := node.(type) {
	case *Select:
		if node.With != nil {
			node.With.Format(buf)
		}
		buf.Myprintf("select %v from ", node.SelectExprs)
		var prefix string
		for _, n := range node.From {
			buf.Myprintf("%s%v", prefix, n)
			prefix = ", "
		}
		buf.Myprintf(" where 1 != 1")
		if node.GroupBy != nil {
			node.GroupBy.Format(buf)
		}
	case *Union:
		if node.With != nil {
			node.With.Format(buf)
		}
		if requiresParen(node.Left) {
			buf.WriteString("(")
			FormatImpossibleQuery(buf, node.Left)
			buf.WriteString(")")
		} else {
			FormatImpossibleQuery(buf, node.Left)
		}

		buf.WriteString(" ")
		if node.Distinct {
			buf.WriteString(UnionStr)
		} else {
			buf.WriteString(UnionAllStr)
		}
		buf.WriteString(" ")

		if requiresParen(node.Right) {
			buf.WriteString("(")
			FormatImpossibleQuery(buf, node.Right)
			buf.WriteString(")")
		} else {
			FormatImpossibleQuery(buf, node.Right)
		}
	default:
		node.Format(buf)
	}
}
