/*
Copyright 2023 The Vitess Authors.

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

package operators

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type Limit struct {
	unaryOperator
	AST *sqlparser.Limit

	// Top is true if the limit is a top level limit. To optimise, we push LIMIT to the RHS of joins,
	// but we need to still LIMIT the total result set to the top level limit.
	Top bool

	// Once we have pushed the top level Limit down, we mark it as pushed so that we don't push it down again.
	Pushed bool
}

func newLimit(op Operator, ast *sqlparser.Limit, top bool) *Limit {
	return &Limit{
		unaryOperator: newUnaryOp(op),
		AST:           ast,
		Top:           top,
	}
}

func (l *Limit) Clone(inputs []Operator) Operator {
	k := *l
	k.Source = inputs[0]
	k.AST = sqlparser.Clone(l.AST)
	return &k
}

func (l *Limit) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) Operator {
	l.Source = l.Source.AddPredicate(ctx, expr)
	return l
}

func (l *Limit) AddColumn(ctx *plancontext.PlanningContext, reuse bool, gb bool, expr *sqlparser.AliasedExpr) int {
	return l.Source.AddColumn(ctx, reuse, gb, expr)
}

func (l *Limit) AddWSColumn(ctx *plancontext.PlanningContext, offset int, underRoute bool) int {
	return l.Source.AddWSColumn(ctx, offset, underRoute)
}

func (l *Limit) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) int {
	return l.Source.FindCol(ctx, expr, underRoute)
}

func (l *Limit) GetColumns(ctx *plancontext.PlanningContext) []*sqlparser.AliasedExpr {
	return l.Source.GetColumns(ctx)
}

func (l *Limit) GetSelectExprs(ctx *plancontext.PlanningContext) []sqlparser.SelectExpr {
	return l.Source.GetSelectExprs(ctx)
}

func (l *Limit) GetOrdering(ctx *plancontext.PlanningContext) []OrderBy {
	return l.Source.GetOrdering(ctx)
}

func (l *Limit) ShortDescription() string {
	r := sqlparser.String(l.AST)
	if l.Top {
		r += " Top"
	}
	if l.Pushed {
		r += " Pushed"
	}
	return r
}
