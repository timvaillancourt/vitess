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
	"fmt"
	"slices"
	"strings"

	"vitess.io/vitess/go/slice"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type (
	// Aggregator represents a GroupBy γ relational operator.
	// Both all aggregations and no grouping, and the inverse
	// of all grouping and no aggregations are valid configurations of this operator
	Aggregator struct {
		unaryOperator
		Columns []*sqlparser.AliasedExpr

		WithRollup   bool
		Grouping     []GroupBy
		Aggregations []Aggr

		// We support a single distinct aggregation per aggregator. It is stored here.
		// When planning the ordering that the OrderedAggregate will require,
		// this needs to be the last ORDER BY expression
		DistinctExpr sqlparser.Expr

		// Pushed will be set to true once this aggregation has been pushed deeper in the tree
		Pushed        bool
		offsetPlanned bool

		// Original will only be true for the original aggregator created from the AST
		Original bool

		// ResultColumns signals how many columns will be produced by this operator
		// This is used to truncate the columns in the final result
		ResultColumns int

		// Truncate is set to true if the columns produced by this operator should be truncated if we added any additional columns
		Truncate bool

		QP *QueryProjection

		DT *DerivedTable
	}
)

func (a *Aggregator) Clone(inputs []Operator) Operator {
	kopy := *a
	kopy.Source = inputs[0]
	kopy.Columns = slices.Clone(a.Columns)
	kopy.Grouping = slices.Clone(a.Grouping)
	kopy.Aggregations = slices.Clone(a.Aggregations)
	return &kopy
}

func (a *Aggregator) AddPredicate(_ *plancontext.PlanningContext, expr sqlparser.Expr) Operator {
	return newFilter(a, expr)
}

// createNonGroupingAggr creates the appropriate aggregation for a non-grouping, non-aggregation column
// If the expression is constant, it returns AggregateConstant, otherwise AggregateAnyValue
func createNonGroupingAggr(expr *sqlparser.AliasedExpr) Aggr {
	if sqlparser.IsConstant(expr.Expr) {
		return NewAggr(opcode.AggregateConstant, nil, expr, expr.ColumnName())
	} else {
		return NewAggr(opcode.AggregateAnyValue, nil, expr, expr.ColumnName())
	}
}

func (a *Aggregator) addColumnWithoutPushing(ctx *plancontext.PlanningContext, expr *sqlparser.AliasedExpr, addToGroupBy bool) int {
	offset := len(a.Columns)
	a.Columns = append(a.Columns, expr)

	if addToGroupBy {
		groupBy := NewGroupBy(expr.Expr)
		groupBy.ColOffset = offset
		a.Grouping = append(a.Grouping, groupBy)
	} else {
		var aggr Aggr
		switch e := expr.Expr.(type) {
		case sqlparser.AggrFunc:
			aggr = createAggrFromAggrFunc(e, expr)
		case *sqlparser.FuncExpr:
			if ctx.IsAggr(e) {
				aggr = NewAggr(opcode.AggregateUDF, nil, expr, expr.ColumnName())
			}
		}

		if aggr.Alias == "" {
			aggr = createNonGroupingAggr(expr)
		}
		aggr.ColOffset = offset
		a.Aggregations = append(a.Aggregations, aggr)
	}
	return offset
}

func (a *Aggregator) isDerived() bool {
	return a.DT != nil
}

func (a *Aggregator) derivedName() string {
	if a.DT == nil {
		return ""
	}

	return a.DT.Alias
}

func (a *Aggregator) FindCol(ctx *plancontext.PlanningContext, in sqlparser.Expr, underRoute bool) int {
	if underRoute && a.isDerived() {
		// We don't want to use columns on this operator if it's a derived table under a route.
		// In this case, we need to add a Projection on top of this operator to make the column available
		return -1
	}

	expr := a.DT.RewriteExpression(ctx, in)
	if offset, found := canReuseColumn(ctx, a.Columns, expr, extractExpr); found {
		a.checkOffset(offset)
		return offset
	}
	return -1
}

func (a *Aggregator) checkOffset(offset int) {
	// if the offset is greater than the number of columns we expect to produce, we need to update the number of columns
	// this is to make sure that the column is not truncated in the final result
	if a.ResultColumns > 0 && a.ResultColumns <= offset {
		a.ResultColumns = offset + 1
	}
}

func (a *Aggregator) AddColumn(ctx *plancontext.PlanningContext, reuse bool, groupBy bool, ae *sqlparser.AliasedExpr) (offset int) {
	a.planOffsets(ctx)

	defer func() {
		a.checkOffset(offset)
	}()
	rewritten := a.DT.RewriteExpression(ctx, ae.Expr)

	ae = &sqlparser.AliasedExpr{
		Expr: rewritten,
		As:   ae.As,
	}

	if reuse {
		offset := a.findColInternal(ctx, ae, groupBy)
		if offset >= 0 {
			return offset
		}
	}

	// Upon receiving a weight string function from an upstream operator, check for an existing grouping on the argument expression.
	// If a grouping is found, continue to push the function down, marking it with 'addToGroupBy' to ensure it's correctly treated as a grouping column.
	// This process also sets the weight string column offset, eliminating the need for a later addition in the aggregator operator's planOffset.
	if wsExpr, isWS := rewritten.(*sqlparser.WeightStringFuncExpr); isWS {
		idx := slices.IndexFunc(a.Grouping, func(by GroupBy) bool {
			return ctx.SemTable.EqualsExprWithDeps(wsExpr.Expr, by.Inner)
		})
		if idx >= 0 {
			a.Grouping[idx].WSOffset = len(a.Columns)
			groupBy = true
		}
	}

	if !groupBy {
		aggr := createNonGroupingAggr(ae)
		aggr.ColOffset = len(a.Columns)
		a.Aggregations = append(a.Aggregations, aggr)
	}

	offset = len(a.Columns)
	a.Columns = append(a.Columns, ae)
	incomingOffset := a.Source.AddColumn(ctx, false, groupBy, ae)

	if offset != incomingOffset {
		panic(errFailedToPlan(ae))
	}

	return offset
}

func (a *Aggregator) AddWSColumn(ctx *plancontext.PlanningContext, offset int, underRoute bool) int {
	if !underRoute {
		a.planOffsets(ctx)
	}

	if len(a.Columns) <= offset {
		panic(vterrors.VT13001("offset out of range"))
	}

	var expr sqlparser.Expr
	// first search for the offset among the groupings
	for i, by := range a.Grouping {
		if by.ColOffset != offset {
			continue
		}
		if by.WSOffset >= 0 {
			// ah, we already have a weigh_string for this column. let's return it as is
			return by.WSOffset
		}

		// we need to add a WS column
		a.Grouping[i].WSOffset = len(a.Columns)
		expr = a.Columns[offset].Expr
		break
	}

	if expr == nil {
		for i, aggr := range a.Aggregations {
			if aggr.ColOffset != offset {
				continue
			}
			if aggr.WSOffset >= 0 {
				// ah, we already have a weigh_string for this column. let's return it as is
				return aggr.WSOffset
			}

			a.Aggregations[i].WSOffset = len(a.Columns)
			expr = a.Columns[offset].Expr
			break
		}
	}

	if expr == nil {
		panic(vterrors.VT13001("could not find expression at offset"))
	}

	wsExpr := weightStringFor(expr)
	wsAe := aeWrap(wsExpr)

	wsOffset := len(a.Columns)
	a.Columns = append(a.Columns, wsAe)
	if underRoute {
		// if we are under a route, we are done here.
		// the column will be use when creating the query to send to the tablet, and that is all we need
		return wsOffset
	}

	incomingOffset := a.Source.AddWSColumn(ctx, offset, false)

	if wsOffset != incomingOffset {
		// TODO: we could handle this case by adding a projection on under the aggregator to make the columns line up
		panic(errFailedToPlan(wsAe))
	}
	a.checkOffset(wsOffset)
	return wsOffset
}

func (a *Aggregator) findColInternal(ctx *plancontext.PlanningContext, ae *sqlparser.AliasedExpr, addToGroupBy bool) int {
	expr := ae.Expr
	offset := a.FindCol(ctx, expr, false)
	if offset >= 0 {
		return offset
	}

	// Aggregator is little special and cannot work if the input offset are not matched with the aggregation columns.
	// So, before pushing anything from above the aggregator offset planning needs to be completed.
	a.planOffsets(ctx)
	if offset, found := canReuseColumn(ctx, a.Columns, expr, extractExpr); found {
		return offset
	}

	if addToGroupBy {
		panic(vterrors.VT13001(fmt.Sprintf("did not expect to add group by here: %s", sqlparser.String(expr))))
	}

	return -1
}

func isDerived(op Operator) bool {
	switch op := op.(type) {
	case *Horizon:
		return op.IsDerived()
	case selectExpressions:
		return op.derivedName() != ""
	default:
		return false
	}
}

func (a *Aggregator) GetColumns(ctx *plancontext.PlanningContext) (res []*sqlparser.AliasedExpr) {
	if isDerived(a.Source) && len(a.Aggregations) > 0 {
		return truncate(a, a.Columns)
	}

	// we update the incoming columns, so we know about any new columns that have been added
	// in the optimization phase, other operators could be pushed down resulting in additional columns for aggregator.
	// Aggregator should be made aware of these to truncate them in final result.
	columns := a.Source.GetColumns(ctx)

	// if this operator is producing more columns than expected, we want to know about it
	if len(columns) > len(a.Columns) {
		a.Columns = append(a.Columns, columns[len(a.Columns):]...)
	}

	return truncate(a, a.Columns)
}

func (a *Aggregator) GetSelectExprs(ctx *plancontext.PlanningContext) []sqlparser.SelectExpr {
	return transformColumnsToSelectExprs(ctx, a)
}

func (a *Aggregator) ShortDescription() string {
	columns := slice.Map(a.Columns, func(from *sqlparser.AliasedExpr) string {
		return sqlparser.String(from)
	})
	if a.DT != nil {
		columns = append([]string{a.DT.String()}, columns...)
	}

	org := ""
	if a.Original {
		org = "ORG "
	}
	if a.ResultColumns > 0 {
		org += fmt.Sprintf(":%d ", a.ResultColumns)
	}

	if len(a.Grouping) == 0 {
		return fmt.Sprintf("%s%s", org, strings.Join(columns, ", "))
	}

	var grouping []string
	for _, gb := range a.Grouping {
		grouping = append(grouping, sqlparser.String(gb.Inner))
	}
	var rollUp string
	if a.WithRollup {
		rollUp = " with rollup"
	}

	return fmt.Sprintf(
		"%s%s group by %s%s",
		org,
		strings.Join(columns, ", "),
		strings.Join(grouping, ","),
		rollUp,
	)
}

func (a *Aggregator) GetOrdering(ctx *plancontext.PlanningContext) []OrderBy {
	return a.Source.GetOrdering(ctx)
}

func (a *Aggregator) planOffsets(ctx *plancontext.PlanningContext) Operator {
	if a.offsetPlanned {
		return nil
	}
	defer func() {
		a.offsetPlanned = true
	}()
	if !a.Pushed {
		a.planOffsetsNotPushed(ctx)
		return nil
	}

	for idx, gb := range a.Grouping {
		if gb.ColOffset == -1 {
			offset := a.internalAddColumn(ctx, aeWrap(gb.Inner), false)
			a.Grouping[idx].ColOffset = offset
			gb.ColOffset = offset
		}
		if gb.WSOffset != -1 || !ctx.NeedsWeightString(gb.Inner) {
			continue
		}

		offset := a.internalAddColumn(ctx, aeWrap(weightStringFor(gb.Inner)), true)
		a.Grouping[idx].WSOffset = offset
	}

	for idx, aggr := range a.Aggregations {
		if !aggr.NeedsWeightString(ctx) {
			continue
		}
		var offset int
		if aggr.PushedDown {
			// if we have already pushed down aggregation, we need to use
			// the weight string of the aggregation and not the argument
			offset = a.internalAddColumn(ctx, aeWrap(weightStringFor(aggr.Func)), false)
		} else {
			// If we have not pushed down the aggregation, we need the weight_string of the argument
			arg := aggr.getPushColumn()
			offset = a.internalAddColumn(ctx, aeWrap(weightStringFor(arg)), true)
		}
		a.Aggregations[idx].WSOffset = offset
	}
	return nil
}

func (aggr Aggr) setPushColumn(exprs []sqlparser.Expr) {
	if aggr.Func == nil {
		if len(exprs) > 1 {
			panic(vterrors.VT13001(fmt.Sprintf("unexpected number of expression in an random aggregation: %s", sqlparser.SliceString(exprs))))
		}
		aggr.Original.Expr = exprs[0]
		return
	}

	err := aggr.Func.SetArgs(exprs)
	if err != nil {
		panic(err)
	}
}

func (aggr Aggr) getPushColumn() sqlparser.Expr {
	switch aggr.OpCode {
	case opcode.AggregateAnyValue, opcode.AggregateConstant:
		return aggr.Original.Expr
	case opcode.AggregateCountStar:
		return sqlparser.NewIntLiteral("1")
	case opcode.AggregateGroupConcat:
		if len(aggr.Func.GetArgs()) > 1 {
			panic(vterrors.VT12001("group_concat with more than 1 column"))
		}
		return aggr.Func.GetArg()
	default:
		if len(aggr.Func.GetArgs()) > 1 {
			panic(vterrors.VT03001(sqlparser.String(aggr.Func)))
		}
		return aggr.Func.GetArg()
	}
}

func (aggr Aggr) getPushColumnExprs() []sqlparser.Expr {
	switch aggr.OpCode {
	case opcode.AggregateAnyValue, opcode.AggregateConstant:
		return []sqlparser.Expr{aggr.Original.Expr}
	case opcode.AggregateCountStar:
		return []sqlparser.Expr{sqlparser.NewIntLiteral("1")}
	default:
		if aggr.Func == nil {
			return nil
		}
		return aggr.Func.GetArgs()
	}
}

func (a *Aggregator) planOffsetsNotPushed(ctx *plancontext.PlanningContext) {
	a.Source = newAliasedProjection(a.Source)
	// we need to keep things in the column order, so we can't iterate over the aggregations or groupings
	for colIdx := range a.Columns {
		idx := a.addIfGroupingColumn(ctx, colIdx)
		if idx >= 0 {
			continue
		}

		idx = a.addIfAggregationColumn(ctx, colIdx)

		if idx < 0 {
			panic(vterrors.VT13001("failed to find the corresponding column"))
		}
	}

	a.pushRemainingGroupingColumnsAndWeightStrings(ctx)
}

func (a *Aggregator) addIfAggregationColumn(ctx *plancontext.PlanningContext, colIdx int) int {
	for _, aggr := range a.Aggregations {
		if aggr.ColOffset != colIdx {
			continue
		}

		wrap := aeWrap(aggr.getPushColumn())
		offset := a.Source.AddColumn(ctx, false, false, wrap)
		if aggr.ColOffset != offset {
			panic(errFailedToPlan(aggr.Original))
		}

		return offset
	}
	return -1
}

func errFailedToPlan(original *sqlparser.AliasedExpr) *vterrors.VitessError {
	return vterrors.VT12001(fmt.Sprintf("failed to plan aggregation on: %s", sqlparser.String(original)))
}

func (a *Aggregator) addIfGroupingColumn(ctx *plancontext.PlanningContext, colIdx int) int {
	for _, gb := range a.Grouping {
		if gb.ColOffset != colIdx {
			continue
		}

		expr := a.Columns[colIdx]
		offset := a.Source.AddColumn(ctx, false, true, expr)
		if gb.ColOffset != offset {
			panic(errFailedToPlan(expr))
		}

		return offset
	}
	return -1
}

// pushRemainingGroupingColumnsAndWeightStrings pushes any grouping column that is not part of the columns list and weight strings needed for performing grouping aggregations.
func (a *Aggregator) pushRemainingGroupingColumnsAndWeightStrings(ctx *plancontext.PlanningContext) {
	for idx, gb := range a.Grouping {
		if gb.ColOffset == -1 {
			offset := a.internalAddColumn(ctx, aeWrap(gb.Inner), false)
			a.Grouping[idx].ColOffset = offset
		}

		if gb.WSOffset != -1 || !ctx.NeedsWeightString(gb.Inner) {
			continue
		}

		offset := a.internalAddWSColumn(ctx, a.Grouping[idx].ColOffset, aeWrap(weightStringFor(gb.Inner)))
		a.Grouping[idx].WSOffset = offset
	}
	for idx, aggr := range a.Aggregations {
		if aggr.WSOffset != -1 || !aggr.NeedsWeightString(ctx) {
			continue
		}

		arg := aggr.getPushColumn()
		offset := a.internalAddWSColumn(ctx, aggr.ColOffset, aeWrap(weightStringFor(arg)))

		a.Aggregations[idx].WSOffset = offset
	}
}

func (a *Aggregator) internalAddWSColumn(ctx *plancontext.PlanningContext, inOffset int, aliasedExpr *sqlparser.AliasedExpr) int {
	if a.ResultColumns == 0 && a.Truncate {
		// if we need to use `internalAddColumn`, it means we are adding columns that are not part of the original list,
		// so we need to set the ResultColumns to the current length of the columns list
		a.ResultColumns = len(a.Columns)
	}

	offset := a.Source.AddWSColumn(ctx, inOffset, false)

	if offset == len(a.Columns) {
		// if we get an offset at the end of our current column list, it means we added a new column
		a.Columns = append(a.Columns, aliasedExpr)
	}
	return offset
}

func (a *Aggregator) setTruncateColumnCount(offset int) {
	a.ResultColumns = offset
}

func (a *Aggregator) getTruncateColumnCount() int {
	return a.ResultColumns
}

func (a *Aggregator) internalAddColumn(ctx *plancontext.PlanningContext, aliasedExpr *sqlparser.AliasedExpr, addToGroupBy bool) int {
	if a.ResultColumns == 0 && a.Truncate {
		// if we need to use `internalAddColumn`, it means we are adding columns that are not part of the original list,
		// so we need to set the ResultColumns to the current length of the columns list
		a.ResultColumns = len(a.Columns)
	}
	offset := a.Source.AddColumn(ctx, true, addToGroupBy, aliasedExpr)

	if offset == len(a.Columns) {
		// if we get an offset at the end of our current column list, it means we added a new column
		a.Columns = append(a.Columns, aliasedExpr)
	}
	return offset
}

// SplitAggregatorBelowOperators returns the aggregator that will live under the Route.
// This is used when we are splitting the aggregation so one part is done
// at the mysql level and one part at the vtgate level
func (a *Aggregator) SplitAggregatorBelowOperators(ctx *plancontext.PlanningContext, input []Operator) *Aggregator {
	newOp := a.Clone(input).(*Aggregator)
	newOp.Pushed = false
	newOp.Original = false
	newOp.DT = nil

	// We need to make sure that the columns are cloned so that the original operator is not affected
	// by the changes we make to the new operator
	newOp.Columns = slice.Map(a.Columns, func(from *sqlparser.AliasedExpr) *sqlparser.AliasedExpr {
		return ctx.SemTable.Clone(from).(*sqlparser.AliasedExpr)
	})
	for idx, aggr := range newOp.Aggregations {
		newOp.Aggregations[idx].Original = ctx.SemTable.Clone(aggr.Original).(*sqlparser.AliasedExpr)
	}
	for idx, gb := range newOp.Grouping {
		newOp.Grouping[idx].Inner = ctx.SemTable.Clone(gb.Inner).(sqlparser.Expr)
	}
	return newOp
}

func (a *Aggregator) introducesTableID() semantics.TableSet {
	return a.DT.introducesTableID()
}

func (a *Aggregator) checkForInvalidAggregations() {
	for _, aggr := range a.Aggregations {
		_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			aggrFunc, isAggregate := node.(sqlparser.AggrFunc)
			if !isAggregate {
				return true, nil
			}
			args := aggrFunc.GetArgs()
			if args != nil && len(args) != 1 {
				panic(vterrors.VT03001(sqlparser.String(node)))
			}
			return true, nil

		}, aggr.Original.Expr)
	}
}

var _ Operator = (*Aggregator)(nil)
