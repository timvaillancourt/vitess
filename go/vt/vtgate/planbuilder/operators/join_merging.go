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

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// mergeJoinInputs checks whether two operators can be merged into a single one.
// If they can be merged, a new operator with the merged routing is returned
// If they cannot be merged, nil is returned.
func (jm *joinMerger) mergeJoinInputs(ctx *plancontext.PlanningContext, lhs, rhs Operator) *Route {
	lhsRoute, rhsRoute, routingA, routingB, a, b, sameKeyspace := prepareInputRoutes(ctx, lhs, rhs)
	if lhsRoute == nil {
		debugNoRewrite("apply join merge blocked: LHS or RHS is not a Route")
		return nil
	}

	switch {
	// We clone the right hand side and try and push all the join predicates that are solved entirely by that side.
	// If a dual is on the left side, and it is a left join (all right joins are changed to left joins), then we can only merge if the right side is a single sharded routing.
	case a == dual:
		newRouting := rhsRoute.Routing.Clone()

		rhsID := TableID(rhsRoute)
		for _, predicate := range jm.predicates {
			if ctx.SemTable.DirectDeps(predicate).IsSolvedBy(rhsID) {
				newRouting = UpdateRoutingLogic(ctx, predicate, newRouting)
			}
		}

		if !(jm.joinType.IsInner() || newRouting.OpCode().IsSingleShard()) {
			debugNoRewrite("apply join merge blocked: dual routing with non-inner join type %s and multi-shard routing %s", jm.joinType.ToString(), newRouting.OpCode().String())
			return nil
		}
		return jm.merge(ctx, lhsRoute, rhsRoute, newRouting)

	// If a dual is on the right side.
	case b == dual:
		return jm.merge(ctx, lhsRoute, rhsRoute, routingA)

	// As both are reference route. We need to merge the alternates as well.
	case a == anyShard && b == anyShard && sameKeyspace:
		newrouting := mergeAnyShardRoutings(ctx, routingA.(*AnyShardRouting), routingB.(*AnyShardRouting), jm.predicates, jm.joinType)
		return jm.merge(ctx, lhsRoute, rhsRoute, newrouting)

	// an unsharded/reference route can be merged with anything going to that keyspace
	case a == anyShard && sameKeyspace:
		return jm.merge(ctx, lhsRoute, rhsRoute, routingB)
	case b == anyShard && sameKeyspace:
		return jm.merge(ctx, lhsRoute, rhsRoute, routingA)

	// None routing can always be merged, as long as we are aiming for the same keyspace
	case a == none && sameKeyspace:
		return jm.merge(ctx, lhsRoute, rhsRoute, routingA)
	case b == none && sameKeyspace:
		return jm.merge(ctx, lhsRoute, rhsRoute, routingB)

	// infoSchema routing is complex, so we handle it in a separate method
	case a == infoSchema && b == infoSchema:
		result := tryMergeInfoSchemaRoutings(ctx, routingA, routingB, jm, lhsRoute, rhsRoute)
		if result == nil {
			debugNoRewrite("apply join merge blocked: info schema routing merge failed")
		}
		return result

	// sharded routing is complex, so we handle it in a separate method
	case a == sharded && b == sharded:
		result := tryMergeShardedRouting(ctx, lhsRoute, rhsRoute, jm, jm.predicates)
		if result == nil {
			debugNoRewrite("apply join merge blocked: sharded routing merge failed (different keyspaces or incompatible vindex predicates)")
		}
		return result

	default:
		if !sameKeyspace {
			debugNoRewrite("apply join merge blocked: routes target different keyspaces (LHS: %s %s, RHS: %s %s)",
				a.String(), getKeyspaceName(routingA), b.String(), getKeyspaceName(routingB))
		} else {
			debugNoRewrite("apply join merge blocked: incompatible routing types (LHS: %s, RHS: %s)", a.String(), b.String())
		}
		return nil
	}
}

func mergeAnyShardRoutings(ctx *plancontext.PlanningContext, a, b *AnyShardRouting, joinPredicates []sqlparser.Expr, joinType sqlparser.JoinType) *AnyShardRouting {
	alternates := make(map[*vindexes.Keyspace]*Route)
	for ak, av := range a.Alternates {
		for bk, bv := range b.Alternates {
			// only same keyspace alternates can be merged.
			if ak != bk {
				continue
			}
			op, _ := mergeOrJoin(ctx, av, bv, joinPredicates, joinType)
			if r, ok := op.(*Route); ok {
				alternates[ak] = r
			}
		}
	}
	return &AnyShardRouting{
		keyspace:   a.keyspace,
		Alternates: alternates,
	}
}

func prepareInputRoutes(ctx *plancontext.PlanningContext, lhs Operator, rhs Operator) (*Route, *Route, Routing, Routing, routingType, routingType, bool) {
	lhsRoute, rhsRoute := operatorsToRoutes(lhs, rhs)
	if lhsRoute == nil || rhsRoute == nil {
		return nil, nil, nil, nil, 0, 0, false
	}

	lhsRoute, rhsRoute, routingA, routingB, sameKeyspace := getRoutesOrAlternates(ctx, lhsRoute, rhsRoute)

	a, b := getRoutingType(routingA), getRoutingType(routingB)
	return lhsRoute, rhsRoute, routingA, routingB, a, b, sameKeyspace
}

type (
	merger interface {
		mergeShardedRouting(ctx *plancontext.PlanningContext, r1, r2 *ShardedRouting, op1, op2 *Route, conditions ...engine.Condition) *Route
		merge(ctx *plancontext.PlanningContext, op1, op2 *Route, r Routing, conditions ...engine.Condition) *Route
	}

	joinMerger struct {
		predicates []sqlparser.Expr
		// joinType is permitted to store only 3 of the possible values
		// NormalJoinType, StraightJoinType and LeftJoinType.
		joinType sqlparser.JoinType
	}

	routingType int
)

const (
	sharded routingType = iota
	infoSchema
	anyShard
	none
	dual
	targeted
)

func (rt routingType) String() string {
	switch rt {
	case sharded:
		return "sharded"
	case infoSchema:
		return "infoSchema"
	case anyShard:
		return "anyShard"
	case none:
		return "none"
	case dual:
		return "dual"
	case targeted:
		return "targeted"
	}
	panic("switch should be exhaustive")
}

// getRoutesOrAlternates gets the Routings from each Route. If they are from different keyspaces,
// we check if this is a table with alternates in other keyspaces that we can use
func getRoutesOrAlternates(ctx *plancontext.PlanningContext, lhsRoute, rhsRoute *Route) (*Route, *Route, Routing, Routing, bool) {
	routingA := lhsRoute.Routing
	routingB := rhsRoute.Routing
	sameKeyspace := routingA.Keyspace() == routingB.Keyspace()

	if sameKeyspace ||
		// if either of these is missing a keyspace, we are not going to be able to find an alternative
		routingA.Keyspace() == nil ||
		routingB.Keyspace() == nil {
		return lhsRoute, rhsRoute, routingA, routingB, sameKeyspace
	}

	// If we have a reference route, we will try to find an alternate route in same keyspace as other routing keyspace.
	// If the reference route is part of DML table update target, alternate keyspace route cannot be considered.
	if refA, ok := routingA.(*AnyShardRouting); ok &&
		!TableID(lhsRoute).IsOverlapping(ctx.SemTable.DMLTargets) {
		if altARoute := refA.AlternateInKeyspace(routingB.Keyspace()); altARoute != nil {
			return altARoute, rhsRoute, altARoute.Routing, routingB, true
		}
	}

	if refB, ok := routingB.(*AnyShardRouting); ok &&
		!TableID(rhsRoute).IsOverlapping(ctx.SemTable.DMLTargets) {
		if altBRoute := refB.AlternateInKeyspace(routingA.Keyspace()); altBRoute != nil {
			return lhsRoute, altBRoute, routingA, altBRoute.Routing, true
		}
	}

	return lhsRoute, rhsRoute, routingA, routingB, sameKeyspace
}

func getRoutingType(r Routing) routingType {
	switch r.(type) {
	case *InfoSchemaRouting:
		return infoSchema
	case *AnyShardRouting:
		return anyShard
	case *DualRouting:
		return dual
	case *ShardedRouting:
		return sharded
	case *NoneRouting:
		return none
	case *TargetedRouting:
		return targeted
	}
	panic(fmt.Sprintf("switch should be exhaustive, got %T", r))
}

func newJoinMerge(predicates []sqlparser.Expr, joinType sqlparser.JoinType) *joinMerger {
	return &joinMerger{
		predicates: predicates,
		joinType:   joinType,
	}
}

func (jm *joinMerger) mergeShardedRouting(ctx *plancontext.PlanningContext, r1, r2 *ShardedRouting, op1, op2 *Route, conditions ...engine.Condition) *Route {
	return jm.merge(ctx, op1, op2, mergeShardedRouting(r1, r2), conditions...)
}

func mergeShardedRouting(r1 *ShardedRouting, r2 *ShardedRouting) *ShardedRouting {
	tr := &ShardedRouting{
		VindexPreds:    append(r1.VindexPreds, r2.VindexPreds...),
		keyspace:       r1.keyspace,
		RouteOpCode:    r1.RouteOpCode,
		SeenPredicates: append(r1.SeenPredicates, r2.SeenPredicates...),
	}
	if r1.SelectedVindex() == r2.SelectedVindex() {
		tr.Selected = r1.Selected
	} else {
		tr.PickBestAvailableVindex()
	}
	return tr
}

func getKeyspaceName(routing Routing) string {
	if ks := routing.Keyspace(); ks != nil {
		return ks.Name
	}
	return "<unknown>"
}

func (jm *joinMerger) merge(ctx *plancontext.PlanningContext, op1, op2 *Route, r Routing, conditions ...engine.Condition) *Route {
	aj := NewApplyJoin(ctx, op1.Source, op2.Source, ctx.SemTable.AndExpressions(jm.predicates...), jm.joinType, false)
	for _, column := range aj.JoinPredicates.columns {
		if column.JoinPredicateID != nil {
			ctx.PredTracker.Set(*column.JoinPredicateID, column.Original)
		}
	}
	return &Route{
		unaryOperator: newUnaryOp(aj),
		MergedWith:    []*Route{op2},
		Routing:       r,
		Conditions:    conditions,
	}
}
