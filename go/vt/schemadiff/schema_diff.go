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

package schemadiff

import (
	"context"
	"fmt"
	"sort"

	"vitess.io/vitess/go/mathutil"
)

type DiffDependencyType int

// diff dependencies in increasing restriction severity
const (
	DiffDependencyNone DiffDependencyType = iota // not a dependency
	DiffDependencyOrderUnknown
	DiffDependencyInOrderCompletion
	DiffDependencySequentialExecution
)

// DiffDependency indicates a dependency between two diffs, and the type of that dependency
type DiffDependency struct {
	diff          EntityDiff
	dependentDiff EntityDiff // depends on the above diff
	typ           DiffDependencyType
}

// NewDiffDependency returns a new diff dependency pairing.
func NewDiffDependency(diff EntityDiff, dependentDiff EntityDiff, typ DiffDependencyType) *DiffDependency {
	return &DiffDependency{
		diff:          diff,
		dependentDiff: dependentDiff,
		typ:           typ,
	}
}

func (d *DiffDependency) hashKey() string {
	return d.diff.CanonicalStatementString() + "/" + d.dependentDiff.CanonicalStatementString()
}

// Diff returns the "benefactor" diff, on which DependentDiff() depends on, ie, should run 1st.
func (d *DiffDependency) Diff() EntityDiff {
	return d.diff
}

// DependentDiff returns the diff that depends on the "benefactor" diff, ie must run 2nd
func (d *DiffDependency) DependentDiff() EntityDiff {
	return d.dependentDiff
}

// Type returns the dependency type. Types are numeric and comparable: the higher the value, the
// stricter, or more constrained, the dependency is.
func (d *DiffDependency) Type() DiffDependencyType {
	return d.typ
}

// IsInOrder returns true if this dependency indicates a known order
func (d *DiffDependency) IsInOrder() bool {
	return d.typ >= DiffDependencyInOrderCompletion
}

// IsSequential returns true if this is a sequential dependency
func (d *DiffDependency) IsSequential() bool {
	return d.typ >= DiffDependencySequentialExecution
}

/*
The below is adapted from https://yourbasic.org/golang/generate-permutation-slice-string/
Licensed under https://creativecommons.org/licenses/by/3.0/
Modified to have an early break
*/

// permutateDiffs calls `callback` with each permutation of a. If the function returns `true`, that means
// the callback has returned `true` for an early break, thus possibly not all permutations have been evaluated.
// callback's `errorIndex` indicates the first index at which the permutation has error, or -1 if there's no such error.
// When `errorIndex` is non negative, then the algorithm skips any further recursive dives following `i`.
func permutateDiffs(
	ctx context.Context,
	diffs []EntityDiff,
	hints *DiffHints,
	callback func([]EntityDiff, *DiffHints) (earlyBreak bool, errorIndex int),
) (earlyBreak bool, err error) {
	if len(diffs) == 0 {
		return false, nil
	}
	// Sort by a heuristic (DROPs first, ALTERs next, CREATEs last). This ordering is then used first in the permutation
	// search and serves as seed for the rest of permutations.

	earlyBreak, _, err = permDiff(ctx, diffs, hints, callback, 0)
	return earlyBreak, err
}

// permDiff is a recursive function to permutate given `a` and call `callback` for each permutation.
// If `callback` returns `true`, then so does this function, and this indicates a request for an early
// break, in which case this function will not be called again.
func permDiff(
	ctx context.Context,
	a []EntityDiff,
	hints *DiffHints,
	callback func([]EntityDiff, *DiffHints) (earlyBreak bool, errorIndex int),
	i int,
) (earlyBreak bool, errorIndex int, err error) {
	if err := ctx.Err(); err != nil {
		return true, -1, err // early break (due to context)
	}
	if i > len(a) {
		earlyBreak, errorIndex := callback(a, hints)
		return earlyBreak, errorIndex, nil
	}
	earlyBreak, errorIndex, err = permDiff(ctx, a, hints, callback, i+1)
	if errorIndex >= 0 && i > errorIndex {
		// Means the current permutation failed at `errorIndex`, and we're beyond that point. There's no
		// point in continuing to permutate the rest of the array.
		return false, errorIndex, err
	}
	if earlyBreak {
		// Found a valid permutation, no need to continue
		return true, -1, err
	}
	for j := i + 1; j < len(a); j++ {
		// An optimization: we don't really need all possible permutations. We can skip some of the recursive search.
		// We know we begin with a heuristic order where DROP VIEW comes first, then DROP TABLE, then ALTER TABLE & VIEW,
		// then CREATE TABLE, then CREATE VIEW. And the entities in that initial order are sorted by dependency. That's
		// thank's to Schema's UnorderedDiffs() existing heuristic.
		// Now, some pairs of statements should be permutated, but some others will have absolutely no advantage to permutate.
		// For example, a DROP VIEW and CREATE VIEW: there's no advantage to permutate the two. If the initial order is
		// inapplicable, then so will be the permutated order.
		// The next section identifies some no-brainers conditions for skipping swapping of elements.
		// There could be even more fine grained scenarios, which we can deal with in the future.
		iIsCreateDropView := false
		iIsTable := false
		switch a[i].(type) {
		case *DropViewEntityDiff, *CreateViewEntityDiff:
			iIsCreateDropView = true
		case *DropTableEntityDiff, *AlterTableEntityDiff, *CreateTableEntityDiff:
			iIsTable = true
		}

		jIsCreateDropView := false
		jIsTable := false
		switch a[j].(type) {
		case *DropViewEntityDiff, *CreateViewEntityDiff:
			jIsCreateDropView = true
		case *DropTableEntityDiff, *AlterTableEntityDiff, *CreateTableEntityDiff:
			jIsTable = true
		}

		if iIsCreateDropView && jIsCreateDropView {
			continue
		}
		if iIsCreateDropView && jIsTable {
			continue
		}
		if iIsTable && jIsCreateDropView {
			continue
		}
		// End of optimization
		a[i], a[j] = a[j], a[i]
		earlyBreak, errorIndex, err = permDiff(ctx, a, hints, callback, i+1)
		if errorIndex >= 0 && i > errorIndex {
			// Means the current permutation failed at `errorIndex`, and we're beyond that point. There's no
			// point in continuing to permutate the rest of the array.
			return false, errorIndex, err
		}
		if earlyBreak {
			return true, -1, err
		}
		a[i], a[j] = a[j], a[i]
	}
	return false, -1, nil
}

// SchemaDiff is a rich diff between two schemas. It includes the following:
// - The source schema (on which the diff would operate)
// - A list of SQL diffs (e.g. CREATE VIEW, ALTER TABLE, ...)
// - A map of dependencies between the diffs
// Operations on SchemaDiff are not concurrency-safe.
type SchemaDiff struct {
	schema *Schema
	from   *Schema
	hints  *DiffHints
	diffs  []EntityDiff

	diffMap      map[string]EntityDiff // key is diff's CanonicalStatementString()
	dependencies map[string]*DiffDependency

	r *mathutil.EquivalenceRelation // internal structure to help determine diffs
}

func NewSchemaDiff(schema *Schema, to *Schema, hints *DiffHints) *SchemaDiff {
	return &SchemaDiff{
		schema:       schema,
		from:         to,
		hints:        hints,
		dependencies: make(map[string]*DiffDependency),
		diffMap:      make(map[string]EntityDiff),
		r:            mathutil.NewEquivalenceRelation(),
	}
}

// loadDiffs loads a list of diffs, as generated by Schema.Diff(other) function. It explodes all subsequent diffs
// into distinct diffs (which then have no subsequent diffs). Thus, the list of diffs loaded can be longer than the
// list of diffs received.
func (d *SchemaDiff) loadDiffs(diffs []EntityDiff) {
	for _, diff := range diffs {
		allSubsequent := AllSubsequent(diff)
		for i, sdiff := range allSubsequent {
			d.diffs = append(d.diffs, sdiff)
			d.diffMap[sdiff.CanonicalStatementString()] = sdiff
			if i > 0 {
				// So this is a 2nd, 3rd etc. diff operating on same table
				// Two migrations on same entity (table in our case) must run sequentially.
				d.addDep(sdiff, allSubsequent[0], DiffDependencySequentialExecution)
			}
			d.r.Add(sdiff.CanonicalStatementString())
			// since we've exploded the subsequent diffs, we now clear any subsequent diffs
			// so that they do not auto-Apply() when we compute a valid path.
			sdiff.SetSubsequentDiff(nil)
		}
	}
}

// addDep adds a dependency: `dependentDiff` depends on `diff`, with given `depType`. If there's an
// already existing dependency between the two diffs, then we compare the dependency type; if the new
// type has a higher order (ie stricter) then we replace the existing dependency with the new one.
func (d *SchemaDiff) addDep(diff EntityDiff, dependentDiff EntityDiff, typ DiffDependencyType) *DiffDependency {
	_, _ = d.r.Relate(diff.CanonicalStatementString(), dependentDiff.CanonicalStatementString())
	diffDep := NewDiffDependency(diff, dependentDiff, typ)
	if existingDep, ok := d.dependencies[diffDep.hashKey()]; ok {
		if existingDep.typ >= diffDep.typ {
			// nothing new here, the new dependency is weaker or equals to an existing dependency
			return existingDep
		}
	}
	// Either the dep wasn't found, or we've just introduced a dep with a more severe type
	d.dependencies[diffDep.hashKey()] = diffDep
	return diffDep
}

// diffByStatementString is a utility function that returns a diff by its canonical statement string
func (d *SchemaDiff) diffByStatementString(s string) (EntityDiff, bool) {
	diff, ok := d.diffMap[s]
	return diff, ok
}

// diffsByEntityName returns all diffs that apply to a given entity (table/view)
func (d *SchemaDiff) diffsByEntityName(name string) (diffs []EntityDiff) {
	for _, diff := range d.diffs {
		if diff.EntityName() == name {
			diffs = append(diffs, diff)
		}
	}
	return diffs
}

// Empty returns 'true' when there are no diff entries
func (d *SchemaDiff) Empty() bool {
	return len(d.diffs) == 0
}

// UnorderedDiffs returns all the diffs. These are not sorted by dependencies. These are basically
// the original diffs, but "flattening" any subsequent diffs they may have. as result:
// - Diffs in the returned slice have no subsequent diffs
// - The returned slice may be longer than the number of diffs supplied by loadDiffs()
func (d *SchemaDiff) UnorderedDiffs() []EntityDiff {
	return d.diffs
}

// AllDependencies returns all known dependencies
func (d *SchemaDiff) AllDependencies() (deps []*DiffDependency) {
	for _, dep := range d.dependencies {
		deps = append(deps, dep)
	}
	return deps
}

// HasDependencies returns `true` if there is at least one known diff dependency.
// If this function returns `false` then that means there is no restriction whatsoever to the order of diffs.
func (d *SchemaDiff) HasDependencies() bool {
	return len(d.dependencies) > 0
}

// AllSequentialExecutionDependencies returns all diffs that are of "sequential execution" type.
func (d *SchemaDiff) AllSequentialExecutionDependencies() (deps []*DiffDependency) {
	for _, dep := range d.dependencies {
		if dep.typ >= DiffDependencySequentialExecution {
			deps = append(deps, dep)
		}
	}
	return deps
}

// HasSequentialExecutionDependencies return `true` if there is at least one "subsequential execution" type diff.
// If not, that means all diffs can be applied in parallel.
func (d *SchemaDiff) HasSequentialExecutionDependencies() bool {
	for _, dep := range d.dependencies {
		if dep.typ >= DiffDependencySequentialExecution {
			return true
		}
	}
	return false
}

// OrderedDiffs returns the list of diff in applicable order, if possible. This is a linearized representation
// where diffs may be applied in-order one after another, keeping the schema in valid state at all times.
func (d *SchemaDiff) OrderedDiffs(ctx context.Context) ([]EntityDiff, error) {
	lastGoodSchema := d.schema.copy()
	var orderedDiffs []EntityDiff
	m := d.r.Map()

	unorderedDiffsMap := map[string]int{}
	for i, diff := range d.UnorderedDiffs() {
		unorderedDiffsMap[diff.CanonicalStatementString()] = i
	}
	// The order of classes in the equivalence relation is, generally speaking, loyal to the order of original diffs.
	for _, class := range d.r.OrderedClasses() {
		classDiffs := []EntityDiff{}
		// Which diffs are in this equivalence class?
		for _, statementString := range m[class] {
			diff, ok := d.diffByStatementString(statementString)
			if !ok {
				return nil, fmt.Errorf("unexpected error: cannot find diff: %v", statementString)
			}
			classDiffs = append(classDiffs, diff)
		}
		sort.SliceStable(classDiffs, func(i, j int) bool {
			return unorderedDiffsMap[classDiffs[i].CanonicalStatementString()] < unorderedDiffsMap[classDiffs[j].CanonicalStatementString()]
		})

		// We will now permutate the diffs in this equivalence class, and hopefully find
		// a valid permutation (one where if we apply the diffs in-order, the schema remains valid throughout the process)
		tryPermutateDiffs := func(hints *DiffHints) (bool, error) {
			return permutateDiffs(ctx, classDiffs, hints, func(permutatedDiffs []EntityDiff, hints *DiffHints) (bool, int) {
				permutationSchema := lastGoodSchema.copy()
				// We want to apply the changes one by one, and validate the schema after each change
				for i := range permutatedDiffs {
					// apply inline
					applyHints := hints
					if hints.ForeignKeyCheckStrategy == ForeignKeyCheckStrategyCreateTableFirst {
						// This is a strategy that handles foreign key loops in a heuristic way.
						// It means: we allow for the very first change to be a CREATE TABLE, and ignore
						// any dependencies that CREATE TABLE may have. But then, we require the rest of
						// changes to have a strict order.
						overrideHints := *hints
						overrideHints.ForeignKeyCheckStrategy = ForeignKeyCheckStrategyStrict
						if i == 0 {
							if _, ok := permutatedDiffs[i].(*CreateTableEntityDiff); ok {
								overrideHints.ForeignKeyCheckStrategy = ForeignKeyCheckStrategyIgnore
							}
						}
						applyHints = &overrideHints
					}
					if err := permutationSchema.apply(permutatedDiffs[i:i+1], applyHints); err != nil {
						// permutation is invalid
						return false, i // let the algorithm know there's no point in pursuing any path after `i`
					}
				}

				// Good news, we managed to apply all of the permutations!
				orderedDiffs = append(orderedDiffs, permutatedDiffs...)
				lastGoodSchema = permutationSchema
				return true, -1 // early break! No need to keep searching
			})
		}
		// We prefer stricter strategy, because that gives best chance of finding a valid path.
		// So on best effort basis, we try to find a valid path starting with the strictest strategy, easing
		// to more relaxed ones, but never below the preconfigured.
		// For example, if the preconfigured strategy is "strict", we will try "strict" and then stop.
		// If the preconfigured strategy is "create-table-first", we will try "strict", then "create-table-first", then stop.
		tryPermutateDiffsAcrossStrategies := func() (found bool, err error) {
			for _, fkStrategy := range []int{ForeignKeyCheckStrategyStrict, ForeignKeyCheckStrategyCreateTableFirst, ForeignKeyCheckStrategyIgnore} {
				hints := *d.hints
				hints.ForeignKeyCheckStrategy = fkStrategy
				found, err = tryPermutateDiffs(&hints)
				if fkStrategy == d.hints.ForeignKeyCheckStrategy {
					// end of the line.
					return found, err
				}
				if err != nil {
					// No luck with this strategy, let's try the next one.
					continue
				}
				if found {
					// Good news!
					return true, nil
				}
			}
			return found, err
		}
		foundValidPathForClass, err := tryPermutateDiffsAcrossStrategies()
		if err != nil {
			return nil, err
		}
		if !foundValidPathForClass {
			// In this equivalence class, there is no valid permutation. We cannot linearize the diffs.
			return nil, &ImpossibleApplyDiffOrderError{
				UnorderedDiffs:   d.UnorderedDiffs(),
				ConflictingDiffs: classDiffs,
			}
		}

		// Done taking care of this equivalence class.
	}
	if d.hints.ForeignKeyCheckStrategy != ForeignKeyCheckStrategyStrict {
		// We may have allowed invalid foreign key dependencies along the way. But we must then validate the final schema
		// to ensure that all foreign keys are valid.
		hints := *d.hints
		hints.ForeignKeyCheckStrategy = ForeignKeyCheckStrategyStrict
		if err := lastGoodSchema.normalize(&hints); err != nil {
			return nil, &ImpossibleApplyDiffOrderError{
				UnorderedDiffs:   d.UnorderedDiffs(),
				ConflictingDiffs: d.UnorderedDiffs(),
			}
		}
	}
	return orderedDiffs, nil
}

// InstantDDLCapability returns an overall summary of the ability of the diffs to run with ALGORITHM=INSTANT.
// It is a convenience method, whose logic anyone can reimplement.
func (d *SchemaDiff) InstantDDLCapability() InstantDDLCapability {
	// The general logic: we return "InstantDDLCapabilityPossible" if there is one or more diffs that is capable of
	// ALGORITHM=INSTANT, and zero or more diffs that are irrelevant, and no diffs that are impossible to run with
	// ALGORITHM=INSTANT.
	capability := InstantDDLCapabilityIrrelevant
	for _, diff := range d.UnorderedDiffs() {
		switch diff.InstantDDLCapability() {
		case InstantDDLCapabilityUnknown:
			return InstantDDLCapabilityUnknown // Early break
		case InstantDDLCapabilityImpossible:
			return InstantDDLCapabilityImpossible // Early break
		case InstantDDLCapabilityPossible:
			capability = InstantDDLCapabilityPossible
		case InstantDDLCapabilityIrrelevant:
			// do nothing
		}
	}
	return capability
}

// RelatedForeignKeyTables returns the foreign key tables, either parent or child, that are affected by the
// diffs in this schema diff, either explicitly (table is modified) or implicitly (table's parent or
// child is modified).
func (d *SchemaDiff) RelatedForeignKeyTables() (fkTables map[string]*CreateTableEntity) {
	fkTables = make(map[string]*CreateTableEntity)

	scanForAffectedFKs := func(entityName string) {
		for _, schema := range []*Schema{d.from, d.schema} {
			for _, child := range schema.fkParentToChildren[entityName] {
				fkTables[entityName] = schema.Table(entityName) // this is a parent
				fkTables[child.Name()] = child
			}
			for _, parent := range schema.fkChildToParents[entityName] {
				fkTables[entityName] = schema.Table(entityName) // this is a child
				fkTables[parent.Name()] = parent
			}
		}
	}
	for _, diff := range d.UnorderedDiffs() {
		switch diff := diff.(type) {
		case *CreateTableEntityDiff, *AlterTableEntityDiff, *DropTableEntityDiff:
			scanForAffectedFKs(diff.EntityName())
		}
	}
	return fkTables
}
