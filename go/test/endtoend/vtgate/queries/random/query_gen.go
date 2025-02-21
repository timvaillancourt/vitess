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

package random

import (
	"fmt"
	"math/rand/v2"
	"slices"

	"vitess.io/vitess/go/slice"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
)

// this file contains the structs and functions to generate random queries

// to test only a particular type of query, delete the corresponding testFailingQueries clause
// there should be a comment indicating the type of query being disabled
// if true then known failing query types are still generated by randomQuery()
const testFailingQueries = false

type (
	// selectGenerator generates select statements
	selectGenerator struct {
		genConfig    sqlparser.ExprGeneratorConfig
		maxTables    int
		maxAggrs     int
		maxGBs       int
		schemaTables []tableT
		sel          *sqlparser.Select
	}

	// queryGenerator generates queries, which can either be unions or select statements
	queryGenerator struct {
		stmt   sqlparser.TableStatement
		selGen *selectGenerator
	}

	column struct {
		name string
		// TODO: perhaps remove tableName and always pass columns through a tableT
		tableName string
		typ       string
	}

	tableT struct {
		// the tableT struct can be used to represent the schema of a table or a derived table
		// in the former case tableExpr will be a sqlparser.TableName, in the latter a sqlparser.DerivedTable
		// in order to create a query with a derived table, its AST form is retrieved from tableExpr
		// once the derived table is aliased, alias is updated
		tableExpr sqlparser.SimpleTableExpr
		alias     string
		cols      []column
	}
)

var _ sqlparser.ExprGenerator = (*tableT)(nil)
var _ sqlparser.ExprGenerator = (*column)(nil)
var _ sqlparser.QueryGenerator = (*selectGenerator)(nil)
var _ sqlparser.QueryGenerator = (*queryGenerator)(nil)

func newQueryGenerator(genConfig sqlparser.ExprGeneratorConfig, maxTables, maxAggrs, maxGBs int, schemaTables []tableT) *queryGenerator {
	return &queryGenerator{
		selGen: newSelectGenerator(genConfig, maxTables, maxAggrs, maxGBs, schemaTables),
	}
}

func newSelectGenerator(genConfig sqlparser.ExprGeneratorConfig, maxTables, maxAggrs, maxGBs int, schemaTables []tableT) *selectGenerator {
	if maxTables <= 0 {
		log.Fatalf("maxTables must be at least 1, currently %d\n", maxTables)
	}

	return &selectGenerator{
		genConfig:    genConfig,
		maxTables:    maxTables,
		maxAggrs:     maxAggrs,
		maxGBs:       maxGBs,
		schemaTables: schemaTables,
		sel:          &sqlparser.Select{},
	}
}

// getASTExpr returns the AST representation of a column
func (c *column) getASTExpr() sqlparser.Expr {
	return sqlparser.NewColNameWithQualifier(c.name, sqlparser.NewTableName(c.tableName))
}

// getName returns the alias if it is nonempty
// if the alias is nonempty and tableExpr is of type sqlparser.TableName,
// then getName returns Name from tableExpr
// otherwise getName returns an empty string
func (t *tableT) getName() string {
	if t.alias != "" {
		return t.alias
	} else if tName, ok := t.tableExpr.(sqlparser.TableName); ok {
		return sqlparser.String(tName.Name)
	}

	return ""
}

// setAlias sets the alias for t, as well as setting the tableName for all columns in cols
func (t *tableT) setAlias(newName string) {
	t.alias = newName
	for i := range t.cols {
		t.cols[i].tableName = newName
	}
}

// addColumns adds columns to t, and automatically assigns each column.tableName
// this makes it unnatural to modify tableName
func (t *tableT) addColumns(col ...column) {
	for i := range col {
		col[i].tableName = t.getName()
		t.cols = append(t.cols, col[i])
	}
}

func (t *tableT) clone() *tableT {
	return &tableT{
		tableExpr: sqlparser.Clone(t.tableExpr),
		alias:     t.alias,
		cols:      slices.Clone(t.cols),
	}
}

func (c *column) Generate(genConfig sqlparser.ExprGeneratorConfig) sqlparser.Expr {
	if c.typ == genConfig.Type || genConfig.Type == "" {
		return c.getASTExpr()
	}

	return nil
}

func (t *tableT) Generate(genConfig sqlparser.ExprGeneratorConfig) sqlparser.Expr {
	colsCopy := slices.Clone(t.cols)

	for len(colsCopy) > 0 {
		idx := rand.IntN(len(colsCopy))
		randCol := colsCopy[idx]
		if randCol.typ == genConfig.Type || genConfig.Type == "" {
			return randCol.getASTExpr()
		}

		// delete randCol from colsCopy
		colsCopy[idx] = colsCopy[len(colsCopy)-1]
		colsCopy = colsCopy[:len(colsCopy)-1]
	}

	return nil
}

// Generate generates a subquery based on sg
// TODO: currently unused; generate random expressions with union
func (sg *selectGenerator) Generate(genConfig sqlparser.ExprGeneratorConfig) sqlparser.Expr {
	var schemaTablesCopy []tableT
	for _, tbl := range sg.schemaTables {
		schemaTablesCopy = append(schemaTablesCopy, *tbl.clone())
	}

	newSG := newQueryGenerator(genConfig, sg.maxTables, sg.maxAggrs, sg.maxGBs, schemaTablesCopy)
	newSG.randomQuery()

	return &sqlparser.Subquery{Select: newSG.selGen.sel}
}

// Generate generates a subquery based on qg
func (qg *queryGenerator) Generate(genConfig sqlparser.ExprGeneratorConfig) sqlparser.Expr {
	var schemaTablesCopy []tableT
	for _, tbl := range qg.selGen.schemaTables {
		schemaTablesCopy = append(schemaTablesCopy, *tbl.clone())
	}

	newQG := newQueryGenerator(genConfig, qg.selGen.maxTables, qg.selGen.maxAggrs, qg.selGen.maxGBs, schemaTablesCopy)
	newQG.randomQuery()

	return &sqlparser.Subquery{Select: newQG.stmt}
}

func (sg *selectGenerator) IsQueryGenerator() {}
func (qg *queryGenerator) IsQueryGenerator()  {}

func (qg *queryGenerator) randomQuery() {
	if rand.IntN(10) < 1 && testFailingQueries {
		qg.createUnion()
	} else {
		qg.selGen.randomSelect()
		qg.stmt = qg.selGen.sel
	}
}

// createUnion creates a simple UNION or UNION ALL; no LIMIT or ORDER BY
func (qg *queryGenerator) createUnion() {
	union := &sqlparser.Union{}

	if rand.IntN(2) < 1 {
		union.Distinct = true
	}

	// specify between 1-4 columns
	qg.selGen.genConfig.NumCols = rand.IntN(4) + 1

	qg.randomQuery()
	union.Left = qg.stmt
	qg.randomQuery()
	union.Right = qg.stmt

	qg.stmt = union
}

func (sg *selectGenerator) randomSelect() {
	// make sure the random expressions can generally not contain aggregates; change appropriately
	sg.genConfig = sg.genConfig.CannotAggregateConfig()

	sg.sel = &sqlparser.Select{}
	sg.sel.SetComments(sqlparser.Comments{"/*vt+ PLANNER=Gen4 */"})

	// select distinct (fails with group by bigint)
	isDistinct := rand.IntN(2) < 1
	if isDistinct {
		sg.sel.MakeDistinct()
	}

	// create both tables and join at the same time since both occupy the from clause
	tables, isJoin := sg.createTablesAndJoin()

	// canAggregate determines if the query will have
	// aggregate columns, group by, and having
	canAggregate := rand.IntN(4) < 3

	var (
		grouping, aggregates []column
		newTable             tableT
	)
	// TODO: distinct makes vitess think there is grouping on aggregation columns
	if canAggregate {
		if testFailingQueries || !isDistinct {
			// group by
			if !sg.genConfig.SingleRow {
				grouping = sg.createGroupBy(tables)
			}
		}

		// having
		isHaving := rand.IntN(2) < 1
		// TODO: having creates a lot of results mismatched
		if isHaving && testFailingQueries {
			sg.createHavingPredicates(grouping)
		}

		// alias the grouping columns
		grouping = sg.aliasGroupingColumns(grouping)

		// aggregation columns
		aggregates = sg.createAggregations(tables)

		// add the grouping and aggregation to newTable
		newTable.addColumns(grouping...)
		newTable.addColumns(aggregates...)
	}

	// where
	sg.createWherePredicates(tables)

	// add random expression to select
	// TODO: random expressions cause a lot of failures
	isRandomExpr := rand.IntN(2) < 1 && testFailingQueries

	// TODO: selecting a random expression potentially with columns creates
	// TODO: only_full_group_by related errors in Vitess
	var exprGenerators []sqlparser.ExprGenerator
	if canAggregate && testFailingQueries {
		exprGenerators = slice.Map(tables, func(t tableT) sqlparser.ExprGenerator { return &t })
		// add scalar subqueries
		if rand.IntN(10) < 1 {
			exprGenerators = append(exprGenerators, sg)
		}
	}

	// make sure we have at least one select expression
	for isRandomExpr || len(sg.sel.SelectExprs.Exprs) == 0 {
		// TODO: if the random expression is an int literal,
		// TODO: and if the query is (potentially) an aggregate query,
		// TODO: then we must group by the random expression,
		// TODO: but we cannot do this for int literals,
		// TODO: so we loop until we get a non-int-literal random expression
		// TODO: this is necessary because grouping by the alias (crandom0) currently fails on vitess
		randomExpr := sg.getRandomExpr(exprGenerators...)
		literal, ok := randomExpr.(*sqlparser.Literal)
		isIntLiteral := ok && literal.Type == sqlparser.IntVal
		if isIntLiteral && canAggregate {
			continue
		}

		// TODO: select distinct [literal] fails
		sg.sel.Distinct = false

		// alias randomly
		col := sg.randomlyAlias(randomExpr, "crandom0")
		newTable.addColumns(col)

		// make sure to add the random expression to group by for only_full_group_by
		if canAggregate {
			sg.sel.AddGroupBy(randomExpr)
		}

		break
	}

	// can add both aggregate and grouping columns to order by
	// TODO: order fails with distinct and outer joins
	isOrdered := rand.IntN(2) < 1 && (!isDistinct || testFailingQueries) && (!isJoin || testFailingQueries)
	if isOrdered || (!canAggregate && sg.genConfig.SingleRow) /* TODO: might be redundant */ {
		sg.createOrderBy()
	}

	// only add a limit if there is an ordering
	// TODO: limit fails a lot
	isLimit := rand.IntN(2) < 1 && len(sg.sel.OrderBy) > 0 && testFailingQueries
	if isLimit || (!canAggregate && sg.genConfig.SingleRow) /* TODO: might be redundant */ {
		sg.createLimit()
	}

	// this makes sure the query generated has the correct number of columns (sg.selGen.genConfig.numCols)
	newTable = sg.matchNumCols(tables, newTable, canAggregate)

	// add new table to schemaTables
	newTable.tableExpr = sqlparser.NewDerivedTable(false, sg.sel)
	sg.schemaTables = append(sg.schemaTables, newTable)

	// derived tables (partially unsupported)
	if rand.IntN(10) < 1 {
		sg.randomSelect()
	}
}

func (sg *selectGenerator) createTablesAndJoin() ([]tableT, bool) {
	var tables []tableT
	// add at least one of original emp/dept tables
	tables = append(tables, sg.schemaTables[rand.IntN(2)])

	tables[0].setAlias("tbl0")
	sg.sel.From = append(sg.sel.From, newAliasedTable(tables[0], "tbl0"))

	numTables := rand.IntN(sg.maxTables)
	for i := 0; i < numTables; i++ {
		tables = append(tables, randomEl(sg.schemaTables))
		alias := fmt.Sprintf("tbl%d", i+1)
		sg.sel.From = append(sg.sel.From, newAliasedTable(tables[i+1], alias))
		tables[i+1].setAlias(alias)
	}

	// TODO: outer joins produce results mismatched
	isJoin := rand.IntN(2) < 1 && testFailingQueries
	if isJoin {
		// TODO: do nested joins
		newTable := randomEl(sg.schemaTables)
		alias := fmt.Sprintf("tbl%d", numTables+1)
		newTable.setAlias(alias)
		tables = append(tables, newTable)

		sg.createJoin(tables)
	}

	return tables, isJoin
}

// creates a left join (without the condition) between the last table in sel and newTable
// tables should have one more table than sel
func (sg *selectGenerator) createJoin(tables []tableT) {
	n := len(sg.sel.From)
	if len(tables) != n+1 {
		log.Fatalf("sel has %d tables and tables has %d tables", len(sg.sel.From), n)
	}

	joinPredicate := sqlparser.AndExpressions(sg.createJoinPredicates(tables)...)
	joinCondition := sqlparser.NewJoinCondition(joinPredicate, nil)
	newTable := newAliasedTable(tables[n], fmt.Sprintf("tbl%d", n))
	sg.sel.From[n-1] = sqlparser.NewJoinTableExpr(sg.sel.From[n-1], getRandomJoinType(), newTable, joinCondition)
}

// returns 1-3 random expressions based on the last two elements of tables
// tables should have at least two elements
func (sg *selectGenerator) createJoinPredicates(tables []tableT) []sqlparser.Expr {
	if len(tables) < 2 {
		log.Fatalf("tables has %d elements, needs at least 2", len(tables))
	}

	exprGenerators := []sqlparser.ExprGenerator{&tables[len(tables)-2], &tables[len(tables)-1]}
	// add scalar subqueries
	// TODO: subqueries fail
	if rand.IntN(10) < 1 && testFailingQueries {
		exprGenerators = append(exprGenerators, sg)
	}

	return sg.createRandomExprs(1, 3, exprGenerators...)
}

// returns the grouping columns as []column
func (sg *selectGenerator) createGroupBy(tables []tableT) (grouping []column) {
	if sg.maxGBs <= 0 {
		return
	}
	numGBs := rand.IntN(sg.maxGBs + 1)
	for i := 0; i < numGBs; i++ {
		tblIdx := rand.IntN(len(tables))
		col := randomEl(tables[tblIdx].cols)
		// TODO: grouping by a date column sometimes errors
		if col.typ == "date" && !testFailingQueries {
			continue
		}
		sg.sel.AddGroupBy(col.getASTExpr())

		// add to select
		if rand.IntN(2) < 1 {
			sg.sel.AddSelectExpr(newAliasedColumn(col, ""))
			grouping = append(grouping, col)
		}
	}

	return
}

// aliasGroupingColumns randomly aliases the grouping columns in the SelectExprs
func (sg *selectGenerator) aliasGroupingColumns(grouping []column) []column {
	if len(grouping) != len(sg.sel.SelectExprs.Exprs) {
		log.Fatalf("grouping (length: %d) and sg.sel.SelectExprs (length: %d) should have the same length at this point", len(grouping), len(sg.sel.SelectExprs.Exprs))
	}

	for i := range grouping {
		if rand.IntN(2) < 1 {
			if aliasedExpr, ok := sg.sel.SelectExprs.Exprs[i].(*sqlparser.AliasedExpr); ok {
				alias := fmt.Sprintf("cgroup%d", i)
				aliasedExpr.SetAlias(alias)
				grouping[i].name = alias
			}
		}
	}

	return grouping
}

// returns the aggregation columns as three types: *sqlparser.SelectExprs, []column
func (sg *selectGenerator) createAggregations(tables []tableT) (aggregates []column) {
	exprGenerators := slice.Map(tables, func(t tableT) sqlparser.ExprGenerator { return &t })
	// add scalar subqueries
	// TODO: subqueries fail
	if rand.IntN(10) < 1 && testFailingQueries {
		exprGenerators = append(exprGenerators, sg)
	}

	sg.genConfig = sg.genConfig.IsAggregateConfig()
	aggrExprs := sg.createRandomExprs(0, sg.maxAggrs, exprGenerators...)
	sg.genConfig = sg.genConfig.CannotAggregateConfig()

	for i, expr := range aggrExprs {
		col := sg.randomlyAlias(expr, fmt.Sprintf("caggr%d", i))
		aggregates = append(aggregates, col)
	}

	return
}

// orders on all grouping expressions and on random SelectExprs
func (sg *selectGenerator) createOrderBy() {
	// always order on grouping expressions
	if sg.sel.GroupBy != nil {
		for _, expr := range sg.sel.GroupBy.Exprs {
			sg.sel.OrderBy = append(sg.sel.OrderBy, sqlparser.NewOrder(expr, getRandomOrderDirection()))
		}
	}

	// randomly order on SelectExprs
	for _, selExpr := range sg.sel.SelectExprs.Exprs {
		if aliasedExpr, ok := selExpr.(*sqlparser.AliasedExpr); ok && rand.IntN(2) < 1 {
			literal, ok := aliasedExpr.Expr.(*sqlparser.Literal)
			isIntLiteral := ok && literal.Type == sqlparser.IntVal
			if isIntLiteral {
				continue
			}
			sg.sel.OrderBy = append(sg.sel.OrderBy, sqlparser.NewOrder(aliasedExpr.Expr, getRandomOrderDirection()))
		}
	}
}

// returns 0-2 random expressions based on tables
func (sg *selectGenerator) createWherePredicates(tables []tableT) {
	exprGenerators := slice.Map(tables, func(t tableT) sqlparser.ExprGenerator { return &t })
	// add scalar subqueries
	// TODO: subqueries fail
	if rand.IntN(10) < 1 && testFailingQueries {
		exprGenerators = append(exprGenerators, sg)
	}

	predicates := sg.createRandomExprs(0, 2, exprGenerators...)
	sg.sel.AddWhere(sqlparser.AndExpressions(predicates...))
}

// creates predicates for the having clause comparing a column to a random expression
func (sg *selectGenerator) createHavingPredicates(grouping []column) {
	exprGenerators := slice.Map(grouping, func(c column) sqlparser.ExprGenerator { return &c })
	// add scalar subqueries
	// TODO: subqueries fail
	if rand.IntN(10) < 1 && testFailingQueries {
		exprGenerators = append(exprGenerators, sg)
	}

	sg.genConfig = sg.genConfig.CanAggregateConfig()
	predicates := sg.createRandomExprs(0, 2, exprGenerators...)
	sg.genConfig = sg.genConfig.CannotAggregateConfig()

	sg.sel.AddHaving(sqlparser.AndExpressions(predicates...))
}

// returns between minExprs and maxExprs random expressions using generators
func (sg *selectGenerator) createRandomExprs(minExprs, maxExprs int, generators ...sqlparser.ExprGenerator) (predicates []sqlparser.Expr) {
	if minExprs > maxExprs {
		log.Fatalf("minExprs is greater than maxExprs; minExprs: %d, maxExprs: %d\n", minExprs, maxExprs)
	} else if maxExprs <= 0 {
		return
	}
	numPredicates := rand.IntN(maxExprs-minExprs+1) + minExprs
	for i := 0; i < numPredicates; i++ {
		predicates = append(predicates, sg.getRandomExpr(generators...))
	}

	return
}

// getRandomExpr returns a random expression
func (sg *selectGenerator) getRandomExpr(generators ...sqlparser.ExprGenerator) sqlparser.Expr {
	var g *sqlparser.Generator
	if generators == nil {
		g = sqlparser.NewGenerator(2)
	} else {
		g = sqlparser.NewGenerator(2, generators...)
	}

	return g.Expression(sg.genConfig.SingleRowConfig().SetNumCols(1))
}

// creates sel.Limit
func (sg *selectGenerator) createLimit() {
	if sg.genConfig.SingleRow {
		sg.sel.Limit = sqlparser.NewLimitWithoutOffset(1)
		return
	}

	limitNum := rand.IntN(10)
	if rand.IntN(2) < 1 {
		offset := rand.IntN(10)
		sg.sel.Limit = sqlparser.NewLimit(offset, limitNum)
	} else {
		sg.sel.Limit = sqlparser.NewLimitWithoutOffset(limitNum)
	}
}

// randomlyAlias randomly aliases expr with alias alias, adds it to sel.SelectExprs, and returns the column created
func (sg *selectGenerator) randomlyAlias(expr sqlparser.Expr, alias string) column {
	var col column
	if rand.IntN(2) < 1 {
		alias = ""
		col.name = sqlparser.String(expr)
	} else {
		col.name = alias
	}
	sg.sel.AddSelectExpr(sqlparser.NewAliasedExpr(expr, alias))

	return col
}

// matchNumCols makes sure sg.sel.SelectExprs and newTable both have the same number of cols: sg.genConfig.NumCols
func (sg *selectGenerator) matchNumCols(tables []tableT, newTable tableT, canAggregate bool) tableT {
	// remove SelectExprs and newTable.cols randomly until there are sg.genConfig.NumCols amount
	for len(sg.sel.SelectExprs.Exprs) > sg.genConfig.NumCols && sg.genConfig.NumCols > 0 {
		// select a random index and remove it from SelectExprs and newTable
		idx := rand.IntN(len(sg.sel.SelectExprs.Exprs))

		sg.sel.SelectExprs.Exprs[idx] = sg.sel.SelectExprs.Exprs[len(sg.sel.SelectExprs.Exprs)-1]
		sg.sel.SelectExprs.Exprs = sg.sel.SelectExprs.Exprs[:len(sg.sel.SelectExprs.Exprs)-1]

		newTable.cols[idx] = newTable.cols[len(newTable.cols)-1]
		newTable.cols = newTable.cols[:len(newTable.cols)-1]
	}

	// alternatively, add random expressions until there are sg.genConfig.NumCols amount
	if sg.genConfig.NumCols > len(sg.sel.SelectExprs.Exprs) {
		diff := sg.genConfig.NumCols - len(sg.sel.SelectExprs.Exprs)
		exprs := sg.createRandomExprs(diff, diff,
			slice.Map(tables, func(t tableT) sqlparser.ExprGenerator { return &t })...)

		for i, expr := range exprs {
			col := sg.randomlyAlias(expr, fmt.Sprintf("crandom%d", i+1))
			newTable.addColumns(col)

			if canAggregate {
				sg.sel.AddGroupBy(expr)
			}
		}
	}

	return newTable
}

func getRandomOrderDirection() sqlparser.OrderDirection {
	// asc, desc
	return randomEl([]sqlparser.OrderDirection{0, 1})
}

func getRandomJoinType() sqlparser.JoinType {
	// normal, straight, left, right, natural, natural left, natural right
	return randomEl([]sqlparser.JoinType{0, 1, 2, 3, 4, 5, 6})
}

func randomEl[K any](in []K) K {
	return in[rand.IntN(len(in))]
}

func newAliasedTable(tbl tableT, alias string) *sqlparser.AliasedTableExpr {
	return sqlparser.NewAliasedTableExpr(tbl.tableExpr, alias)
}

func newAliasedColumn(col column, alias string) *sqlparser.AliasedExpr {
	return sqlparser.NewAliasedExpr(col.getASTExpr(), alias)
}
