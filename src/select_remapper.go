package main

import (
	"errors"
	"strings"

	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

var KNOWN_SET_STATEMENTS = NewSet([]string{
	"client_encoding",             // SET client_encoding TO 'UTF8'
	"client_min_messages",         // SET client_min_messages TO 'warning'
	"standard_conforming_strings", // SET standard_conforming_strings = on
	"intervalstyle",               // SET intervalstyle = iso_8601
	"timezone",                    // SET SESSION timezone TO 'UTC'
	"extra_float_digits",          // SET extra_float_digits = 3
	"application_name",            // SET application_name = 'psql'
	"datestyle",                   // SET datestyle TO 'ISO'
	"session characteristics",     // SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED
})

var FALLBACK_QUERY_TREE, _ = pgQuery.Parse(FALLBACK_SQL_QUERY)
var FALLBACK_SET_QUERY_TREE, _ = pgQuery.Parse("SET schema TO public")

type SelectRemapper struct {
	parserTable    *QueryParserTable
	parserType     *QueryParserType
	remapperTable  *SelectRemapperTable
	remapperWhere  *SelectRemapperWhere
	remapperSelect *SelectRemapperSelect
	icebergReader  *IcebergReader
	duckdb         *Duckdb
	config         *Config
}

func NewSelectRemapper(config *Config, icebergReader *IcebergReader, duckdb *Duckdb) *SelectRemapper {
	return &SelectRemapper{
		parserTable:    NewQueryParserTable(config),
		parserType:     NewQueryParserType(config),
		remapperTable:  NewSelectRemapperTable(config, icebergReader, duckdb),
		remapperWhere:  NewSelectRemapperWhere(config),
		remapperSelect: NewSelectRemapperSelect(config),
		icebergReader:  icebergReader,
		duckdb:         duckdb,
		config:         config,
	}
}

func (selectRemapper *SelectRemapper) RemapSelectStatements(statements []*pgQuery.RawStmt) ([]*pgQuery.RawStmt, error) {
	if len(statements) == 0 {
		return FALLBACK_QUERY_TREE.Stmts, nil
	}

	for i, stmt := range statements {
		node := stmt.Stmt

		switch {
		// Empty query
		case node == nil:
			return nil, errors.New("empty query")

		// SELECT ...
		case node.GetSelectStmt() != nil:
			remappedSelect := selectRemapper.remapSelectStatement(stmt.Stmt.GetSelectStmt(), 1)
			stmt.Stmt = &pgQuery.Node{Node: &pgQuery.Node_SelectStmt{SelectStmt: remappedSelect}}
			statements[i] = stmt

		// SET ...
		case node.GetVariableSetStmt() != nil:
			statements[i] = selectRemapper.remapSetStatement(stmt)

		// DISCARD ALL
		case node.GetDiscardStmt() != nil:
			statements[i] = FALLBACK_QUERY_TREE.Stmts[0]

		// SHOW ...
		case node.GetVariableShowStmt() != nil:
			if node.GetVariableShowStmt().Name == "search_path" {
				searchPathStmt, _ := pgQuery.Parse(`SELECT CONCAT('"$user", ', value) AS search_path FROM duckdb_settings() WHERE name = 'search_path'`)
				statements[i] = searchPathStmt.Stmts[0]
			} else {
				statements[i] = FALLBACK_QUERY_TREE.Stmts[0]
			}

		// Unsupported query
		default:
			LogDebug(selectRemapper.config, "Query tree:", stmt, node)
			return nil, errors.New("unsupported query type")
		}
	}

	return statements, nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// SET ... (no-op)
func (selectRemapper *SelectRemapper) remapSetStatement(stmt *pgQuery.RawStmt) *pgQuery.RawStmt {
	setStatement := stmt.Stmt.GetVariableSetStmt()

	if !KNOWN_SET_STATEMENTS.Contains(strings.ToLower(setStatement.Name)) {
		LogWarn(selectRemapper.config, "Unsupported SET ", setStatement.Name, ":", setStatement)
	}

	return FALLBACK_SET_QUERY_TREE.Stmts[0]
}

func (selectRemapper *SelectRemapper) remapSelectStatement(selectStatement *pgQuery.SelectStmt, indentLevel int) *pgQuery.SelectStmt {
	// Target Sublinks's
	for _, target := range selectStatement.TargetList {
		if subLink := target.GetResTarget().Val.GetSubLink(); subLink != nil {
			selectRemapper.traceTreeTraversal("Target SubLink", indentLevel)
			subSelect := subLink.Subselect.GetSelectStmt()
			selectRemapper.remapSelectStatement(subSelect, indentLevel+1) // self-recursion
		}
	}

	// CASE
	if hasCaseExpr := selectRemapper.hasCaseExpressions(selectStatement); hasCaseExpr {
		selectRemapper.traceTreeTraversal("CASE expressions", indentLevel)
		selectRemapper.remapCaseExpressions(selectStatement, indentLevel) // recursive
	}

	// UNION
	if selectStatement.FromClause == nil && selectStatement.Larg != nil && selectStatement.Rarg != nil {
		selectRemapper.traceTreeTraversal("UNION left", indentLevel)
		leftSelectStatement := selectStatement.Larg
		leftSelectStatement = selectRemapper.remapSelectStatement(leftSelectStatement, indentLevel+1) // self-recursion

		selectRemapper.traceTreeTraversal("UNION right", indentLevel)
		rightSelectStatement := selectStatement.Rarg
		rightSelectStatement = selectRemapper.remapSelectStatement(rightSelectStatement, indentLevel+1) // self-recursion
	}

	// JOIN
	if len(selectStatement.FromClause) > 0 && selectStatement.FromClause[0].GetJoinExpr() != nil {
		selectStatement.FromClause[0] = selectRemapper.remapJoinExpressions(selectStatement, selectStatement.FromClause[0], indentLevel+1) // recursive with self-recursion
	}

	// WHERE
	if selectStatement.WhereClause != nil {
		selectStatement.WhereClause = selectRemapper.remapTypeCastsInNode(selectStatement.WhereClause)
		selectStatement = selectRemapper.remapperWhere.RemapWhereExpressions(selectStatement, selectStatement.WhereClause, indentLevel)
	}

	// WITH
	if selectStatement.WithClause != nil {
		selectRemapper.traceTreeTraversal("WITH CTE's", indentLevel)
		for _, cte := range selectStatement.WithClause.Ctes {
			if cteSelect := cte.GetCommonTableExpr().Ctequery.GetSelectStmt(); cteSelect != nil {
				selectRemapper.remapSelectStatement(cteSelect, indentLevel+1) // self-recursion
			}
		}
	}

	// FROM
	if len(selectStatement.FromClause) > 0 {
		for i, fromNode := range selectStatement.FromClause {
			if fromNode.GetRangeVar() != nil {
				selectRemapper.traceTreeTraversal("WHERE statements", indentLevel)
				// FROM [TABLE]
				selectRemapper.traceTreeTraversal("FROM table", indentLevel)
				selectStatement.FromClause[i] = selectRemapper.remapperTable.RemapTable(fromNode)
				qSchemaTable := selectRemapper.parserTable.NodeToQuerySchemaTable(fromNode)
				selectStatement = selectRemapper.remapperTable.RemapWhereClauseForTable(qSchemaTable, selectStatement)
			} else if fromNode.GetRangeSubselect() != nil {
				// FROM (SELECT ...)
				selectRemapper.traceTreeTraversal("FROM subselect", indentLevel)
				subSelectStatement := fromNode.GetRangeSubselect().Subquery.GetSelectStmt()
				subSelectStatement = selectRemapper.remapSelectStatement(subSelectStatement, indentLevel+1) // self-recursion
			} else if fromNode.GetRangeFunction() != nil {
				// FROM PG_FUNCTION()
				selectStatement.FromClause[i] = selectRemapper.remapTableFunction(fromNode, indentLevel) // recursive
			}
		}
	}

	selectStatement = selectRemapper.remapSelect(selectStatement, indentLevel) // recursive
	return selectStatement
}

func (selectRemapper *SelectRemapper) hasCaseExpressions(selectStatement *pgQuery.SelectStmt) bool {
	for _, target := range selectStatement.TargetList {
		if target.GetResTarget().Val.GetCaseExpr() != nil {
			return true
		}
	}
	return false
}

func (selectRemapper *SelectRemapper) remapCaseExpressions(selectStatement *pgQuery.SelectStmt, indentLevel int) *pgQuery.SelectStmt {
	for _, target := range selectStatement.TargetList {
		if caseExpr := target.GetResTarget().Val.GetCaseExpr(); caseExpr != nil {

			selectRemapper.ensureConsistentCaseTypes(caseExpr)

			for _, when := range caseExpr.Args {
				if whenClause := when.GetCaseWhen(); whenClause != nil {
					if whenClause.Expr != nil {
						if aExpr := whenClause.Expr.GetAExpr(); aExpr != nil {
							if subLink := aExpr.Lexpr.GetSubLink(); subLink != nil {
								selectRemapper.traceTreeTraversal("CASE WHEN left", indentLevel+1)
								subSelect := subLink.Subselect.GetSelectStmt()
								subSelect = selectRemapper.remapSelectStatement(subSelect, indentLevel+1)
							}
							if subLink := aExpr.Rexpr.GetSubLink(); subLink != nil {
								selectRemapper.traceTreeTraversal("CASE WHEN right", indentLevel+1)
								subSelect := subLink.Subselect.GetSelectStmt()
								subSelect = selectRemapper.remapSelectStatement(subSelect, indentLevel+1)
							}
						}
					}

					if whenClause.Result != nil {
						if subLink := whenClause.Result.GetSubLink(); subLink != nil {
							selectRemapper.traceTreeTraversal("CASE THEN", indentLevel+1)
							subSelect := subLink.Subselect.GetSelectStmt()
							subSelect = selectRemapper.remapSelectStatement(subSelect, indentLevel+1)
						}
						if funcCall := whenClause.Result.GetFuncCall(); funcCall != nil {
							selectRemapper.traceTreeTraversal("CASE THEN function", indentLevel+1)
							whenClause.Result = selectRemapper.remapperSelect.remappedToConstant(funcCall)
						}
					}
				}
			}

			if caseExpr.Defresult != nil {
				if subLink := caseExpr.Defresult.GetSubLink(); subLink != nil {
					selectRemapper.traceTreeTraversal("CASE ELSE", indentLevel+1)
					subSelect := subLink.Subselect.GetSelectStmt()
					subSelect = selectRemapper.remapSelectStatement(subSelect, indentLevel+1)
				}
				if funcCall := caseExpr.Defresult.GetFuncCall(); funcCall != nil {
					selectRemapper.traceTreeTraversal("CASE ELSE function", indentLevel+1)
					caseExpr.Defresult = selectRemapper.remapperSelect.remappedToConstant(funcCall)
				}
			}
		}
	}
	return selectStatement
}

func (selectRemapper *SelectRemapper) ensureConsistentCaseTypes(caseExpr *pgQuery.CaseExpr) {
	if len(caseExpr.Args) > 0 {
		if when := caseExpr.Args[0].GetCaseWhen(); when != nil && when.Result != nil {
			if typeName := selectRemapper.parserType.inferNodeType(when.Result); typeName != "" {
				// WHEN
				for i := 1; i < len(caseExpr.Args); i++ {
					if whenClause := caseExpr.Args[i].GetCaseWhen(); whenClause != nil && whenClause.Result != nil {
						whenClause.Result = selectRemapper.parserType.MakeCaseTypeCastNode(whenClause.Result, typeName)
					}
				}
				// ELSE
				if caseExpr.Defresult != nil {
					caseExpr.Defresult = selectRemapper.parserType.MakeCaseTypeCastNode(caseExpr.Defresult, typeName)
				}
			}
		}
	}
}

// FROM PG_FUNCTION()
func (selectRemapper *SelectRemapper) remapTableFunction(fromNode *pgQuery.Node, indentLevel int) *pgQuery.Node {
	selectRemapper.traceTreeTraversal("FROM function()", indentLevel)

	fromNode = selectRemapper.remapperTable.RemapTableFunction(fromNode)
	if fromNode.GetRangeFunction() == nil {
		return fromNode
	}

	for _, funcNode := range fromNode.GetRangeFunction().Functions {
		for _, funcItemNode := range funcNode.GetList().Items {
			funcCallNode := funcItemNode.GetFuncCall()
			if funcCallNode == nil {
				continue
			}
			funcCallNode = selectRemapper.remapTableFunctionArgs(funcCallNode, indentLevel+1) // recursive
		}
	}

	return fromNode
}

// FROM PG_FUNCTION(PG_NESTED_FUNCTION())
func (selectRemapper *SelectRemapper) remapTableFunctionArgs(funcCallNode *pgQuery.FuncCall, indentLevel int) *pgQuery.FuncCall {
	selectRemapper.traceTreeTraversal("FROM nested_function()", indentLevel)

	for i, argNode := range funcCallNode.GetArgs() {
		nestedFunctionCall := argNode.GetFuncCall()
		if nestedFunctionCall == nil {
			continue
		}

		nestedFunctionCall = selectRemapper.remapperTable.RemapNestedTableFunction(nestedFunctionCall)
		nestedFunctionCall = selectRemapper.remapTableFunctionArgs(nestedFunctionCall, indentLevel+1) // recursive

		funcCallNode.Args[i].Node = &pgQuery.Node_FuncCall{FuncCall: nestedFunctionCall}
	}

	return funcCallNode
}

func (selectRemapper *SelectRemapper) remapTypeCastsInNode(node *pgQuery.Node) *pgQuery.Node {
	if node == nil {
		return nil
	}

	// Direct typecast
	if node.GetTypeCast() != nil {
		return selectRemapper.remapTypecast(node)
	}

	// Handle CASE expressions
	if node.GetCaseExpr() != nil {
		caseExpr := node.GetCaseExpr()
		// Handle WHEN clauses
		for i, when := range caseExpr.Args {
			whenClause := when.GetCaseWhen()
			if whenClause.Result != nil {
				whenClause.Result = selectRemapper.remapTypeCastsInNode(whenClause.Result) // self-recursion
			}
			caseExpr.Args[i] = when
		}
		// Handle ELSE clause
		if caseExpr.Defresult != nil {
			caseExpr.Defresult = selectRemapper.remapTypeCastsInNode(caseExpr.Defresult) // self-recursion
		}
	}

	// AND/OR expressions
	if node.GetBoolExpr() != nil {
		boolExpr := node.GetBoolExpr()
		for i, arg := range boolExpr.Args {
			boolExpr.Args[i] = selectRemapper.remapTypeCastsInNode(arg) // self-recursion
		}
	}

	// Comparison expressions
	if node.GetAExpr() != nil {
		aExpr := node.GetAExpr()
		if aExpr.Lexpr != nil {
			aExpr.Lexpr = selectRemapper.remapTypeCastsInNode(aExpr.Lexpr) // self-recursion
		}
		if aExpr.Rexpr != nil {
			aExpr.Rexpr = selectRemapper.remapTypeCastsInNode(aExpr.Rexpr) // self-recursion
		}
	}

	// IN expressions
	if node.GetList() != nil {
		list := node.GetList()
		for i, item := range list.Items {
			list.Items[i] = selectRemapper.remapTypeCastsInNode(item) // self-recursion
		}
	}

	return node
}

func (selectRemapper *SelectRemapper) remapJoinExpressions(selectStatement *pgQuery.SelectStmt, node *pgQuery.Node, indentLevel int) *pgQuery.Node {
	selectRemapper.traceTreeTraversal("JOIN left", indentLevel)
	leftJoinNode := node.GetJoinExpr().Larg
	if leftJoinNode.GetJoinExpr() != nil {
		leftJoinNode = selectRemapper.remapJoinExpressions(selectStatement, leftJoinNode, indentLevel+1) // self-recursion
	} else if leftJoinNode.GetRangeVar() != nil {
		// WHERE
		selectRemapper.traceTreeTraversal("WHERE left", indentLevel+1)
		qSchemaTable := selectRemapper.parserTable.NodeToQuerySchemaTable(leftJoinNode)
		selectStatement = selectRemapper.remapperTable.RemapWhereClauseForTable(qSchemaTable, selectStatement)
		// TABLE
		selectRemapper.traceTreeTraversal("TABLE left", indentLevel+1)
		leftJoinNode = selectRemapper.remapperTable.RemapTable(leftJoinNode)
	} else if leftJoinNode.GetRangeSubselect() != nil {
		leftSelectStatement := leftJoinNode.GetRangeSubselect().Subquery.GetSelectStmt()
		leftSelectStatement = selectRemapper.remapSelectStatement(leftSelectStatement, indentLevel+1) // parent-recursion
	}
	node.GetJoinExpr().Larg = leftJoinNode

	selectRemapper.traceTreeTraversal("JOIN right", indentLevel)
	rightJoinNode := node.GetJoinExpr().Rarg
	if rightJoinNode.GetJoinExpr() != nil {
		rightJoinNode = selectRemapper.remapJoinExpressions(selectStatement, rightJoinNode, indentLevel+1) // self-recursion
	} else if rightJoinNode.GetRangeVar() != nil {
		// WHERE
		selectRemapper.traceTreeTraversal("WHERE right", indentLevel+1)
		qSchemaTable := selectRemapper.parserTable.NodeToQuerySchemaTable(rightJoinNode)
		selectStatement = selectRemapper.remapperTable.RemapWhereClauseForTable(qSchemaTable, selectStatement)
		// TABLE
		selectRemapper.traceTreeTraversal("TABLE right", indentLevel+1)
		rightJoinNode = selectRemapper.remapperTable.RemapTable(rightJoinNode)
	} else if rightJoinNode.GetRangeSubselect() != nil {
		rightSelectStatement := rightJoinNode.GetRangeSubselect().Subquery.GetSelectStmt()
		rightSelectStatement = selectRemapper.remapSelectStatement(rightSelectStatement, indentLevel+1) // parent-recursion
	}
	node.GetJoinExpr().Rarg = rightJoinNode

	selectRemapper.traceTreeTraversal("JOIN on", indentLevel)
	if node.GetJoinExpr().Quals != nil {
		node.GetJoinExpr().Quals = selectRemapper.remapTypeCastsInNode(node.GetJoinExpr().Quals)
	}

	return node
}

func (selectRemapper *SelectRemapper) remapSelect(selectStatement *pgQuery.SelectStmt, indentLevel int) *pgQuery.SelectStmt {
	selectRemapper.traceTreeTraversal("SELECT statements", indentLevel)

	// SELECT ...
	for i, targetNode := range selectStatement.TargetList {
		targetNode = selectRemapper.remapperSelect.RemapSelect(targetNode)
		selectStatement.TargetList[i] = targetNode
	}

	// VALUES (...)
	if len(selectStatement.ValuesLists) > 0 {
		for i, valuesList := range selectStatement.ValuesLists {
			for j, value := range valuesList.GetList().Items {
				selectStatement.ValuesLists[i].GetList().Items[j] = selectRemapper.remapTypeCastsInNode(value)
			}
		}
	}

	return selectStatement
}

func (selectRemapper *SelectRemapper) remapTypecast(node *pgQuery.Node) *pgQuery.Node {
	return selectRemapper.parserType.RemapTypeCast(node)
}

func (selectRemapper *SelectRemapper) traceTreeTraversal(label string, indentLevel int) {
	LogTrace(selectRemapper.config, strings.Repeat(">", indentLevel), label)
}
