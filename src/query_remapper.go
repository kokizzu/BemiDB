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

type QueryRemapper struct {
	parserTable    *ParserTable
	parserType     *ParserType
	remapperTable  *QueryRemapperTable
	remapperWhere  *QueryRemapperWhere
	remapperSelect *QueryRemapperSelect
	icebergReader  *IcebergReader
	duckdb         *Duckdb
	config         *Config
}

func NewQueryRemapper(config *Config, icebergReader *IcebergReader, duckdb *Duckdb) *QueryRemapper {
	return &QueryRemapper{
		parserTable:    NewParserTable(config),
		parserType:     NewParserType(config),
		remapperTable:  NewQueryRemapperTable(config, icebergReader, duckdb),
		remapperWhere:  NewQueryRemapperWhere(config),
		remapperSelect: NewQueryRemapperSelect(config),
		icebergReader:  icebergReader,
		duckdb:         duckdb,
		config:         config,
	}
}

func (remapper *QueryRemapper) RemapStatements(statements []*pgQuery.RawStmt) ([]*pgQuery.RawStmt, error) {
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
			remappedSelect := remapper.remapSelectStatement(stmt.Stmt.GetSelectStmt(), 1)
			stmt.Stmt = &pgQuery.Node{Node: &pgQuery.Node_SelectStmt{SelectStmt: remappedSelect}}
			statements[i] = stmt

		// SET ...
		case node.GetVariableSetStmt() != nil:
			statements[i] = remapper.remapSetStatement(stmt)

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
			LogDebug(remapper.config, "Query tree:", stmt, node)
			return nil, errors.New("unsupported query type")
		}
	}

	return statements, nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// SET ... (no-op)
func (remapper *QueryRemapper) remapSetStatement(stmt *pgQuery.RawStmt) *pgQuery.RawStmt {
	setStatement := stmt.Stmt.GetVariableSetStmt()

	if !KNOWN_SET_STATEMENTS.Contains(strings.ToLower(setStatement.Name)) {
		LogWarn(remapper.config, "Unsupported SET ", setStatement.Name, ":", setStatement)
	}

	return FALLBACK_SET_QUERY_TREE.Stmts[0]
}

func (remapper *QueryRemapper) remapSelectStatement(selectStatement *pgQuery.SelectStmt, indentLevel int) *pgQuery.SelectStmt {
	// Target Sublinks's
	for _, target := range selectStatement.TargetList {
		if subLink := target.GetResTarget().Val.GetSubLink(); subLink != nil {
			remapper.traceTreeTraversal("Target SubLink", indentLevel)
			subSelect := subLink.Subselect.GetSelectStmt()
			remapper.remapSelectStatement(subSelect, indentLevel+1) // self-recursion
		}
	}

	// CASE
	if hasCaseExpr := remapper.hasCaseExpressions(selectStatement); hasCaseExpr {
		remapper.traceTreeTraversal("CASE expressions", indentLevel)
		remapper.remapCaseExpressions(selectStatement, indentLevel) // recursive
	}

	// UNION
	if selectStatement.FromClause == nil && selectStatement.Larg != nil && selectStatement.Rarg != nil {
		remapper.traceTreeTraversal("UNION left", indentLevel)
		leftSelectStatement := selectStatement.Larg
		remapper.remapSelectStatement(leftSelectStatement, indentLevel+1) // self-recursion

		remapper.traceTreeTraversal("UNION right", indentLevel)
		rightSelectStatement := selectStatement.Rarg
		remapper.remapSelectStatement(rightSelectStatement, indentLevel+1) // self-recursion
	}

	// JOIN
	if len(selectStatement.FromClause) > 0 && selectStatement.FromClause[0].GetJoinExpr() != nil {
		selectStatement.FromClause[0] = remapper.remapJoinExpressions(selectStatement, selectStatement.FromClause[0], indentLevel+1) // recursive with self-recursion
	}

	// WHERE
	if selectStatement.WhereClause != nil {
		selectStatement.WhereClause = remapper.remapTypeCastsInNode(selectStatement.WhereClause)
		selectStatement = remapper.remapperWhere.RemapWhereExpressions(selectStatement, selectStatement.WhereClause, indentLevel)
	}

	// WITH
	if selectStatement.WithClause != nil {
		remapper.traceTreeTraversal("WITH CTE's", indentLevel)
		for _, cte := range selectStatement.WithClause.Ctes {
			if cteSelect := cte.GetCommonTableExpr().Ctequery.GetSelectStmt(); cteSelect != nil {
				remapper.remapSelectStatement(cteSelect, indentLevel+1) // self-recursion
			}
		}
	}

	// FROM
	if len(selectStatement.FromClause) > 0 {
		for i, fromNode := range selectStatement.FromClause {
			if fromNode.GetRangeVar() != nil {
				remapper.traceTreeTraversal("WHERE statements", indentLevel)
				// FROM [TABLE]
				remapper.traceTreeTraversal("FROM table", indentLevel)
				selectStatement.FromClause[i] = remapper.remapperTable.RemapTable(fromNode)
				qSchemaTable := remapper.parserTable.NodeToQuerySchemaTable(fromNode)
				selectStatement = remapper.remapperTable.RemapWhereClauseForTable(qSchemaTable, selectStatement)
			} else if fromNode.GetRangeSubselect() != nil {
				// FROM (SELECT ...)
				remapper.traceTreeTraversal("FROM subselect", indentLevel)
				subSelectStatement := fromNode.GetRangeSubselect().Subquery.GetSelectStmt()
				remapper.remapSelectStatement(subSelectStatement, indentLevel+1) // self-recursion
			} else if fromNode.GetRangeFunction() != nil {
				// FROM PG_FUNCTION()
				selectStatement.FromClause[i] = remapper.remapTableFunction(fromNode, indentLevel) // recursive
			}
		}
	}

	selectStatement = remapper.remapSelect(selectStatement, indentLevel) // recursive
	return selectStatement
}

func (remapper *QueryRemapper) hasCaseExpressions(selectStatement *pgQuery.SelectStmt) bool {
	for _, target := range selectStatement.TargetList {
		if target.GetResTarget().Val.GetCaseExpr() != nil {
			return true
		}
	}
	return false
}

func (remapper *QueryRemapper) remapCaseExpressions(selectStatement *pgQuery.SelectStmt, indentLevel int) *pgQuery.SelectStmt {
	for _, target := range selectStatement.TargetList {
		if caseExpr := target.GetResTarget().Val.GetCaseExpr(); caseExpr != nil {

			remapper.ensureConsistentCaseTypes(caseExpr)

			for _, when := range caseExpr.Args {
				if whenClause := when.GetCaseWhen(); whenClause != nil {
					if whenClause.Expr != nil {
						if aExpr := whenClause.Expr.GetAExpr(); aExpr != nil {
							if subLink := aExpr.Lexpr.GetSubLink(); subLink != nil {
								remapper.traceTreeTraversal("CASE WHEN left", indentLevel+1)
								subSelect := subLink.Subselect.GetSelectStmt()
								remapper.remapSelectStatement(subSelect, indentLevel+1)
							}
							if subLink := aExpr.Rexpr.GetSubLink(); subLink != nil {
								remapper.traceTreeTraversal("CASE WHEN right", indentLevel+1)
								subSelect := subLink.Subselect.GetSelectStmt()
								remapper.remapSelectStatement(subSelect, indentLevel+1)
							}
						}
					}

					if whenClause.Result != nil {
						if subLink := whenClause.Result.GetSubLink(); subLink != nil {
							remapper.traceTreeTraversal("CASE THEN", indentLevel+1)
							subSelect := subLink.Subselect.GetSelectStmt()
							remapper.remapSelectStatement(subSelect, indentLevel+1)
						}
						if funcCall := whenClause.Result.GetFuncCall(); funcCall != nil {
							remapper.traceTreeTraversal("CASE THEN function", indentLevel+1)
							whenClause.Result = remapper.remapperSelect.RemapFunctionToConstant(funcCall)
						}
					}
				}
			}

			if caseExpr.Defresult != nil {
				if subLink := caseExpr.Defresult.GetSubLink(); subLink != nil {
					remapper.traceTreeTraversal("CASE ELSE", indentLevel+1)
					subSelect := subLink.Subselect.GetSelectStmt()
					remapper.remapSelectStatement(subSelect, indentLevel+1)
				}
				if funcCall := caseExpr.Defresult.GetFuncCall(); funcCall != nil {
					remapper.traceTreeTraversal("CASE ELSE function", indentLevel+1)
					caseExpr.Defresult = remapper.remapperSelect.RemapFunctionToConstant(funcCall)
				}
			}
		}
	}
	return selectStatement
}

func (remapper *QueryRemapper) ensureConsistentCaseTypes(caseExpr *pgQuery.CaseExpr) {
	if len(caseExpr.Args) > 0 {
		if when := caseExpr.Args[0].GetCaseWhen(); when != nil && when.Result != nil {
			if typeName := remapper.parserType.inferNodeType(when.Result); typeName != "" {
				// WHEN
				for i := 1; i < len(caseExpr.Args); i++ {
					if whenClause := caseExpr.Args[i].GetCaseWhen(); whenClause != nil && whenClause.Result != nil {
						whenClause.Result = remapper.parserType.MakeCaseTypeCastNode(whenClause.Result, typeName)
					}
				}
				// ELSE
				if caseExpr.Defresult != nil {
					caseExpr.Defresult = remapper.parserType.MakeCaseTypeCastNode(caseExpr.Defresult, typeName)
				}
			}
		}
	}
}

// FROM PG_FUNCTION()
func (remapper *QueryRemapper) remapTableFunction(fromNode *pgQuery.Node, indentLevel int) *pgQuery.Node {
	remapper.traceTreeTraversal("FROM function()", indentLevel)

	fromNode = remapper.remapperTable.RemapTableFunction(fromNode)
	if fromNode.GetRangeFunction() == nil {
		return fromNode
	}

	for _, funcNode := range fromNode.GetRangeFunction().Functions {
		for _, funcItemNode := range funcNode.GetList().Items {
			funcCallNode := funcItemNode.GetFuncCall()
			if funcCallNode == nil {
				continue
			}
			remapper.remapTableFunctionArgs(funcCallNode, indentLevel+1) // recursive
		}
	}

	return fromNode
}

// FROM PG_FUNCTION(PG_NESTED_FUNCTION())
func (remapper *QueryRemapper) remapTableFunctionArgs(funcCallNode *pgQuery.FuncCall, indentLevel int) *pgQuery.FuncCall {
	remapper.traceTreeTraversal("FROM nested_function()", indentLevel)

	for i, argNode := range funcCallNode.GetArgs() {
		nestedFunctionCall := argNode.GetFuncCall()
		if nestedFunctionCall == nil {
			continue
		}

		nestedFunctionCall = remapper.remapperTable.RemapNestedTableFunction(nestedFunctionCall)
		nestedFunctionCall = remapper.remapTableFunctionArgs(nestedFunctionCall, indentLevel+1) // recursive

		funcCallNode.Args[i].Node = &pgQuery.Node_FuncCall{FuncCall: nestedFunctionCall}
	}

	return funcCallNode
}

func (remapper *QueryRemapper) remapTypeCastsInNode(node *pgQuery.Node) *pgQuery.Node {
	if node == nil {
		return nil
	}

	// Direct typecast
	if node.GetTypeCast() != nil {
		return remapper.remapTypecast(node)
	}

	// Handle CASE expressions
	if node.GetCaseExpr() != nil {
		caseExpr := node.GetCaseExpr()
		// Handle WHEN clauses
		for i, when := range caseExpr.Args {
			whenClause := when.GetCaseWhen()
			if whenClause.Result != nil {
				whenClause.Result = remapper.remapTypeCastsInNode(whenClause.Result) // self-recursion
			}
			caseExpr.Args[i] = when
		}
		// Handle ELSE clause
		if caseExpr.Defresult != nil {
			caseExpr.Defresult = remapper.remapTypeCastsInNode(caseExpr.Defresult) // self-recursion
		}
	}

	// AND/OR expressions
	if node.GetBoolExpr() != nil {
		boolExpr := node.GetBoolExpr()
		for i, arg := range boolExpr.Args {
			boolExpr.Args[i] = remapper.remapTypeCastsInNode(arg) // self-recursion
		}
	}

	// Comparison expressions
	if node.GetAExpr() != nil {
		aExpr := node.GetAExpr()
		if aExpr.Lexpr != nil {
			aExpr.Lexpr = remapper.remapTypeCastsInNode(aExpr.Lexpr) // self-recursion
		}
		if aExpr.Rexpr != nil {
			aExpr.Rexpr = remapper.remapTypeCastsInNode(aExpr.Rexpr) // self-recursion
		}
	}

	// IN expressions
	if node.GetList() != nil {
		list := node.GetList()
		for i, item := range list.Items {
			list.Items[i] = remapper.remapTypeCastsInNode(item) // self-recursion
		}
	}

	return node
}

func (remapper *QueryRemapper) remapJoinExpressions(selectStatement *pgQuery.SelectStmt, node *pgQuery.Node, indentLevel int) *pgQuery.Node {
	remapper.traceTreeTraversal("JOIN left", indentLevel)
	leftJoinNode := node.GetJoinExpr().Larg
	if leftJoinNode.GetJoinExpr() != nil {
		leftJoinNode = remapper.remapJoinExpressions(selectStatement, leftJoinNode, indentLevel+1) // self-recursion
	} else if leftJoinNode.GetRangeVar() != nil {
		// WHERE
		remapper.traceTreeTraversal("WHERE left", indentLevel+1)
		qSchemaTable := remapper.parserTable.NodeToQuerySchemaTable(leftJoinNode)
		selectStatement = remapper.remapperTable.RemapWhereClauseForTable(qSchemaTable, selectStatement)
		// TABLE
		remapper.traceTreeTraversal("TABLE left", indentLevel+1)
		leftJoinNode = remapper.remapperTable.RemapTable(leftJoinNode)
	} else if leftJoinNode.GetRangeSubselect() != nil {
		leftSelectStatement := leftJoinNode.GetRangeSubselect().Subquery.GetSelectStmt()
		remapper.remapSelectStatement(leftSelectStatement, indentLevel+1) // parent-recursion
	}
	node.GetJoinExpr().Larg = leftJoinNode

	remapper.traceTreeTraversal("JOIN right", indentLevel)
	rightJoinNode := node.GetJoinExpr().Rarg
	if rightJoinNode.GetJoinExpr() != nil {
		rightJoinNode = remapper.remapJoinExpressions(selectStatement, rightJoinNode, indentLevel+1) // self-recursion
	} else if rightJoinNode.GetRangeVar() != nil {
		// WHERE
		remapper.traceTreeTraversal("WHERE right", indentLevel+1)
		qSchemaTable := remapper.parserTable.NodeToQuerySchemaTable(rightJoinNode)
		remapper.remapperTable.RemapWhereClauseForTable(qSchemaTable, selectStatement)
		// TABLE
		remapper.traceTreeTraversal("TABLE right", indentLevel+1)
		rightJoinNode = remapper.remapperTable.RemapTable(rightJoinNode)
	} else if rightJoinNode.GetRangeSubselect() != nil {
		rightSelectStatement := rightJoinNode.GetRangeSubselect().Subquery.GetSelectStmt()
		remapper.remapSelectStatement(rightSelectStatement, indentLevel+1) // parent-recursion
	}
	node.GetJoinExpr().Rarg = rightJoinNode

	remapper.traceTreeTraversal("JOIN on", indentLevel)
	if node.GetJoinExpr().Quals != nil {
		node.GetJoinExpr().Quals = remapper.remapTypeCastsInNode(node.GetJoinExpr().Quals)
	}

	return node
}

func (remapper *QueryRemapper) remapSelect(selectStatement *pgQuery.SelectStmt, indentLevel int) *pgQuery.SelectStmt {
	remapper.traceTreeTraversal("SELECT statements", indentLevel)

	// SELECT ...
	for i, targetNode := range selectStatement.TargetList {
		targetNode = remapper.remapperSelect.RemapSelect(targetNode)
		selectStatement.TargetList[i] = targetNode
	}

	// VALUES (...)
	if len(selectStatement.ValuesLists) > 0 {
		for i, valuesList := range selectStatement.ValuesLists {
			for j, value := range valuesList.GetList().Items {
				selectStatement.ValuesLists[i].GetList().Items[j] = remapper.remapTypeCastsInNode(value)
			}
		}
	}

	return selectStatement
}

func (remapper *QueryRemapper) remapTypecast(node *pgQuery.Node) *pgQuery.Node {
	return remapper.parserType.RemapTypeCast(node)
}

func (remapper *QueryRemapper) traceTreeTraversal(label string, indentLevel int) {
	LogTrace(remapper.config, strings.Repeat(">", indentLevel), label)
}
