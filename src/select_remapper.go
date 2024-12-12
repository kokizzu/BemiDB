package main

import (
	"strings"

	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

var REMAPPED_CONSTANT_BY_PG_FUNCTION_NAME = map[string]string{
	"version":                            "PostgreSQL " + PG_VERSION + ", compiled by Bemi",
	"pg_get_userbyid":                    "bemidb",
	"pg_get_function_identity_arguments": "",
	"pg_total_relation_size":             "0",
	"pg_table_size":                      "0",
	"pg_indexes_size":                    "0",
	"pg_get_partkeydef":                  "",
}

var KNOWN_SET_STATEMENTS = NewSet([]string{
	"client_encoding",             // SET client_encoding TO 'UTF8'
	"client_min_messages",         // SET client_min_messages TO 'warning'
	"standard_conforming_strings", // SET standard_conforming_strings = on
	"intervalstyle",               // SET intervalstyle = iso_8601
	"timezone",                    // SET SESSION timezone TO 'UTC'
})

type SelectRemapper struct {
	remapperTable  *SelectRemapperTable
	remapperWhere  *SelectRemapperWhere
	remapperSelect *SelectRemapperSelect
	icebergReader  *IcebergReader
	config         *Config
}

func NewSelectRemapper(config *Config, icebergReader *IcebergReader) *SelectRemapper {
	return &SelectRemapper{
		remapperTable:  NewSelectRemapperTable(config, icebergReader),
		remapperWhere:  NewSelectRemapperWhere(config),
		remapperSelect: NewSelectRemapperSelect(config),
		icebergReader:  icebergReader,
		config:         config,
	}
}

// SELECT ...
func (selectRemapper *SelectRemapper) RemapQueryTreeWithSelect(queryTree *pgQuery.ParseResult) *pgQuery.ParseResult {
	selectStatement := queryTree.Stmts[0].Stmt.GetSelectStmt()
	selectStatement = selectRemapper.remapSelectStatement(selectStatement, 0)

	return queryTree
}

// SET ... (no-op)
func (selectRemapper *SelectRemapper) RemapQueryTreeWithSet(queryTree *pgQuery.ParseResult) *pgQuery.ParseResult {
	setStatement := queryTree.Stmts[0].Stmt.GetVariableSetStmt()

	if !KNOWN_SET_STATEMENTS.Contains(setStatement.Name) {
		LogWarn(selectRemapper.config, "Unsupported SET ", setStatement.Name, ":", setStatement)
	}

	queryTree.Stmts[0].Stmt.GetVariableSetStmt().Name = "schema"
	queryTree.Stmts[0].Stmt.GetVariableSetStmt().Args = []*pgQuery.Node{
		pgQuery.MakeAConstStrNode("main", 0),
	}

	return queryTree
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (selectRemapper *SelectRemapper) remapSelectStatement(selectStatement *pgQuery.SelectStmt, indentLevel int) *pgQuery.SelectStmt {
	selectStatement = selectRemapper.remapTypeCastsInSelect(selectStatement)

	// UNION
	if selectStatement.FromClause == nil && selectStatement.Larg != nil && selectStatement.Rarg != nil {
		selectRemapper.logTreeTraversal("UNION left", indentLevel)
		leftSelectStatement := selectStatement.Larg
		leftSelectStatement = selectRemapper.remapSelectStatement(leftSelectStatement, indentLevel+1) // self-recursion

		selectRemapper.logTreeTraversal("UNION right", indentLevel)
		rightSelectStatement := selectStatement.Rarg
		rightSelectStatement = selectRemapper.remapSelectStatement(rightSelectStatement, indentLevel+1) // self-recursion

		return selectStatement
	}

	// JOIN
	if len(selectStatement.FromClause) > 0 && selectStatement.FromClause[0].GetJoinExpr() != nil {
		// SELECT
		selectStatement = selectRemapper.remapSelect(selectStatement, indentLevel)                                      // recursive
		selectStatement.FromClause[0] = selectRemapper.remapJoinExpressions(selectStatement.FromClause[0], indentLevel) // recursive with self-recursion
		return selectStatement
	}

	// FROM
	if len(selectStatement.FromClause) > 0 {
		// WHERE
		if selectStatement.FromClause[0].GetRangeVar() != nil {
			selectRemapper.logTreeTraversal("WHERE statements", indentLevel)
			selectStatement = selectRemapper.remapperWhere.RemapWhere(selectStatement)
		}

		// SELECT
		selectStatement = selectRemapper.remapSelect(selectStatement, indentLevel) // recursive

		for i, fromNode := range selectStatement.FromClause {
			if fromNode.GetRangeVar() != nil {
				selectRemapper.logTreeTraversal("FROM table", indentLevel)
				selectStatement.FromClause[i] = selectRemapper.remapperTable.RemapTable(fromNode)
			} else if fromNode.GetRangeSubselect() != nil {
				selectRemapper.logTreeTraversal("FROM subselect", indentLevel)
				subSelectStatement := fromNode.GetRangeSubselect().Subquery.GetSelectStmt()
				subSelectStatement = selectRemapper.remapSelectStatement(subSelectStatement, indentLevel+1) // self-recursion
			}

			if fromNode.GetRangeFunction() != nil {
				selectStatement.FromClause[i] = selectRemapper.remapTableFunction(fromNode, indentLevel+1) // recursive
			}
		}
		return selectStatement
	}

	selectStatement = selectRemapper.remapSelect(selectStatement, indentLevel) // recursive
	return selectStatement
}

// FROM PG_FUNCTION()
func (selectRemapper *SelectRemapper) remapTableFunction(fromNode *pgQuery.Node, indentLevel int) *pgQuery.Node {
	selectRemapper.logTreeTraversal("FROM function()", indentLevel)

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
	selectRemapper.logTreeTraversal("FROM nested_function()", indentLevel)

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

func (selectRemapper *SelectRemapper) remapTypeCastsInSelect(selectStatement *pgQuery.SelectStmt) *pgQuery.SelectStmt {
	// WHERE [CONDITION]
	if selectStatement.WhereClause != nil {
		selectStatement.WhereClause = selectRemapper.remapTypeCastsInNode(selectStatement.WhereClause)
	}

	// FROM / JOIN [TABLE] and VALUES
	if len(selectStatement.FromClause) > 0 {
		for _, fromNode := range selectStatement.FromClause {
			if fromNode.GetJoinExpr() != nil {
				joinExpr := fromNode.GetJoinExpr()
				if joinExpr.Quals != nil {
					joinExpr.Quals = selectRemapper.remapTypeCastsInNode(joinExpr.Quals)
				}
			}
			// Subqueries
			if fromNode.GetRangeSubselect() != nil {
				subSelect := fromNode.GetRangeSubselect().Subquery.GetSelectStmt()
				selectRemapper.remapTypeCastsInSelect(subSelect)
			}
		}
	}

	// VALUES list
	if len(selectStatement.ValuesLists) > 0 {
		for i, valuesList := range selectStatement.ValuesLists {
			for j, value := range valuesList.GetList().Items {
				selectStatement.ValuesLists[i].GetList().Items[j] = selectRemapper.remapTypeCastsInNode(value)
			}
		}
	}

	return selectStatement
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
				whenClause.Result = selectRemapper.remapTypeCastsInNode(whenClause.Result)
			}
			caseExpr.Args[i] = when
		}
		// Handle ELSE clause
		if caseExpr.Defresult != nil {
			caseExpr.Defresult = selectRemapper.remapTypeCastsInNode(caseExpr.Defresult)
		}
	}

	// AND/OR expressions
	if node.GetBoolExpr() != nil {
		boolExpr := node.GetBoolExpr()
		for i, arg := range boolExpr.Args {
			boolExpr.Args[i] = selectRemapper.remapTypeCastsInNode(arg)
		}
	}

	// Comparison expressions
	if node.GetAExpr() != nil {
		aExpr := node.GetAExpr()
		if aExpr.Lexpr != nil {
			aExpr.Lexpr = selectRemapper.remapTypeCastsInNode(aExpr.Lexpr)
		}
		if aExpr.Rexpr != nil {
			aExpr.Rexpr = selectRemapper.remapTypeCastsInNode(aExpr.Rexpr)
		}
	}

	// IN expressions
	if node.GetList() != nil {
		list := node.GetList()
		for i, item := range list.Items {
			list.Items[i] = selectRemapper.remapTypeCastsInNode(item)
		}
	}

	return node
}

func (selectRemapper *SelectRemapper) remapJoinExpressions(node *pgQuery.Node, indentLevel int) *pgQuery.Node {
	selectRemapper.logTreeTraversal("JOIN left", indentLevel+1)
	leftJoinNode := node.GetJoinExpr().Larg
	if leftJoinNode.GetJoinExpr() != nil {
		leftJoinNode = selectRemapper.remapJoinExpressions(leftJoinNode, indentLevel+1) // self-recursion
	} else if leftJoinNode.GetRangeVar() != nil {
		leftJoinNode = selectRemapper.remapperTable.RemapTable(leftJoinNode)
	} else if leftJoinNode.GetRangeSubselect() != nil {
		leftSelectStatement := leftJoinNode.GetRangeSubselect().Subquery.GetSelectStmt()
		leftSelectStatement = selectRemapper.remapSelectStatement(leftSelectStatement, indentLevel+1) // parent-recursion
	}
	node.GetJoinExpr().Larg = leftJoinNode

	selectRemapper.logTreeTraversal("JOIN right", indentLevel+1)
	rightJoinNode := node.GetJoinExpr().Rarg
	if rightJoinNode.GetJoinExpr() != nil {
		rightJoinNode = selectRemapper.remapJoinExpressions(rightJoinNode, indentLevel+1) // self-recursion
	} else if rightJoinNode.GetRangeVar() != nil {
		rightJoinNode = selectRemapper.remapperTable.RemapTable(rightJoinNode)
	} else if rightJoinNode.GetRangeSubselect() != nil {
		rightSelectStatement := rightJoinNode.GetRangeSubselect().Subquery.GetSelectStmt()
		rightSelectStatement = selectRemapper.remapSelectStatement(rightSelectStatement, indentLevel+1) // parent-recursion
	}
	node.GetJoinExpr().Rarg = rightJoinNode

	return node
}

func (selectRemapper *SelectRemapper) remapSelect(selectStatement *pgQuery.SelectStmt, indentLevel int) *pgQuery.SelectStmt {
	selectRemapper.logTreeTraversal("SELECT statements", indentLevel+1)

	for i, targetNode := range selectStatement.TargetList {
		targetNode = selectRemapper.remapperSelect.RemapSelect(targetNode)

		// Recursively remap sub-selects
		subSelectStatement := selectRemapper.remapperSelect.SubselectStatement(targetNode)
		if subSelectStatement != nil {
			subSelectStatement = selectRemapper.remapSelect(subSelectStatement, indentLevel+1) // self-recursion
		}

		selectStatement.TargetList[i] = targetNode
	}

	return selectStatement
}

func (selectRemapper *SelectRemapper) remapTypecast(node *pgQuery.Node) *pgQuery.Node {
	if node.GetTypeCast() != nil {
		typeCast := node.GetTypeCast()
		if len(typeCast.TypeName.Names) > 0 {
			typeName := typeCast.TypeName.Names[0].GetString_().Sval
			if typeName == "regclass" {
				return typeCast.Arg
			}

			if typeName == "text" {
				arrayStr := typeCast.Arg.GetAConst().GetSval().Sval
				arrayStr = strings.Trim(arrayStr, "{}")
				elements := strings.Split(arrayStr, ",")

				funcCall := &pgQuery.FuncCall{
					Funcname: []*pgQuery.Node{
						pgQuery.MakeStrNode("list_value"),
					},
				}

				for _, elem := range elements {
					funcCall.Args = append(funcCall.Args,
						pgQuery.MakeAConstStrNode(elem, 0))
				}

				return &pgQuery.Node{
					Node: &pgQuery.Node_FuncCall{
						FuncCall: funcCall,
					},
				}
			}
		}
	}
	return node
}

func (selectRemapper *SelectRemapper) logTreeTraversal(label string, indentLevel int) {
	LogDebug(selectRemapper.config, strings.Repeat(">", indentLevel), label)
}
