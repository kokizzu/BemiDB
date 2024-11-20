package main

import (
	"strings"

	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

const (
	INFORMATION_SCHEMA = "information_schema"
	PG_DEFAULT_SCHEMA  = "public"
	PG_SYSTEM_SCHEMA   = "pg_catalog"

	INFORMATION_SCHEMA_TABLES = "tables"
	PG_NAMESPACE              = "pg_namespace"
	PG_STATIO_USER_TABLES     = "pg_statio_user_tables"
	PG_SHADOW                 = "pg_shadow"

	PG_QUOTE_INDENT_FUNCTION_NAME = "quote_ident"
)

var REMAPPED_CONSTANT_BY_PG_FUNCTION_NAME = map[string]string{
	"version":                            "PostgreSQL " + PG_VERSION + ", compiled by Bemi",
	"pg_get_userbyid":                    "bemidb",
	"pg_get_function_identity_arguments": "",
	"pg_total_relation_size":             "0",
	"pg_table_size":                      "0",
	"pg_indexes_size":                    "0",
}

var KNOWN_SET_STATEMENTS = NewSet([]string{
	"client_encoding",             // SET client_encoding TO 'UTF8'
	"client_min_messages",         // SET client_min_messages TO 'warning'
	"standard_conforming_strings", // SET standard_conforming_strings = on
	"intervalstyle",               // SET intervalstyle = iso_8601
	"timezone",                    // SET SESSION timezone TO 'UTC'
})

type SelectRemapper struct {
	icebergReader *IcebergReader
	config        *Config
}

func (selectRemapper *SelectRemapper) RemapQueryTreeWithSelect(queryTree *pgQuery.ParseResult) *pgQuery.ParseResult {
	selectStatement := queryTree.Stmts[0].Stmt.GetSelectStmt()
	selectStatement = selectRemapper.remapSelectStatement(selectStatement, 0)

	return queryTree
}

// No-op
func (selectRemapper *SelectRemapper) RemapQueryTreeWithSet(queryTree *pgQuery.ParseResult) *pgQuery.ParseResult {
	setStatement := queryTree.Stmts[0].Stmt.GetVariableSetStmt()

	if !KNOWN_SET_STATEMENTS.Contains(setStatement.Name) {
		LogError(selectRemapper.config, "Unsupported SET ", setStatement.Name, ":", setStatement)
	}

	queryTree.Stmts[0].Stmt.GetVariableSetStmt().Name = "schema"
	queryTree.Stmts[0].Stmt.GetVariableSetStmt().Args = []*pgQuery.Node{
		pgQuery.MakeAConstStrNode("main", 0),
	}

	return queryTree
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (selectRemapper *SelectRemapper) remapSelectStatement(selectStatement *pgQuery.SelectStmt, indentLevel int) *pgQuery.SelectStmt {
	if selectStatement.FromClause == nil && selectStatement.Larg != nil && selectStatement.Rarg != nil {
		LogDebug(selectRemapper.config, strings.Repeat(">", indentLevel+1)+" UNION left")
		leftSelectStatement := selectStatement.Larg
		leftSelectStatement = selectRemapper.remapSelectStatement(leftSelectStatement, indentLevel+1)

		LogDebug(selectRemapper.config, strings.Repeat(">", indentLevel+1)+" UNION right")
		rightSelectStatement := selectStatement.Rarg
		rightSelectStatement = selectRemapper.remapSelectStatement(rightSelectStatement, indentLevel+1)

		return selectStatement
	}

	if len(selectStatement.FromClause) > 0 && selectStatement.FromClause[0].GetJoinExpr() != nil {
		selectStatement = selectRemapper.remapSelect(selectStatement, indentLevel)
		selectRemapper.remapJoinExpressions(selectStatement.FromClause[0], indentLevel)
		return selectStatement
	}

	if len(selectStatement.FromClause) > 0 {
		if selectStatement.FromClause[0].GetRangeVar() != nil {
			selectStatement = selectRemapper.remapWhere(selectStatement)
		}
		selectStatement = selectRemapper.remapSelect(selectStatement, indentLevel)
		for i, fromNode := range selectStatement.FromClause {
			if fromNode.GetRangeVar() != nil {
				LogDebug(selectRemapper.config, strings.Repeat(">", indentLevel+1)+" SELECT statement")
				selectStatement.FromClause[i] = selectRemapper.remapTable(fromNode)
			} else if fromNode.GetRangeSubselect() != nil {
				selectRemapper.remapSelectStatement(fromNode.GetRangeSubselect().Subquery.GetSelectStmt(), indentLevel+1)
			}
		}
		return selectStatement
	}

	selectStatement = selectRemapper.remapSelect(selectStatement, indentLevel)
	return selectStatement
}

func (selectRemapper *SelectRemapper) remapJoinExpressions(node *pgQuery.Node, indentLevel int) *pgQuery.Node {
	LogDebug(selectRemapper.config, strings.Repeat(">", indentLevel+1)+" JOIN left")
	leftJoinNode := node.GetJoinExpr().Larg
	if leftJoinNode.GetJoinExpr() != nil {
		leftJoinNode = selectRemapper.remapJoinExpressions(leftJoinNode, indentLevel+1)
	} else if leftJoinNode.GetRangeVar() != nil {
		leftJoinNode = selectRemapper.remapTable(leftJoinNode)
	} else if leftJoinNode.GetRangeSubselect() != nil {
		leftSelectStatement := leftJoinNode.GetRangeSubselect().Subquery.GetSelectStmt()
		leftSelectStatement = selectRemapper.remapSelectStatement(leftSelectStatement, indentLevel+1)
	}

	LogDebug(selectRemapper.config, strings.Repeat(">", indentLevel+1)+" JOIN right")
	rightJoinNode := node.GetJoinExpr().Rarg
	if rightJoinNode.GetJoinExpr() != nil {
		rightJoinNode = selectRemapper.remapJoinExpressions(rightJoinNode, indentLevel+1)
	} else if rightJoinNode.GetRangeVar() != nil {
		rightJoinNode = selectRemapper.remapTable(rightJoinNode)
	} else if rightJoinNode.GetRangeSubselect() != nil {
		rightSelectStatement := rightJoinNode.GetRangeSubselect().Subquery.GetSelectStmt()
		rightSelectStatement = selectRemapper.remapSelectStatement(rightSelectStatement, indentLevel+1)
	}

	return node
}

// WHERE [CONDITION]
func (selectRemapper *SelectRemapper) remapWhere(selectStatement *pgQuery.SelectStmt) *pgQuery.SelectStmt {
	fromVar := selectStatement.FromClause[0].GetRangeVar()
	schemaName := fromVar.Schemaname
	tableName := fromVar.Relname

	// System pg_* tables
	if (schemaName == PG_SYSTEM_SCHEMA || schemaName == "") && IsSystemTable(tableName) {
		switch tableName {
		case PG_NAMESPACE:
			// FROM pg_catalog.pg_namespace => FROM pg_catalog.pg_namespace WHERE nspname != 'main'
			withoutMainSchemaWhereCondition := MakeStringExpressionNode("nspname", "!=", "main")
			return selectRemapper.appendWhereCondition(selectStatement, withoutMainSchemaWhereCondition)
		case PG_STATIO_USER_TABLES:
			// FROM pg_catalog.pg_statio_user_tables -> return nothing
			falseWhereCondition := MakeAConstBoolNode(false)
			selectStatement = selectRemapper.overrideWhereCondition(selectStatement, falseWhereCondition)
			return selectStatement
		}
	}

	return selectStatement
}

// FROM / JOIN [TABLE]
func (selectRemapper *SelectRemapper) remapTable(node *pgQuery.Node) *pgQuery.Node {
	rangeVar := node.GetRangeVar()
	schemaName := rangeVar.Schemaname
	tableName := rangeVar.Relname

	// System pg_* tables
	if (schemaName == PG_SYSTEM_SCHEMA || schemaName == "") && IsSystemTable(tableName) {
		switch tableName {
		case PG_STATIO_USER_TABLES:
			// FROM pg_catalog.pg_statio_user_tables -> return nothing
			tableNode := MakePgStatioUserTablesNode()
			return selectRemapper.overrideTable(node, tableNode)
		case PG_SHADOW:
			// FROM pg_shadow -> return hard-coded credentials
			tableNode := MakePgShadowNode(selectRemapper.config.User, selectRemapper.config.EncryptedPassword)
			return selectRemapper.overrideTable(node, tableNode)
		default:
			// System pg_* tables
			return node
		}
	}

	// Information schema
	if schemaName == INFORMATION_SCHEMA {
		switch tableName {
		case INFORMATION_SCHEMA_TABLES:
			icebergSchemaTables, err := selectRemapper.icebergReader.SchemaTables()
			if err != nil {
				LogError(selectRemapper.config, "Failed to get Iceberg schema tables:", err)
				return node
			}
			if len(icebergSchemaTables) == 0 {
				return node
			}
			// FROM information_schema.tables -> return Iceberg tables
			tableNode := MakeInformationSchemaTablesNode(selectRemapper.config.Database, icebergSchemaTables)
			return selectRemapper.overrideTable(node, tableNode)
		default:
			return node
		}
	}

	// iceberg.table
	return node
}

func (selectRemapper *SelectRemapper) appendWhereCondition(selectStatement *pgQuery.SelectStmt, whereCondition *pgQuery.Node) *pgQuery.SelectStmt {
	whereClause := selectStatement.WhereClause

	if whereClause == nil {
		selectStatement.WhereClause = whereCondition
	} else if whereClause.GetBoolExpr() != nil {
		boolExpr := whereClause.GetBoolExpr()
		if boolExpr.Boolop.String() == "AND_EXPR" {
			selectStatement.WhereClause.GetBoolExpr().Args = append(boolExpr.Args, whereCondition)
		}
	} else if whereClause.GetAExpr() != nil {
		selectStatement.WhereClause = pgQuery.MakeBoolExprNode(
			pgQuery.BoolExprType_AND_EXPR,
			[]*pgQuery.Node{whereClause, whereCondition},
			0,
		)
	}
	return selectStatement
}

func (selectRemapper *SelectRemapper) overrideWhereCondition(selectStatement *pgQuery.SelectStmt, whereCondition *pgQuery.Node) *pgQuery.SelectStmt {
	selectStatement.WhereClause = whereCondition
	return selectStatement
}

func (selectRemapper *SelectRemapper) overrideTable(node *pgQuery.Node, fromClause *pgQuery.Node) *pgQuery.Node {
	node = fromClause
	return node
}

// SELECT [PG_FUNCTION()]
func (selectRemapper *SelectRemapper) remapSelect(selectStatement *pgQuery.SelectStmt, indentLevel int) *pgQuery.SelectStmt {
	LogDebug(selectRemapper.config, strings.Repeat(">", indentLevel+1)+" SELECT functions")

	for _, targetItem := range selectStatement.TargetList {
		target := targetItem.GetResTarget()
		if target.Val.GetFuncCall() != nil {
			functionCall := target.Val.GetFuncCall()
			originalFunctionName := functionCall.Funcname[len(functionCall.Funcname)-1].GetString_().Sval

			renamedFunctionCall := selectRemapper.remappedFunctionName(functionCall)
			if renamedFunctionCall != nil {
				functionCall = renamedFunctionCall
				if target.Name == "" {
					target.Name = originalFunctionName
				}
			}

			constantNode := selectRemapper.remappedConstantNode(functionCall)
			if constantNode != nil {
				target.Val = constantNode
				if target.Name == "" {
					target.Name = originalFunctionName
				}
			}

			functionCall = selectRemapper.remapFunctionCallArgs(functionCall, indentLevel+1)
		} else if target.Val.GetSubLink() != nil {
			subSelectStatement := target.Val.GetSubLink().Subselect.GetSelectStmt()
			subSelectStatement = selectRemapper.remapSelect(subSelectStatement, indentLevel+1)
		}
	}

	return selectStatement
}

func (selectRemapper *SelectRemapper) remapFunctionCallArgs(functionCall *pgQuery.FuncCall, indentLevel int) *pgQuery.FuncCall {
	LogDebug(selectRemapper.config, strings.Repeat(">", indentLevel+1)+" SELECT function args")

	for i, arg := range functionCall.Args {
		if arg.GetFuncCall() != nil {
			argFunctionCall := arg.GetFuncCall()

			renamedFunctionCall := selectRemapper.remappedFunctionName(argFunctionCall)
			if renamedFunctionCall != nil {
				argFunctionCall = renamedFunctionCall
			}

			constantNode := selectRemapper.remappedConstantNode(argFunctionCall)
			if constantNode != nil {
				functionCall.Args[i] = constantNode
			}
			argFunctionCall = selectRemapper.remapFunctionCallArgs(argFunctionCall, indentLevel+1)
		}
	}

	return functionCall
}

func (selectRemapper *SelectRemapper) remappedFunctionName(functionCall *pgQuery.FuncCall) *pgQuery.FuncCall {
	functionName := functionCall.Funcname[len(functionCall.Funcname)-1].GetString_().Sval

	if functionName == PG_QUOTE_INDENT_FUNCTION_NAME {
		functionCall.Funcname[0] = pgQuery.MakeStrNode("concat")
		argConstant := functionCall.Args[0].GetAConst()
		if argConstant != nil {
			str := argConstant.GetSval().Sval
			str = "\"" + str + "\""
			functionCall.Args[0] = pgQuery.MakeAConstStrNode(str, 0)
		}

		return functionCall
	}

	return nil
}

func (selectRemapper *SelectRemapper) remappedConstantNode(functionCall *pgQuery.FuncCall) *pgQuery.Node {
	functionName := functionCall.Funcname[len(functionCall.Funcname)-1].GetString_().Sval
	constant, ok := REMAPPED_CONSTANT_BY_PG_FUNCTION_NAME[functionName]
	if ok {
		return pgQuery.MakeAConstStrNode(constant, 0)
	}

	return nil
}
