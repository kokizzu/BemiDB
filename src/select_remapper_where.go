package main

import (
	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

type SelectRemapperWhere struct {
	parserWhere *QueryParserWhere
	parserTable *QueryParserTable
	config      *Config
}

func NewSelectRemapperWhere(config *Config) *SelectRemapperWhere {
	return &SelectRemapperWhere{
		parserWhere: NewQueryParserWhere(config),
		parserTable: NewQueryParserTable(config),
		config:      config,
	}
}

// WHERE [CONDITION]
func (remapper *SelectRemapperWhere) RemapWhere(qSchemaTable QuerySchemaTable, selectStatement *pgQuery.SelectStmt) *pgQuery.SelectStmt {
	// FROM pg_catalog.pg_namespace -> FROM pg_catalog.pg_namespace WHERE nspname != 'main'
	if remapper.parserTable.IsPgNamespaceTable(qSchemaTable) {
		withoutMainSchemaWhereCondition := remapper.parserWhere.MakeExpressionNode("nspname", "!=", "main")
		return remapper.appendWhereCondition(selectStatement, withoutMainSchemaWhereCondition)
	}

	// FROM pg_catalog.pg_statio_user_tables -> FROM pg_catalog.pg_statio_user_tables WHERE false
	if remapper.parserTable.IsPgStatioUserTablesTable(qSchemaTable) {
		falseWhereCondition := remapper.parserWhere.MakeFalseConditionNode()
		selectStatement = remapper.overrideWhereCondition(selectStatement, falseWhereCondition)
		return selectStatement
	}

	return selectStatement
}

func (remapper *SelectRemapperWhere) appendWhereCondition(selectStatement *pgQuery.SelectStmt, whereCondition *pgQuery.Node) *pgQuery.SelectStmt {
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

func (remapper *SelectRemapperWhere) overrideWhereCondition(selectStatement *pgQuery.SelectStmt, whereCondition *pgQuery.Node) *pgQuery.SelectStmt {
	selectStatement.WhereClause = whereCondition
	return selectStatement
}
