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
		return remapper.parserWhere.AppendWhereCondition(selectStatement, withoutMainSchemaWhereCondition)
	}

	// FROM pg_catalog.pg_statio_user_tables -> FROM pg_catalog.pg_statio_user_tables WHERE false
	if remapper.parserTable.IsPgStatioUserTablesTable(qSchemaTable) {
		falseWhereCondition := remapper.parserWhere.MakeFalseConditionNode()
		selectStatement = remapper.parserWhere.OverrideWhereCondition(selectStatement, falseWhereCondition)
		return selectStatement
	}

	return selectStatement
}
