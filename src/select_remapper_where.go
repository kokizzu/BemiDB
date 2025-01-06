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

func (remapper *SelectRemapperWhere) RemapWhereExpressions(selectStatement *pgQuery.SelectStmt, node *pgQuery.Node, indentLevel int) *pgQuery.SelectStmt {
	if aExpr := node.GetAExpr(); aExpr != nil {
		if aExpr.Lexpr != nil {
			selectStatement = remapper.RemapWhereExpressions(selectStatement, aExpr.Lexpr, indentLevel)
		}
		if aExpr.Rexpr != nil {
			selectStatement = remapper.RemapWhereExpressions(selectStatement, aExpr.Rexpr, indentLevel)
		}
	}

	if funcCall := node.GetFuncCall(); funcCall != nil {
		functionName := funcCall.Funcname[0].GetString_().Sval
		if constant, ok := REMAPPED_CONSTANT_BY_PG_FUNCTION_NAME[functionName]; ok {
			node.Node = pgQuery.MakeAConstStrNode(constant, 0).Node
		}
	}

	return selectStatement
}
