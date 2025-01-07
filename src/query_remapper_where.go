package main

import (
	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

type QueryRemapperWhere struct {
	parserWhere    *ParserWhere
	parserFunction *ParserFunction
	config         *Config
}

func NewQueryRemapperWhere(config *Config) *QueryRemapperWhere {
	return &QueryRemapperWhere{
		parserWhere:    NewParserWhere(config),
		parserFunction: NewParserFunction(config),
		config:         config,
	}
}

func (remapper *QueryRemapperWhere) RemapWhereExpressions(selectStatement *pgQuery.SelectStmt, node *pgQuery.Node, indentLevel int) *pgQuery.SelectStmt {
	if aExpr := node.GetAExpr(); aExpr != nil {
		if aExpr.Lexpr != nil {
			selectStatement = remapper.RemapWhereExpressions(selectStatement, aExpr.Lexpr, indentLevel) // self-recursion
		}
		if aExpr.Rexpr != nil {
			selectStatement = remapper.RemapWhereExpressions(selectStatement, aExpr.Rexpr, indentLevel) // self-recursion
		}
	}

	functionCall := remapper.parserWhere.FunctionCall(node)
	if functionCall == nil {
		return selectStatement
	}

	constantNode := remapper.parserFunction.RemapToConstant(functionCall)
	if constantNode == nil {
		return selectStatement
	}

	node.Node = constantNode.Node
	return selectStatement
}
