package main

import (
	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

type QueryParserWhere struct {
	config *Config
	utils  *QueryParserUtils
}

func NewQueryParserWhere(config *Config) *QueryParserWhere {
	return &QueryParserWhere{config: config, utils: NewQueryParserUtils(config)}
}

// column != 'value'
func (parser *QueryParserWhere) MakeExpressionNode(column string, operation string, value string) *pgQuery.Node {
	return pgQuery.MakeAExprNode(
		pgQuery.A_Expr_Kind_AEXPR_OP,
		[]*pgQuery.Node{pgQuery.MakeStrNode(operation)},
		pgQuery.MakeColumnRefNode([]*pgQuery.Node{pgQuery.MakeStrNode(column)}, 0),
		pgQuery.MakeAConstStrNode(value, 0),
		0,
	)
}

func (parser *QueryParserWhere) MakeFalseConditionNode() *pgQuery.Node {
	return parser.utils.MakeAConstBoolNode(false)
}
