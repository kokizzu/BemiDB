package main

import (
	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

type ParserSelect struct {
	config *Config
}

func NewParserSelect(config *Config) *ParserSelect {
	return &ParserSelect{config: config}
}

func (parser *ParserSelect) OverrideTargetValue(targetNode *pgQuery.Node, node *pgQuery.Node) {
	targetNode.GetResTarget().Val = node
}

func (parser *ParserSelect) SetDefaultTargetName(targetNode *pgQuery.Node, name string) {
	target := targetNode.GetResTarget()

	if target.Name == "" {
		target.Name = name
	}
}
