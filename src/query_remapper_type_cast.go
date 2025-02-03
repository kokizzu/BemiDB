package main

import (
	"strings"

	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

type QueryRemapperTypeCast struct {
	parserTypeCast *ParserTypeCast
	config         *Config
}

func NewQueryRemapperTypeCast(config *Config) *QueryRemapperTypeCast {
	remapper := &QueryRemapperTypeCast{
		parserTypeCast: NewParserTypeCast(config),
		config:         config,
	}
	return remapper
}

// value::type -> value
func (remapper *QueryRemapperTypeCast) RemapTypeCast(node *pgQuery.Node) *pgQuery.Node {
	typeCast := remapper.parserTypeCast.TypeCast(node)
	if typeCast == nil {
		return node
	}

	typeName := remapper.parserTypeCast.TypeName(typeCast)
	switch typeName {
	case "regclass":
		return typeCast.Arg
	case "text":
		return remapper.parserTypeCast.MakeListValueFromArray(typeCast.Arg)
	case "regproc":
		functionNameParts := strings.Split(remapper.parserTypeCast.ArgStringValue(typeCast), ".") // pg_catalog.func_name
		return pgQuery.MakeAConstStrNode(functionNameParts[len(functionNameParts)-1], 0)
	}

	return node
}
