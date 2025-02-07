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

// value::type or CAST(value AS type)
func (remapper *QueryRemapperTypeCast) RemapTypeCast(node *pgQuery.Node) *pgQuery.Node {
	typeCast := remapper.parserTypeCast.TypeCast(node)
	if typeCast == nil {
		return node
	}

	typeName := remapper.parserTypeCast.TypeName(typeCast)
	switch typeName {
	case "regclass":
		// 'schema.table'::regclass -> 'schema.table'
		return typeCast.Arg
	case "text":
		// '{a,b,c}'::text[] -> ARRAY['a', 'b', 'c']
		return remapper.parserTypeCast.MakeListValueFromArray(typeCast.Arg)
	case "regproc":
		// 'schema.function_name'::regproc -> 'function_name'
		functionNameParts := strings.Split(remapper.parserTypeCast.ArgStringValue(typeCast), ".")
		return pgQuery.MakeAConstStrNode(functionNameParts[len(functionNameParts)-1], 0)
	case "oid":
		// 'schema.table'::regclass::oid -> SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'schema' AND c.relname = 'table'
		nestedNode := typeCast.Arg
		nestedTypeCast := remapper.parserTypeCast.TypeCast(nestedNode)
		if nestedTypeCast == nil {
			return node
		}
		nestedTypeName := remapper.parserTypeCast.TypeName(nestedTypeCast)
		if nestedTypeName != "regclass" {
			return node
		}

		return remapper.parserTypeCast.MakeSubselectOidBySchemaTable(nestedTypeCast.Arg)
	}

	return node
}
