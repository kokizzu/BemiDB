package main

import (
	"strings"

	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

type ParserTypeCast struct {
	utils  *ParserUtils
	config *Config
}

func NewParserTypeCast(config *Config) *ParserTypeCast {
	return &ParserTypeCast{utils: NewParserUtils(config), config: config}
}

func (parser *ParserTypeCast) TypeCast(node *pgQuery.Node) *pgQuery.TypeCast {
	if node.GetTypeCast() == nil {
		return nil
	}

	typeCast := node.GetTypeCast()
	if len(typeCast.TypeName.Names) == 0 {
		return nil
	}

	return typeCast
}

func (parser *ParserTypeCast) TypeName(typeCast *pgQuery.TypeCast) string {
	return typeCast.TypeName.Names[0].GetString_().Sval
}

func (parser *ParserTypeCast) ArgStringValue(typeCast *pgQuery.TypeCast) string {
	return typeCast.Arg.GetAConst().GetSval().Sval
}

func (parser *ParserTypeCast) MakeCaseTypeCastNode(arg *pgQuery.Node, typeName string) *pgQuery.Node {
	if existingType := parser.inferNodeType(arg); existingType == typeName {
		return arg
	}
	return parser.utils.MakeTypeCastNode(arg, typeName)
}

func (parser *ParserTypeCast) MakeListValueFromArray(node *pgQuery.Node) *pgQuery.Node {
	arrayStr := node.GetAConst().GetSval().Sval
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

func (parser *ParserTypeCast) inferNodeType(node *pgQuery.Node) string {
	if typeCast := node.GetTypeCast(); typeCast != nil {
		return typeCast.TypeName.Names[0].GetString_().Sval
	}

	if aConst := node.GetAConst(); aConst != nil {
		switch {
		case aConst.GetBoolval() != nil:
			return "boolean"
		case aConst.GetIval() != nil:
			return "int8"
		case aConst.GetSval() != nil:
			return "text"
		}
	}
	return ""
}
