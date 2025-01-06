package main

import (
	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

type QueryParserSelect struct {
	config *Config
	utils  *QueryParserUtils
}

func NewQueryParserSelect(config *Config) *QueryParserSelect {
	return &QueryParserSelect{config: config, utils: NewQueryParserUtils(config)}
}

func (parser *QueryParserSelect) FunctionCall(targetNode *pgQuery.Node) *pgQuery.FuncCall {
	return targetNode.GetResTarget().Val.GetFuncCall()
}

func (parser *QueryParserSelect) InderectionFunctionCall(targetNode *pgQuery.Node) *pgQuery.FuncCall {
	indirection := targetNode.GetResTarget().Val.GetAIndirection()
	if indirection != nil && indirection.Arg.GetFuncCall() != nil {
		return indirection.Arg.GetFuncCall()
	}

	return nil
}

func (parser *QueryParserSelect) NestedFunctionCalls(functionCall *pgQuery.FuncCall) []*pgQuery.FuncCall {
	nestedFunctionCalls := []*pgQuery.FuncCall{}

	for _, arg := range functionCall.Args {
		nestedFunctionCalls = append(nestedFunctionCalls, arg.GetFuncCall())
	}

	return nestedFunctionCalls
}

func (parser *QueryParserSelect) SchemaFunction(functionCall *pgQuery.FuncCall) PgSchemaFunction {
	return parser.utils.SchemaFunction(functionCall)
}

// quote_ident(str) -> concat("\""+str+"\"")
func (parser *QueryParserSelect) RemapQuoteIdentToConcat(functionCall *pgQuery.FuncCall) *pgQuery.FuncCall {
	functionCall.Funcname[0] = pgQuery.MakeStrNode("concat")
	argConstant := functionCall.Args[0].GetAConst()
	if argConstant != nil {
		str := argConstant.GetSval().Sval
		str = "\"" + str + "\""
		functionCall.Args[0] = pgQuery.MakeAConstStrNode(str, 0)
	}

	return functionCall
}

// pg_get_expr(pg_node_tree, relation_oid, pretty_bool) -> pg_get_expr(pg_node_tree, relation_oid)
func (parser *QueryParserSelect) RemoveThirdArgumentFromPgGetExpr(functionCall *pgQuery.FuncCall) *pgQuery.FuncCall {
	if len(functionCall.Args) > 2 {
		functionCall.Args = functionCall.Args[:2]
	}

	return functionCall
}

// set_config(setting_name, new_value, is_local) -> new_value
func (parser *QueryParserSelect) RemapSetConfigFunction(targetNode *pgQuery.Node, functionCall *pgQuery.FuncCall) {
	valueNode := functionCall.Args[1]
	settingName := functionCall.Args[0].GetAConst().GetSval().Sval
	LogWarn(parser.config, "Unsupported set_config", settingName, ":", functionCall)

	parser.OverrideTargetValue(targetNode, valueNode)
	parser.SetDefaultTargetName(targetNode, PG_FUNCTION_SET_CONFIG)
}

// row_to_json() -> to_json()
func (parser *QueryParserSelect) RemapRowToJson(functionCall *pgQuery.FuncCall) *pgQuery.FuncCall {
	functionCall.Funcname = []*pgQuery.Node{pgQuery.MakeStrNode("to_json")}
	return functionCall
}

// information_schema._pg_expandarray(array) -> unnest(anyarray)
func (parser *QueryParserSelect) RemapPgExpandArray(functionCall *pgQuery.FuncCall) *pgQuery.FuncCall {
	functionCall.Funcname = []*pgQuery.Node{pgQuery.MakeStrNode("unnest")}
	return functionCall
}

// (...).n -> func() AS n
func (parser *QueryParserSelect) RemapInderectionToFunctionCall(targetNode *pgQuery.Node, functionCall *pgQuery.FuncCall) *pgQuery.Node {
	targetNode.GetResTarget().Val = &pgQuery.Node{Node: &pgQuery.Node_FuncCall{FuncCall: functionCall}}
	return targetNode
}

// array_to_string() -> main.array_to_string()
func (parser *QueryParserSelect) RemapArrayToString(functionCall *pgQuery.FuncCall) *pgQuery.FuncCall {
	functionCall.Funcname = []*pgQuery.Node{
		pgQuery.MakeStrNode("main"),
		pgQuery.MakeStrNode("array_to_string"),
	}
	return functionCall
}

func (parser *QueryParserSelect) OverrideFunctionCallArg(functionCall *pgQuery.FuncCall, index int, node *pgQuery.Node) {
	functionCall.Args[index] = node
}

func (parser *QueryParserSelect) OverrideTargetValue(targetNode *pgQuery.Node, node *pgQuery.Node) {
	targetNode.GetResTarget().Val = node
}

func (parser *QueryParserSelect) SetDefaultTargetName(targetNode *pgQuery.Node, name string) {
	target := targetNode.GetResTarget()

	if target.Name == "" {
		target.Name = name
	}
}
