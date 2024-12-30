package main

import (
	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

var REMAPPED_CONSTANT_BY_PG_FUNCTION_NAME = map[string]string{
	"version":                            "PostgreSQL " + PG_VERSION + ", compiled by Bemi",
	"pg_get_userbyid":                    "bemidb",
	"pg_get_function_identity_arguments": "",
	"pg_total_relation_size":             "0",
	"pg_table_size":                      "0",
	"pg_indexes_size":                    "0",
	"pg_get_partkeydef":                  "",
	"pg_tablespace_location":             "",
	"pg_encoding_to_char":                "UTF8",
	"pg_backend_pid":                     "0",
	"pg_is_in_recovery":                  "f",
}

type SelectRemapperSelect struct {
	parserSelect *QueryParserSelect
	config       *Config
}

func NewSelectRemapperSelect(config *Config) *SelectRemapperSelect {
	return &SelectRemapperSelect{
		parserSelect: NewQueryParserSelect(config),
		config:       config,
	}
}

// SELECT [PG_FUNCTION()]
func (remapper *SelectRemapperSelect) RemapSelect(targetNode *pgQuery.Node) *pgQuery.Node {
	functionCall := remapper.parserSelect.FunctionCall(targetNode)
	if functionCall == nil {
		return targetNode
	}

	originalFunctionName := remapper.parserSelect.FunctionName(functionCall)

	if remapper.parserSelect.IsSetConfigFunction(originalFunctionName) {
		remapper.parserSelect.RemapSetConfigFunction(targetNode, functionCall)
		return targetNode
	}

	renamedNameFunction := remapper.remappedFunctionName(functionCall)
	if renamedNameFunction != nil {
		functionCall = renamedNameFunction
		remapper.parserSelect.SetDefaultTargetName(targetNode, originalFunctionName)
	}

	remappedArgsFunction := remapper.remappedFunctionArgs(functionCall)
	if remappedArgsFunction != nil {
		functionCall = remappedArgsFunction
	}

	constantNode := remapper.remappedToConstant(functionCall)
	if constantNode != nil {
		remapper.parserSelect.OverrideTargetValue(targetNode, constantNode)
		remapper.parserSelect.SetDefaultTargetName(targetNode, originalFunctionName)
	}

	functionCall = remapper.remapNestedFunctionCalls(functionCall) // recursive

	return targetNode
}

func (remapper *SelectRemapperSelect) SubselectStatement(targetNode *pgQuery.Node) *pgQuery.SelectStmt {
	target := targetNode.GetResTarget()

	if target.Val.GetSubLink() != nil {
		return target.Val.GetSubLink().Subselect.GetSelectStmt()
	}

	return nil
}

func (remapper *SelectRemapperSelect) remappedFunctionName(functionCall *pgQuery.FuncCall) *pgQuery.FuncCall {
	functionName := remapper.parserSelect.FunctionName(functionCall)

	if remapper.parserSelect.IsRowToJsonFunction(functionName) {
		return remapper.parserSelect.RemapRowToJson(functionCall)
	}

	// quote_ident(str) -> concat("\""+str+"\"")
	if remapper.parserSelect.IsQuoteIdentFunction(functionName) {
		return remapper.parserSelect.RemapQuoteIdentToConcat(functionCall)
	}

	return nil
}

func (remapper *SelectRemapperSelect) remappedFunctionArgs(functionCall *pgQuery.FuncCall) *pgQuery.FuncCall {
	functionName := remapper.parserSelect.FunctionName(functionCall)

	// pg_get_expr(pg_node_tree, relation_oid, pretty_bool) -> pg_get_expr(pg_node_tree, relation_oid)
	if remapper.parserSelect.IsPgGetExprFunction(functionName) {
		return remapper.parserSelect.RemoveThirdArgumentFromPgGetExpr(functionCall)
	}

	return nil
}

func (remapper *SelectRemapperSelect) remapNestedFunctionCalls(functionCall *pgQuery.FuncCall) *pgQuery.FuncCall {
	nestedFunctionCalls := remapper.parserSelect.NestedFunctionCalls(functionCall)

	for i, nestedFunctionCall := range nestedFunctionCalls {
		if nestedFunctionCall == nil {
			continue
		}

		renamedFunctionCall := remapper.remappedFunctionName(nestedFunctionCall)
		if renamedFunctionCall != nil {
			nestedFunctionCall = renamedFunctionCall
		}

		constantNode := remapper.remappedToConstant(nestedFunctionCall)
		if constantNode != nil {
			remapper.parserSelect.OverrideFunctionCallArg(functionCall, i, constantNode)
		}

		nestedFunctionCall = remapper.remapNestedFunctionCalls(nestedFunctionCall)
	}

	return functionCall
}

func (remapper *SelectRemapperSelect) remappedToConstant(functionCall *pgQuery.FuncCall) *pgQuery.Node {
	functionName := remapper.parserSelect.FunctionName(functionCall)
	constant, ok := REMAPPED_CONSTANT_BY_PG_FUNCTION_NAME[functionName]
	if ok {
		return pgQuery.MakeAConstStrNode(constant, 0)
	}

	return nil
}
