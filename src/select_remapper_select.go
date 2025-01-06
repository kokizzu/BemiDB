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
	"current_setting":                    "",
	"aclexplode":                         "",
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
	newTargetNode := remapper.remappedInderectionFunctionCall(targetNode)
	if newTargetNode != nil {
		return newTargetNode
	}

	functionCall := remapper.parserSelect.FunctionCall(targetNode)
	if functionCall == nil {
		return targetNode
	}

	schemaFunction := remapper.parserSelect.SchemaFunction(functionCall)

	// set_config(setting_name, new_value, is_local) -> new_value
	if schemaFunction.Function == PG_FUNCTION_SET_CONFIG {
		remapper.parserSelect.RemapSetConfigFunction(targetNode, functionCall)
		return targetNode
	}

	renamedNameFunction := remapper.remappedFunctionName(functionCall)
	if renamedNameFunction != nil {
		functionCall = renamedNameFunction
		remapper.parserSelect.SetDefaultTargetName(targetNode, schemaFunction.Function)
	}

	remappedArgsFunction := remapper.remappedFunctionArgs(functionCall)
	if remappedArgsFunction != nil {
		functionCall = remappedArgsFunction
	}

	constantNode := remapper.remappedToConstant(functionCall)
	if constantNode != nil {
		remapper.parserSelect.OverrideTargetValue(targetNode, constantNode)
		remapper.parserSelect.SetDefaultTargetName(targetNode, schemaFunction.Function)
	}

	functionCall = remapper.remapNestedFunctionCalls(functionCall) // recursive

	return targetNode
}

func (remapper *SelectRemapperSelect) remappedInderectionFunctionCall(targetNode *pgQuery.Node) *pgQuery.Node {
	parser := remapper.parserSelect

	functionCall := parser.InderectionFunctionCall(targetNode)
	if functionCall == nil {
		return nil
	}

	schemaFunction := parser.SchemaFunction(functionCall)

	switch {

	// (information_schema._pg_expandarray(array)).n -> unnest(anyarray) AS n
	case schemaFunction.Schema == PG_SCHEMA_INFORMATION_SCHEMA && schemaFunction.Function == PG_FUNCTION_PG_EXPANDARRAY:
		inderectionColumnName := targetNode.GetResTarget().Val.GetAIndirection().Indirection[0].GetString_().Sval
		newTargetNode := parser.RemapInderectionToFunctionCall(targetNode, parser.RemapPgExpandArray(functionCall))
		remapper.parserSelect.SetDefaultTargetName(newTargetNode, inderectionColumnName)
		return newTargetNode

	default:
		return nil
	}
}

func (remapper *SelectRemapperSelect) remappedFunctionName(functionCall *pgQuery.FuncCall) *pgQuery.FuncCall {
	schemaFunction := remapper.parserSelect.SchemaFunction(functionCall)

	switch {

	// quote_ident(str) -> concat("\""+str+"\"")
	case schemaFunction.Function == PG_FUNCTION_QUOTE_INDENT:
		return remapper.parserSelect.RemapQuoteIdentToConcat(functionCall)

	// array_to_string(array, separator) -> main.array_to_string(array, separator)
	case schemaFunction.Function == PG_FUNCTION_ARRAY_TO_STRING:
		return remapper.parserSelect.RemapArrayToString(functionCall)

	// row_to_json(col) -> to_json(col)
	case schemaFunction.Function == PG_FUNCTION_ROW_TO_JSON:
		return remapper.parserSelect.RemapRowToJson(functionCall)

	default:
		return nil
	}
}

func (remapper *SelectRemapperSelect) remappedFunctionArgs(functionCall *pgQuery.FuncCall) *pgQuery.FuncCall {
	schemaFunction := remapper.parserSelect.SchemaFunction(functionCall)

	// pg_get_expr(pg_node_tree, relation_oid, pretty_bool) -> pg_get_expr(pg_node_tree, relation_oid)
	if schemaFunction.Schema == PG_SCHEMA_PG_CATALOG && schemaFunction.Function == PG_FUNCTION_PG_GET_EXPR {
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
	schemaFunction := remapper.parserSelect.SchemaFunction(functionCall)
	constant, ok := REMAPPED_CONSTANT_BY_PG_FUNCTION_NAME[schemaFunction.Function]
	if ok {
		return pgQuery.MakeAConstStrNode(constant, 0)
	}

	return nil
}
