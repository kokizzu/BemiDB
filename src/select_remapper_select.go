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
	target := targetNode.GetResTarget()

	if target.Val.GetFuncCall() != nil {
		functionCall := target.Val.GetFuncCall()
		originalFunctionName := functionCall.Funcname[len(functionCall.Funcname)-1].GetString_().Sval

		renamedFunctionCall := remapper.remappedFunctionName(functionCall)
		if renamedFunctionCall != nil {
			functionCall = renamedFunctionCall
			if target.Name == "" {
				target.Name = originalFunctionName
			}
		}

		constantNode := remapper.remappedConstantNode(functionCall)
		if constantNode != nil {
			target.Val = constantNode
			if target.Name == "" {
				target.Name = originalFunctionName
			}
		}

		functionCall = remapper.remapFunctionCallArgs(functionCall)
	}

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
	functionName := functionCall.Funcname[len(functionCall.Funcname)-1].GetString_().Sval

	if remapper.parserSelect.IsQuoteIdentFunction(functionName) {
		functionCall.Funcname[0] = pgQuery.MakeStrNode("concat")
		argConstant := functionCall.Args[0].GetAConst()
		if argConstant != nil {
			str := argConstant.GetSval().Sval
			str = "\"" + str + "\""
			functionCall.Args[0] = pgQuery.MakeAConstStrNode(str, 0)
		}

		return functionCall
	}

	return nil
}

func (remapper *SelectRemapperSelect) remapFunctionCallArgs(functionCall *pgQuery.FuncCall) *pgQuery.FuncCall {
	for i, arg := range functionCall.Args {
		if arg.GetFuncCall() != nil {
			argFunctionCall := arg.GetFuncCall()

			renamedFunctionCall := remapper.remappedFunctionName(argFunctionCall)
			if renamedFunctionCall != nil {
				argFunctionCall = renamedFunctionCall
			}

			constantNode := remapper.remappedConstantNode(argFunctionCall)
			if constantNode != nil {
				functionCall.Args[i] = constantNode
			}
			argFunctionCall = remapper.remapFunctionCallArgs(argFunctionCall)
		}
	}

	return functionCall
}

func (remapper *SelectRemapperSelect) remappedConstantNode(functionCall *pgQuery.FuncCall) *pgQuery.Node {
	functionName := functionCall.Funcname[len(functionCall.Funcname)-1].GetString_().Sval
	constant, ok := REMAPPED_CONSTANT_BY_PG_FUNCTION_NAME[functionName]
	if ok {
		return pgQuery.MakeAConstStrNode(constant, 0)
	}

	return nil
}
