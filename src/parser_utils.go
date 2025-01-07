package main

import (
	"strconv"
	"strings"

	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

type ParserUtils struct {
	config *Config
}

func NewParserUtils(config *Config) *ParserUtils {
	return &ParserUtils{config: config}
}

func (utils *ParserUtils) SchemaFunction(functionCall *pgQuery.FuncCall) PgSchemaFunction {
	switch len(functionCall.Funcname) {
	case 1:
		return PgSchemaFunction{
			Schema:   "",
			Function: functionCall.Funcname[0].GetString_().Sval,
		}
	case 2:
		return PgSchemaFunction{
			Schema:   functionCall.Funcname[0].GetString_().Sval,
			Function: functionCall.Funcname[1].GetString_().Sval,
		}
	default:
		panic("Invalid function call")
	}
}

func (utils *ParserUtils) MakeSubselectWithRowsNode(tableName string, columns []string, rowsValues [][]string, alias string) *pgQuery.Node {
	parserType := NewParserType(utils.config)

	columnNodes := make([]*pgQuery.Node, len(columns))
	for i, column := range columns {
		columnNodes[i] = pgQuery.MakeStrNode(column)
	}

	selectStmt := &pgQuery.SelectStmt{}

	for _, row := range rowsValues {
		var rowList []*pgQuery.Node
		for _, val := range row {
			if val == "NULL" {
				constNode := &pgQuery.Node{
					Node: &pgQuery.Node_AConst{
						AConst: &pgQuery.A_Const{
							Isnull: true,
						},
					},
				}
				rowList = append(rowList, constNode)
			} else {
				constNode := pgQuery.MakeAConstStrNode(val, 0)
				if _, err := strconv.ParseInt(val, 10, 64); err == nil {
					constNode = parserType.MakeCaseTypeCastNode(constNode, "int8")
				} else {
					valLower := strings.ToLower(val)
					if valLower == "true" || valLower == "false" {
						constNode = parserType.MakeCaseTypeCastNode(constNode, "bool")
					}
				}
				rowList = append(rowList, constNode)
			}
		}
		selectStmt.ValuesLists = append(selectStmt.ValuesLists,
			&pgQuery.Node{Node: &pgQuery.Node_List{List: &pgQuery.List{Items: rowList}}})
	}

	if alias == "" {
		alias = tableName
	}

	return &pgQuery.Node{
		Node: &pgQuery.Node_RangeSubselect{
			RangeSubselect: &pgQuery.RangeSubselect{
				Subquery: &pgQuery.Node{
					Node: &pgQuery.Node_SelectStmt{
						SelectStmt: selectStmt,
					},
				},
				Alias: &pgQuery.Alias{
					Aliasname: alias,
					Colnames:  columnNodes,
				},
			},
		},
	}
}

func (utils *ParserUtils) MakeSubselectWithoutRowsNode(tableName string, columns []string, alias string) *pgQuery.Node {
	columnNodes := make([]*pgQuery.Node, len(columns))
	for i, column := range columns {
		columnNodes[i] = pgQuery.MakeStrNode(column)
	}

	targetList := make([]*pgQuery.Node, len(columns))
	for i := range columns {
		targetList[i] = pgQuery.MakeResTargetNodeWithVal(
			utils.MakeAConstBoolNode(false),
			0,
		)
	}

	if alias == "" {
		alias = tableName
	}

	return &pgQuery.Node{
		Node: &pgQuery.Node_RangeSubselect{
			RangeSubselect: &pgQuery.RangeSubselect{
				Subquery: &pgQuery.Node{
					Node: &pgQuery.Node_SelectStmt{
						SelectStmt: &pgQuery.SelectStmt{
							TargetList:  targetList,
							WhereClause: utils.MakeAConstBoolNode(false),
						},
					},
				},
				Alias: &pgQuery.Alias{
					Aliasname: alias,
					Colnames:  columnNodes,
				},
			},
		},
	}
}

func (utils *ParserUtils) MakeSubselectFromNode(tableName string, targetList []*pgQuery.Node, fromNode *pgQuery.Node, alias string) *pgQuery.Node {
	if alias == "" {
		alias = tableName
	}

	return &pgQuery.Node{
		Node: &pgQuery.Node_RangeSubselect{
			RangeSubselect: &pgQuery.RangeSubselect{
				Subquery: &pgQuery.Node{
					Node: &pgQuery.Node_SelectStmt{
						SelectStmt: &pgQuery.SelectStmt{
							TargetList: targetList,
							FromClause: []*pgQuery.Node{fromNode},
						},
					},
				},
				Alias: &pgQuery.Alias{
					Aliasname: alias,
				},
			},
		},
	}
}

func (utils *ParserUtils) MakeAConstBoolNode(val bool) *pgQuery.Node {
	return &pgQuery.Node{
		Node: &pgQuery.Node_AConst{
			AConst: &pgQuery.A_Const{
				Val: &pgQuery.A_Const_Boolval{
					Boolval: &pgQuery.Boolean{
						Boolval: val,
					},
				},
				Isnull:   false,
				Location: 0,
			},
		},
	}
}
