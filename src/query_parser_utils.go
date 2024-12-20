package main

import (
	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

type QueryParserUtils struct {
	config *Config
}

func NewQueryParserUtils(config *Config) *QueryParserUtils {
	return &QueryParserUtils{config: config}
}

func (utils *QueryParserUtils) MakeSubselectWithRowsNode(tableName string, columns []string, rowsValues [][]string, alias string) *pgQuery.Node {
	columnNodes := make([]*pgQuery.Node, len(columns))
	for i, column := range columns {
		columnNodes[i] = pgQuery.MakeStrNode(column)
	}

	rowsValuesNodes := make([]*pgQuery.Node, len(rowsValues))
	for rowsIndex, rowValues := range rowsValues {
		rowValuesNodes := make([]*pgQuery.Node, len(rowValues))
		for rowIndex, value := range rowValues {
			rowValuesNodes[rowIndex] = pgQuery.MakeAConstStrNode(value, 0)
		}
		rowsValuesNodes[rowsIndex] = pgQuery.MakeListNode(rowValuesNodes)
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
							ValuesLists: rowsValuesNodes,
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

func (utils *QueryParserUtils) MakeSubselectWithoutRowsNode(tableName string, columns []string, alias string) *pgQuery.Node {
	columnNodes := make([]*pgQuery.Node, len(columns))
	for i, column := range columns {
		columnNodes[i] = pgQuery.MakeStrNode(column)
	}

	targetList := make([]*pgQuery.Node, len(columns))
	for i, _ := range columns {
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

func (utils *QueryParserUtils) MakeSubselectFromNode(tableName string, targetList []*pgQuery.Node, fromNode *pgQuery.Node, alias string) *pgQuery.Node {
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

func (utils *QueryParserUtils) MakeAConstBoolNode(val bool) *pgQuery.Node {
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
