package main

import (
	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

type QueryUtils struct {
	config *Config
}

func NewQueryUtils(config *Config) *QueryUtils {
	return &QueryUtils{config: config}
}

func (utils *QueryUtils) MakeSubselectNode(columns []string, rowsValues [][]string) *pgQuery.Node {
	var columnNodes []*pgQuery.Node
	for _, column := range columns {
		columnNodes = append(columnNodes, pgQuery.MakeStrNode(column))
	}

	var rowsValuesNodes []*pgQuery.Node
	for _, rowValues := range rowsValues {
		var rowValuesNodes []*pgQuery.Node
		for _, value := range rowValues {
			rowValuesNodes = append(rowValuesNodes, pgQuery.MakeAConstStrNode(value, 0))
		}

		rowsValuesNodes = append(rowsValuesNodes, pgQuery.MakeListNode(rowValuesNodes))
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
					Aliasname: "t",
					Colnames:  columnNodes,
				},
			},
		},
	}
}

func (utils *QueryUtils) MakeAConstBoolNode(val bool) *pgQuery.Node {
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
