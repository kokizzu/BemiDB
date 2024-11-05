package main

import (
	"slices"

	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

var PG_SYSTEM_TABLES = []string{
	"pg_aggregate",
	"pg_am",
	"pg_amop",
	"pg_amproc",
	"pg_attrdef",
	"pg_attribute",
	"pg_auth_members",
	"pg_authid",
	"pg_cast",
	"pg_class",
	"pg_collation",
	"pg_constraint",
	"pg_conversion",
	"pg_database",
	"pg_db_role_setting",
	"pg_default_acl",
	"pg_depend",
	"pg_description",
	"pg_enum",
	"pg_event_trigger",
	"pg_extension",
	"pg_foreign_data_wrapper",
	"pg_foreign_server",
	"pg_foreign_table",
	"pg_index",
	"pg_inherits",
	"pg_init_privs",
	"pg_language",
	"pg_largeobject",
	"pg_largeobject_metadata",
	"pg_namespace",
	"pg_opclass",
	"pg_operator",
	"pg_opfamily",
	"pg_parameter_acl",
	"pg_partitioned_table",
	"pg_policy",
	"pg_proc",
	"pg_publication",
	"pg_publication_namespace",
	"pg_publication_rel",
	"pg_range",
	"pg_replication_origin",
	"pg_rewrite",
	"pg_seclabel",
	"pg_sequence",
	"pg_shdepend",
	"pg_shdescription",
	"pg_shseclabel",
	"pg_statistic",
	"pg_statistic_ext",
	"pg_statistic_ext_data",
	"pg_subscription",
	"pg_subscription_rel",
	"pg_tablespace",
	"pg_transform",
	"pg_trigger",
	"pg_ts_config",
	"pg_ts_config_map",
	"pg_ts_dict",
	"pg_ts_parser",
	"pg_ts_template",
	"pg_type",
	"pg_user_mapping",
}

var PG_SYSTEM_VIEWS = []string{
	"pg_stat_activity",
	"pg_stat_replication",
	"pg_stat_wal_receiver",
	"pg_stat_recovery_prefetch",
	"pg_stat_subscription",
	"pg_stat_ssl",
	"pg_stat_gssapi",
	"pg_stat_progress_analyze",
	"pg_stat_progress_create_index",
	"pg_stat_progress_vacuum",
	"pg_stat_progress_cluster",
	"pg_stat_progress_basebackup",
	"pg_stat_progress_copy",
	"pg_stat_archiver",
	"pg_stat_bgwriter",
	"pg_stat_checkpointer",
	"pg_stat_database",
	"pg_stat_database_conflicts",
	"pg_stat_io",
	"pg_stat_replication_slots",
	"pg_stat_slru",
	"pg_stat_subscription_stats",
	"pg_stat_wal",
	"pg_stat_all_tables",
	"pg_stat_sys_tables",
	"pg_stat_user_tables",
	"pg_stat_xact_all_tables",
	"pg_stat_xact_sys_tables",
	"pg_stat_xact_user_tables",
	"pg_stat_all_indexes",
	"pg_stat_sys_indexes",
	"pg_stat_user_indexes",
	"pg_stat_user_functions",
	"pg_stat_xact_user_functions",
	"pg_statio_all_tables",
	"pg_statio_sys_tables",
	"pg_statio_user_tables",
	"pg_statio_all_indexes",
	"pg_statio_sys_indexes",
	"pg_statio_user_indexes",
	"pg_statio_all_sequences",
	"pg_statio_sys_sequences",
	"pg_statio_user_sequences",
}

var PG_INFORMATION_SCHEMA_TABLES_VALUE_BY_COLUMN = NewOrderedMap([][]string{
	{"table_catalog", "bemidb"},
	{"table_schema", "public"},
	{"table_name", "bemidb_table"},
	{"table_type", "BASE TABLE"},
	{"self_referencing_column_name", "NULL"},
	{"reference_generation", "NULL"},
	{"user_defined_type_catalog", "NULL"},
	{"user_defined_type_schema", "NULL"},
	{"user_defined_type_name", "NULL"},
	{"is_insertable_into", "YES"},
	{"is_typed", "NO"},
	{"commit_action", "NULL"},
})

var PG_STATIO_USER_TABLES_VALUE_BY_COLUMN = NewOrderedMap([][]string{
	{"relid", "0"},
	{"schemaname", "public"},
	{"relname", "bemidb_table"},
	{"heap_blks_read", "0"},
	{"heap_blks_hit", "0"},
	{"idx_blks_read", "0"},
	{"idx_blks_hit", "0"},
	{"toast_blks_read", "0"},
	{"toast_blks_hit", "0"},
	{"tidx_blks_read", "0"},
	{"tidx_blks_hit", "0"},
})

func IsSystemTable(table string) bool {
	return slices.Contains(PG_SYSTEM_TABLES, table) || slices.Contains(PG_SYSTEM_VIEWS, table)
}

func RawSelectColumns(selectStatement *pgQuery.SelectStmt) []string {
	var columns = []string{}
	for _, targetItem := range selectStatement.TargetList {
		var column string
		target := targetItem.GetResTarget()
		if target.Val.GetColumnRef() != nil {
			columnRef := target.Val.GetColumnRef()
			if columnRef.Fields[0].GetAStar() != nil {
				return []string{"*"}
			}
			column = columnRef.Fields[0].GetString_().Sval
			columns = append(columns, column)
		} else if target.Val.GetFuncCall() != nil {
			return []string{"*"} // Don't attempt to detect the used columns in the functions, return all
		}
	}
	return columns
}

// FROM pg_catalog.pg_statio_user_tables: return nothing
func MakePgStatioUserTablesNode() *pgQuery.Node {
	columns := PG_STATIO_USER_TABLES_VALUE_BY_COLUMN.Keys()
	rowValues := PG_STATIO_USER_TABLES_VALUE_BY_COLUMN.Values()

	return makeSubselectNode(columns, [][]string{rowValues})
}

// FROM information_schema.tables: VALUES(values...) t(columns...)
func MakeInformationSchemaTablesNode(database string, schemaAndTables []SchemaTable) *pgQuery.Node {
	columns := PG_INFORMATION_SCHEMA_TABLES_VALUE_BY_COLUMN.Keys()
	staticRowValues := PG_INFORMATION_SCHEMA_TABLES_VALUE_BY_COLUMN.Values()

	var rowsValues [][]string

	for _, schemaTable := range schemaAndTables {
		rowValues := make([]string, len(staticRowValues))
		copy(rowValues, staticRowValues)

		for i, column := range columns {
			switch column {
			case "table_catalog":
				rowValues[i] = database
			case "table_schema":
				rowValues[i] = schemaTable.Schema
			case "table_name":
				rowValues[i] = schemaTable.Table
			}
		}

		rowsValues = append(rowsValues, rowValues)
	}

	return makeSubselectNode(columns, rowsValues)
}

func MakeStringExpressionNode(column string, operation string, value string) *pgQuery.Node {
	return pgQuery.MakeAExprNode(
		pgQuery.A_Expr_Kind_AEXPR_OP,
		[]*pgQuery.Node{pgQuery.MakeStrNode(operation)},
		pgQuery.MakeColumnRefNode([]*pgQuery.Node{pgQuery.MakeStrNode(column)}, 0),
		pgQuery.MakeAConstStrNode(value, 0),
		0,
	)
}

func MakeAConstBoolNode(val bool) *pgQuery.Node {
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

func MakeStatementNode(targetList []*pgQuery.Node) *pgQuery.Node {
	return &pgQuery.Node{
		Node: &pgQuery.Node_SelectStmt{
			SelectStmt: &pgQuery.SelectStmt{
				TargetList: targetList,
			},
		},
	}
}

func makeSubselectNode(columns []string, rowsValues [][]string) *pgQuery.Node {
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
