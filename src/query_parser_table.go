package main

import (
	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

type QueryParserTable struct {
	config *Config
	utils  *QueryParserUtils
}

func NewQueryParserTable(config *Config) *QueryParserTable {
	return &QueryParserTable{config: config, utils: NewQueryParserUtils(config)}
}

func (parser *QueryParserTable) NodeToQuerySchemaTable(node *pgQuery.Node) QuerySchemaTable {
	rangeVar := node.GetRangeVar()
	var alias string

	if rangeVar.Alias != nil {
		alias = rangeVar.Alias.Aliasname
	}

	return QuerySchemaTable{
		Schema: rangeVar.Schemaname,
		Table:  rangeVar.Relname,
		Alias:  alias,
	}
}

func (parser *QueryParserTable) MakeEmptyTableNode(tableName string, columns []string, alias string) *pgQuery.Node {
	return parser.utils.MakeSubselectWithoutRowsNode(tableName, columns, alias)
}

// pg_catalog.pg_shadow -> VALUES(values...) t(columns...)
func (parser *QueryParserTable) MakePgShadowNode(user string, encryptedPassword string, alias string) *pgQuery.Node {
	columns := PG_SHADOW_VALUE_BY_COLUMN.Keys()
	staticRowValues := PG_SHADOW_VALUE_BY_COLUMN.Values()

	var rowsValues [][]string

	rowValues := make([]string, len(staticRowValues))
	copy(rowValues, staticRowValues)
	for i, column := range columns {
		switch column {
		case "usename":
			rowValues[i] = user
		case "passwd":
			rowValues[i] = encryptedPassword
		}
	}
	rowsValues = append(rowsValues, rowValues)

	return parser.utils.MakeSubselectWithRowsNode(PG_TABLE_PG_SHADOW, columns, rowsValues, alias)
}

// pg_catalog.pg_roles -> VALUES(values...) t(columns...)
func (parser *QueryParserTable) MakePgRolesNode(user string, alias string) *pgQuery.Node {
	columns := PG_ROLES_VALUE_BY_COLUMN.Keys()
	staticRowValues := PG_ROLES_VALUE_BY_COLUMN.Values()

	var rowsValues [][]string
	rowValues := make([]string, len(staticRowValues))
	copy(rowValues, staticRowValues)

	for i, column := range columns {
		if column == "rolname" {
			rowValues[i] = user
		}
	}
	rowsValues = append(rowsValues, rowValues)

	return parser.utils.MakeSubselectWithRowsNode(PG_TABLE_PG_ROLES, columns, rowsValues, alias)
}

// pg_catalog.pg_extension -> VALUES(values...) t(columns...)
func (parser *QueryParserTable) MakePgExtensionNode(alias string) *pgQuery.Node {
	columns := PG_EXTENSION_VALUE_BY_COLUMN.Keys()
	staticRowValues := PG_EXTENSION_VALUE_BY_COLUMN.Values()
	rowsValues := [][]string{staticRowValues}
	return parser.utils.MakeSubselectWithRowsNode(PG_TABLE_PG_EXTENSION, columns, rowsValues, alias)
}

// pg_catalog.pg_database -> VALUES(values...) t(columns...)
func (parser *QueryParserTable) MakePgDatabaseNode(database string, alias string) *pgQuery.Node {
	columns := PG_DATABASE_VALUE_BY_COLUMN.Keys()
	staticRowValues := PG_DATABASE_VALUE_BY_COLUMN.Values()

	var rowsValues [][]string
	rowValues := make([]string, len(staticRowValues))
	copy(rowValues, staticRowValues)
	for i, column := range columns {
		if column == "datname" {
			rowValues[i] = database
		}
	}
	rowsValues = append(rowsValues, rowValues)

	return parser.utils.MakeSubselectWithRowsNode(PG_TABLE_PG_DATABASE, columns, rowsValues, alias)
}

// pg_catalog.pg_user -> VALUES(values...) t(columns...)
func (parser *QueryParserTable) MakePgUserNode(user string, alias string) *pgQuery.Node {
	columns := PG_USER_VALUE_BY_COLUMN.Keys()
	rowValues := PG_USER_VALUE_BY_COLUMN.Values()

	rowValues[0] = user

	return parser.utils.MakeSubselectWithRowsNode(PG_TABLE_PG_USER, columns, [][]string{rowValues}, alias)
}

// pg_catalog.pg_stat_user_tables -> VALUES(values...) t(columns...)
func (parser *QueryParserTable) MakePgStatUserTablesNode(schemaTables []IcebergSchemaTable, alias string) *pgQuery.Node {
	columns := PG_STAT_USER_TABLES_VALUE_BY_COLUMN.Keys()
	staticRowValues := PG_STAT_USER_TABLES_VALUE_BY_COLUMN.Values()

	var rowsValues [][]string

	for _, schemaTable := range schemaTables {
		rowValues := make([]string, len(staticRowValues))
		copy(rowValues, staticRowValues)
		for i, column := range columns {
			switch column {
			case "schemaname":
				rowValues[i] = schemaTable.Schema
			case "relname":
				rowValues[i] = schemaTable.Table
			}
		}
		rowsValues = append(rowsValues, rowValues)
	}

	return parser.utils.MakeSubselectWithRowsNode(PG_TABLE_PG_STAT_USER_TABLES, columns, rowsValues, alias)
}

// Other information_schema.* tables
func (parser *QueryParserTable) IsTableFromInformationSchema(qSchemaTable QuerySchemaTable) bool {
	return qSchemaTable.Schema == PG_SCHEMA_INFORMATION_SCHEMA
}

// iceberg.table -> FROM iceberg_scan('path', skip_schema_inference = true)
func (parser *QueryParserTable) MakeIcebergTableNode(tablePath string, qSchemaTable QuerySchemaTable) *pgQuery.Node {
	node := pgQuery.MakeSimpleRangeFunctionNode([]*pgQuery.Node{
		pgQuery.MakeListNode([]*pgQuery.Node{
			pgQuery.MakeFuncCallNode(
				[]*pgQuery.Node{
					pgQuery.MakeStrNode("iceberg_scan"),
				},
				[]*pgQuery.Node{
					pgQuery.MakeAConstStrNode(
						tablePath,
						0,
					),
					pgQuery.MakeAExprNode(
						pgQuery.A_Expr_Kind_AEXPR_OP,
						[]*pgQuery.Node{pgQuery.MakeStrNode("=")},
						pgQuery.MakeColumnRefNode([]*pgQuery.Node{pgQuery.MakeStrNode("skip_schema_inference")}, 0),
						parser.utils.MakeAConstBoolNode(true),
						0,
					),
				},
				0,
			),
		}),
	})

	// DuckDB doesn't support aliases on iceberg_scan() functions, so we need to wrap it in a nested select that can have an alias
	selectStarNode := pgQuery.MakeResTargetNodeWithVal(
		pgQuery.MakeColumnRefNode(
			[]*pgQuery.Node{pgQuery.MakeAStarNode()},
			0,
		),
		0,
	)
	return parser.utils.MakeSubselectFromNode(qSchemaTable.Table, []*pgQuery.Node{selectStarNode}, node, qSchemaTable.Alias)
}

func (parser *QueryParserTable) SchemaFunction(node *pgQuery.Node) PgSchemaFunction {
	for _, funcNode := range node.GetRangeFunction().Functions {
		for _, funcItemNode := range funcNode.GetList().Items {
			funcCallNode := funcItemNode.GetFuncCall()
			if funcCallNode == nil {
				continue
			}

			return parser.utils.SchemaFunction(funcCallNode)
		}
	}

	return PgSchemaFunction{}
}

// pg_catalog.pg_get_keywords() -> VALUES(values...) t(columns...)
func (parser *QueryParserTable) MakePgGetKeywordsNode(node *pgQuery.Node) *pgQuery.Node {
	columns := []string{"word", "catcode", "barelabel", "catdesc", "baredesc"}

	var rows [][]string
	for _, kw := range DUCKDB_KEYWORDS {
		catcode := "U"
		catdesc := "unreserved"

		switch kw.category {
		case "reserved":
			catcode = "R"
			catdesc = "reserved"
		case "type_function":
			catcode = "T"
			catdesc = "reserved (can be function or type name)"
		case "column_name":
			catcode = "C"
			catdesc = "unreserved (cannot be function or type name)"
		}

		row := []string{
			kw.word,
			catcode,
			"t",
			catdesc,
			"can be bare label",
		}
		rows = append(rows, row)
	}

	var alias string
	if node.GetAlias() != nil {
		alias = node.GetAlias().Aliasname
	}

	return parser.utils.MakeSubselectWithRowsNode(PG_FUNCTION_PG_GET_KEYWORDS, columns, rows, alias)
}

// array_upper(array, 1) -> len(array)
func (parser *QueryParserTable) MakeArrayUpperNode(funcCallNode *pgQuery.FuncCall) *pgQuery.FuncCall {
	dimension := funcCallNode.Args[1].GetAConst().GetIval().Ival
	if dimension != 1 {
		return funcCallNode
	}

	return pgQuery.MakeFuncCallNode(
		[]*pgQuery.Node{
			pgQuery.MakeStrNode("len"),
		},
		[]*pgQuery.Node{
			funcCallNode.Args[0],
		},
		0,
	).GetFuncCall()
}

// pg_catalog.pg_show_all_settings() -> duckdb_settings() mapped to pg format
func (parser *QueryParserTable) MakePgShowAllSettingsNode(node *pgQuery.Node) *pgQuery.Node {
	targetList := []*pgQuery.Node{
		pgQuery.MakeResTargetNodeWithNameAndVal(
			"name",
			pgQuery.MakeColumnRefNode([]*pgQuery.Node{pgQuery.MakeStrNode("name")}, 0),
			0,
		),
		pgQuery.MakeResTargetNodeWithNameAndVal(
			"setting",
			pgQuery.MakeColumnRefNode([]*pgQuery.Node{pgQuery.MakeStrNode("value")}, 0),
			0,
		),
		pgQuery.MakeResTargetNodeWithNameAndVal(
			"unit",
			pgQuery.MakeAConstStrNode("", 0),
			0,
		),
		pgQuery.MakeResTargetNodeWithNameAndVal(
			"category",
			pgQuery.MakeAConstStrNode("Settings", 0),
			0,
		),
		pgQuery.MakeResTargetNodeWithNameAndVal(
			"short_desc",
			pgQuery.MakeColumnRefNode([]*pgQuery.Node{pgQuery.MakeStrNode("description")}, 0),
			0,
		),
		pgQuery.MakeResTargetNodeWithNameAndVal(
			"extra_desc",
			pgQuery.MakeAConstStrNode("", 0),
			0,
		),
		pgQuery.MakeResTargetNodeWithNameAndVal(
			"context",
			pgQuery.MakeColumnRefNode([]*pgQuery.Node{pgQuery.MakeStrNode("scope")}, 0),
			0,
		),
		pgQuery.MakeResTargetNodeWithNameAndVal(
			"vartype",
			pgQuery.MakeColumnRefNode([]*pgQuery.Node{pgQuery.MakeStrNode("input_type")}, 0),
			0,
		),
		pgQuery.MakeResTargetNodeWithNameAndVal(
			"source",
			pgQuery.MakeAConstStrNode("default", 0),
			0,
		),
		pgQuery.MakeResTargetNodeWithNameAndVal(
			"min_val",
			pgQuery.MakeAConstStrNode("", 0),
			0,
		),
		pgQuery.MakeResTargetNodeWithNameAndVal(
			"max_val",
			pgQuery.MakeAConstStrNode("", 0),
			0,
		),
		pgQuery.MakeResTargetNodeWithNameAndVal(
			"enumvals",
			pgQuery.MakeAConstStrNode("", 0),
			0,
		),
		pgQuery.MakeResTargetNodeWithNameAndVal(
			"boot_val",
			pgQuery.MakeColumnRefNode([]*pgQuery.Node{pgQuery.MakeStrNode("value")}, 0),
			0,
		),
		pgQuery.MakeResTargetNodeWithNameAndVal(
			"reset_val",
			pgQuery.MakeColumnRefNode([]*pgQuery.Node{pgQuery.MakeStrNode("value")}, 0),
			0,
		),
		pgQuery.MakeResTargetNodeWithNameAndVal(
			"sourcefile",
			pgQuery.MakeAConstStrNode("", 0),
			0,
		),
		pgQuery.MakeResTargetNodeWithNameAndVal(
			"sourceline",
			pgQuery.MakeAConstStrNode("0", 0),
			0,
		),
		pgQuery.MakeResTargetNodeWithNameAndVal(
			"pending_restart",
			pgQuery.MakeAConstStrNode("f", 0),
			0,
		),
	}
	fromNode := pgQuery.MakeSimpleRangeFunctionNode([]*pgQuery.Node{
		pgQuery.MakeListNode([]*pgQuery.Node{
			pgQuery.MakeFuncCallNode(
				[]*pgQuery.Node{
					pgQuery.MakeStrNode("duckdb_settings"),
				},
				nil,
				0,
			),
		}),
	})

	var alias string
	if node.GetAlias() != nil {
		alias = node.GetAlias().Aliasname
	}

	return parser.utils.MakeSubselectFromNode(PG_FUNCTION_PG_SHOW_ALL_SETTINGS, targetList, fromNode, alias)
}

// pg_catalog.pg_is_in_recovery() -> 'f'::bool
func (parser *QueryParserTable) MakePgIsInRecoveryNode(node *pgQuery.Node) *pgQuery.Node {
	var alias string
	if node.GetAlias() != nil {
		alias = node.GetAlias().Aliasname
	}

	return parser.utils.MakeSubselectWithRowsNode(
		PG_FUNCTION_PG_IS_IN_RECOVERY,
		[]string{"pg_is_in_recovery"},
		[][]string{{"f"}},
		alias,
	)
}

// pg_catalog.pg_get_indexdef(index_oid, column_no, pretty_bool) -> VALUES(NULL) t(pg_get_indexdef)
func (parser *QueryParserTable) MakePgGetIndexdefNode(node *pgQuery.Node) *pgQuery.Node {
	columns := PG_GET_INDEXDEF_VALUE_BY_COLUMN.Keys()
	rowValues := PG_GET_INDEXDEF_VALUE_BY_COLUMN.Values()

	return parser.utils.MakeSubselectWithRowsNode("t", columns, [][]string{rowValues}, "")
}

var PG_SHADOW_VALUE_BY_COLUMN = NewOrderedMap([][]string{
	{"usename", "bemidb"},
	{"usesysid", "10"},
	{"usecreatedb", "FALSE"},
	{"usesuper", "FALSE"},
	{"userepl", "TRUE"},
	{"usebypassrls", "FALSE"},
	{"passwd", ""},
	{"valuntil", "NULL"},
	{"useconfig", "NULL"},
})

var PG_ROLES_VALUE_BY_COLUMN = NewOrderedMap([][]string{
	{"oid", "10"},
	{"rolname", ""},
	{"rolsuper", "true"},
	{"rolinherit", "true"},
	{"rolcreaterole", "true"},
	{"rolcreatedb", "true"},
	{"rolcanlogin", "true"},
	{"rolreplication", "false"},
	{"rolconnlimit", "-1"},
	{"rolpassword", "NULL"},
	{"rolvaliduntil", "NULL"},
	{"rolbypassrls", "false"},
	{"rolconfig", "NULL"},
})

var PG_EXTENSION_VALUE_BY_COLUMN = NewOrderedMap([][]string{
	{"oid", "13823"},
	{"extname", "plpgsql"},
	{"extowner", "10"},
	{"extnamespace", "11"},
	{"extrelocatable", "false"},
	{"extversion", "1.0"},
	{"extconfig", "NULL"},
	{"extcondition", "NULL"},
})

var PG_DATABASE_VALUE_BY_COLUMN = NewOrderedMap([][]string{
	{"oid", "16388"},
	{"datname", "bemidb"},
	{"datdba", "10"},
	{"encoding", "6"},
	{"datlocprovider", "c"},
	{"datistemplate", "FALSE"},
	{"datallowconn", "TRUE"},
	{"datconnlimit", "-1"},
	{"datfrozenxid", "722"},
	{"datminmxid", "1"},
	{"dattablespace", "1663"},
	{"datcollate", "en_US.UTF-8"},
	{"datctype", "en_US.UTF-8"},
	{"datlocale", "NULL"},
	{"daticurules", "NULL"},
	{"datcollversion", "NULL"},
	{"datacl", "NULL"},
})

var PG_USER_VALUE_BY_COLUMN = NewOrderedMap([][]string{
	{"usename", "bemidb"},
	{"usesysid", "10"},
	{"usecreatedb", "t"},
	{"usesuper", "t"},
	{"userepl", "t"},
	{"usebypassrls", "t"},
	{"passwd", ""},
	{"valuntil", "NULL"},
	{"useconfig", "NULL"},
})

var PG_STAT_USER_TABLES_VALUE_BY_COLUMN = NewOrderedMap([][]string{
	{"relid", "123456"},
	{"schemaname", "public"},
	{"relname", "table"},
	{"seq_scan", "0"},
	{"last_seq_scan", "NULL"},
	{"seq_tup_read", "0"},
	{"idx_scan", "0"},
	{"last_idx_scan", "NULL"},
	{"idx_tup_fetch", "0"},
	{"n_tup_ins", "0"},
	{"n_tup_upd", "0"},
	{"n_tup_del", "0"},
	{"n_tup_hot_upd", "0"},
	{"n_tup_newpage_upd", "0"},
	{"n_live_tup", "0"},
	{"n_dead_tup", "0"},
	{"n_mod_since_analyze", "0"},
	{"n_ins_since_vacuum", "0"},
	{"last_vacuum", "NULL"},
	{"last_autovacuum", "NULL"},
	{"last_analyze", "NULL"},
	{"last_autoanalyze", "NULL"},
	{"vacuum_count", "0"},
	{"autovacuum_count", "0"},
	{"analyze_count", "0"},
	{"autoanalyze_count", "0"},
})

var PG_GET_INDEXDEF_VALUE_BY_COLUMN = NewOrderedMap([][]string{
	{"pg_get_indexdef", "NULL"},
})

type DuckDBKeyword struct {
	word     string
	category string
}

var DUCKDB_KEYWORDS = []DuckDBKeyword{
	{"abort", "unreserved"},
	{"absolute", "unreserved"},
	{"access", "unreserved"},
	{"action", "unreserved"},
	{"add", "unreserved"},
	{"admin", "unreserved"},
	{"after", "unreserved"},
	{"aggregate", "unreserved"},
	{"all", "reserved"},
	{"also", "unreserved"},
	{"alter", "unreserved"},
	{"always", "unreserved"},
	{"analyse", "reserved"},
	{"analyze", "reserved"},
	{"and", "reserved"},
	{"anti", "type_function"},
	{"any", "reserved"},
	{"array", "reserved"},
	{"as", "reserved"},
	{"asc", "reserved"},
	{"asof", "type_function"},
	{"assertion", "unreserved"},
	{"assignment", "unreserved"},
	{"asymmetric", "reserved"},
	{"at", "unreserved"},
	{"attach", "unreserved"},
	{"attribute", "unreserved"},
	{"authorization", "type_function"},
	{"backward", "unreserved"},
	{"before", "unreserved"},
	{"begin", "unreserved"},
	{"between", "column_name"},
	{"bigint", "column_name"},
	{"binary", "type_function"},
	{"bit", "column_name"},
	{"boolean", "column_name"},
	{"both", "reserved"},
	{"by", "unreserved"},
	{"cache", "unreserved"},
	{"call", "unreserved"},
	{"called", "unreserved"},
	{"cascade", "unreserved"},
	{"cascaded", "unreserved"},
	{"case", "reserved"},
	{"cast", "reserved"},
	{"catalog", "unreserved"},
	{"centuries", "unreserved"},
	{"century", "unreserved"},
	{"chain", "unreserved"},
	{"char", "column_name"},
	{"character", "column_name"},
	{"characteristics", "unreserved"},
	{"check", "reserved"},
	{"checkpoint", "unreserved"},
	{"class", "unreserved"},
	{"close", "unreserved"},
	{"cluster", "unreserved"},
	{"coalesce", "column_name"},
	{"collate", "reserved"},
	{"collation", "type_function"},
	{"column", "reserved"},
	{"columns", "type_function"},
	{"comment", "unreserved"},
	{"comments", "unreserved"},
	{"commit", "unreserved"},
	{"committed", "unreserved"},
	{"compression", "unreserved"},
	{"concurrently", "type_function"},
	{"configuration", "unreserved"},
	{"conflict", "unreserved"},
	{"connection", "unreserved"},
	{"constraint", "reserved"},
	{"constraints", "unreserved"},
	{"content", "unreserved"},
	{"continue", "unreserved"},
	{"conversion", "unreserved"},
	{"copy", "unreserved"},
	{"cost", "unreserved"},
	{"create", "reserved"},
	{"cross", "type_function"},
	{"csv", "unreserved"},
	{"cube", "unreserved"},
	{"current", "unreserved"},
	{"cursor", "unreserved"},
	{"cycle", "unreserved"},
	{"data", "unreserved"},
	{"database", "unreserved"},
	{"day", "unreserved"},
	{"days", "unreserved"},
	{"deallocate", "unreserved"},
	{"dec", "column_name"},
	{"decade", "unreserved"},
	{"decades", "unreserved"},
	{"decimal", "column_name"},
	{"declare", "unreserved"},
	{"default", "reserved"},
	{"defaults", "unreserved"},
	{"deferrable", "reserved"},
	{"deferred", "unreserved"},
	{"definer", "unreserved"},
	{"delete", "unreserved"},
	{"delimiter", "unreserved"},
	{"delimiters", "unreserved"},
	{"depends", "unreserved"},
	{"desc", "reserved"},
	{"describe", "reserved"},
	{"detach", "unreserved"},
	{"dictionary", "unreserved"},
	{"disable", "unreserved"},
	{"discard", "unreserved"},
	{"distinct", "reserved"},
	{"do", "reserved"},
	{"document", "unreserved"},
	{"domain", "unreserved"},
	{"double", "unreserved"},
	{"drop", "unreserved"},
	{"each", "unreserved"},
	{"else", "reserved"},
	{"enable", "unreserved"},
	{"encoding", "unreserved"},
	{"encrypted", "unreserved"},
	{"end", "reserved"},
	{"enum", "unreserved"},
	{"escape", "unreserved"},
	{"event", "unreserved"},
	{"except", "reserved"},
	{"exclude", "unreserved"},
	{"excluding", "unreserved"},
	{"exclusive", "unreserved"},
	{"execute", "unreserved"},
	{"exists", "column_name"},
	{"explain", "unreserved"},
	{"export", "unreserved"},
	{"export_state", "unreserved"},
	{"extension", "unreserved"},
	{"extensions", "unreserved"},
	{"external", "unreserved"},
	{"extract", "column_name"},
	{"family", "unreserved"},
	{"fetch", "reserved"},
	{"filter", "unreserved"},
	{"first", "unreserved"},
	{"float", "column_name"},
	{"following", "unreserved"},
	{"for", "reserved"},
	{"force", "unreserved"},
	{"foreign", "reserved"},
	{"forward", "unreserved"},
	{"freeze", "type_function"},
	{"from", "reserved"},
	{"full", "type_function"},
	{"function", "unreserved"},
	{"functions", "unreserved"},
	{"generated", "type_function"},
	{"glob", "type_function"},
	{"global", "unreserved"},
	{"grant", "reserved"},
	{"granted", "unreserved"},
	{"group", "reserved"},
	{"grouping", "column_name"},
	{"grouping_id", "column_name"},
	{"groups", "unreserved"},
	{"handler", "unreserved"},
	{"having", "reserved"},
	{"header", "unreserved"},
	{"hold", "unreserved"},
	{"hour", "unreserved"},
	{"hours", "unreserved"},
	{"identity", "unreserved"},
	{"if", "unreserved"},
	{"ignore", "unreserved"},
	{"ilike", "type_function"},
	{"immediate", "unreserved"},
	{"immutable", "unreserved"},
	{"implicit", "unreserved"},
	{"import", "unreserved"},
	{"in", "reserved"},
	{"include", "unreserved"},
	{"including", "unreserved"},
	{"increment", "unreserved"},
	{"index", "unreserved"},
	{"indexes", "unreserved"},
	{"inherit", "unreserved"},
	{"inherits", "unreserved"},
	{"initially", "reserved"},
	{"inline", "unreserved"},
	{"inner", "type_function"},
	{"inout", "column_name"},
	{"input", "unreserved"},
	{"insensitive", "unreserved"},
	{"insert", "unreserved"},
	{"install", "unreserved"},
	{"instead", "unreserved"},
	{"int", "column_name"},
	{"integer", "column_name"},
	{"intersect", "reserved"},
	{"interval", "column_name"},
	{"into", "reserved"},
	{"invoker", "unreserved"},
	{"is", "type_function"},
	{"isnull", "type_function"},
	{"isolation", "unreserved"},
	{"join", "type_function"},
	{"json", "unreserved"},
	{"key", "unreserved"},
	{"label", "unreserved"},
	{"language", "unreserved"},
	{"large", "unreserved"},
	{"last", "unreserved"},
	{"lateral", "reserved"},
	{"leading", "reserved"},
	{"leakproof", "unreserved"},
	{"left", "type_function"},
	{"level", "unreserved"},
	{"like", "type_function"},
	{"limit", "reserved"},
	{"listen", "unreserved"},
	{"load", "unreserved"},
	{"local", "unreserved"},
	{"location", "unreserved"},
	{"lock", "unreserved"},
	{"locked", "unreserved"},
	{"logged", "unreserved"},
	{"macro", "unreserved"},
	{"map", "type_function"},
	{"mapping", "unreserved"},
	{"match", "unreserved"},
	{"materialized", "unreserved"},
	{"maxvalue", "unreserved"},
	{"method", "unreserved"},
	{"microsecond", "unreserved"},
	{"microseconds", "unreserved"},
	{"millennia", "unreserved"},
	{"millennium", "unreserved"},
	{"millisecond", "unreserved"},
	{"milliseconds", "unreserved"},
	{"minute", "unreserved"},
	{"minutes", "unreserved"},
	{"minvalue", "unreserved"},
	{"mode", "unreserved"},
	{"month", "unreserved"},
	{"months", "unreserved"},
	{"move", "unreserved"},
	{"name", "unreserved"},
	{"names", "unreserved"},
	{"national", "column_name"},
	{"natural", "type_function"},
	{"nchar", "column_name"},
	{"new", "unreserved"},
	{"next", "unreserved"},
	{"no", "unreserved"},
	{"none", "column_name"},
	{"not", "reserved"},
	{"nothing", "unreserved"},
	{"notify", "unreserved"},
	{"notnull", "type_function"},
	{"nowait", "unreserved"},
	{"null", "reserved"},
	{"nullif", "column_name"},
	{"nulls", "unreserved"},
	{"numeric", "column_name"},
	{"object", "unreserved"},
	{"of", "unreserved"},
	{"off", "unreserved"},
	{"offset", "reserved"},
	{"oids", "unreserved"},
	{"old", "unreserved"},
	{"on", "reserved"},
	{"only", "reserved"},
	{"operator", "unreserved"},
	{"option", "unreserved"},
	{"options", "unreserved"},
	{"or", "reserved"},
	{"order", "reserved"},
	{"ordinality", "unreserved"},
	{"others", "unreserved"},
	{"out", "column_name"},
	{"outer", "type_function"},
	{"over", "unreserved"},
	{"overlaps", "type_function"},
	{"overlay", "column_name"},
	{"overriding", "unreserved"},
	{"owned", "unreserved"},
	{"owner", "unreserved"},
	{"parallel", "unreserved"},
	{"parser", "unreserved"},
	{"partial", "unreserved"},
	{"partition", "unreserved"},
	{"passing", "unreserved"},
	{"password", "unreserved"},
	{"percent", "unreserved"},
	{"persistent", "unreserved"},
	{"pivot", "reserved"},
	{"pivot_longer", "reserved"},
	{"pivot_wider", "reserved"},
	{"placing", "reserved"},
	{"plans", "unreserved"},
	{"policy", "unreserved"},
	{"position", "column_name"},
	{"positional", "type_function"},
	{"pragma", "unreserved"},
	{"preceding", "unreserved"},
	{"precision", "column_name"},
	{"prepare", "unreserved"},
	{"prepared", "unreserved"},
	{"preserve", "unreserved"},
	{"primary", "reserved"},
	{"prior", "unreserved"},
	{"privileges", "unreserved"},
	{"procedural", "unreserved"},
	{"procedure", "unreserved"},
	{"program", "unreserved"},
	{"publication", "unreserved"},
	{"qualify", "reserved"},
	{"quarter", "unreserved"},
	{"quarters", "unreserved"},
	{"quote", "unreserved"},
	{"range", "unreserved"},
	{"read", "unreserved"},
	{"real", "column_name"},
	{"reassign", "unreserved"},
	{"recheck", "unreserved"},
	{"recursive", "unreserved"},
	{"ref", "unreserved"},
	{"references", "reserved"},
	{"referencing", "unreserved"},
	{"refresh", "unreserved"},
	{"reindex", "unreserved"},
	{"relative", "unreserved"},
	{"release", "unreserved"},
	{"rename", "unreserved"},
	{"repeatable", "unreserved"},
	{"replace", "unreserved"},
	{"replica", "unreserved"},
	{"reset", "unreserved"},
	{"respect", "unreserved"},
	{"restart", "unreserved"},
	{"restrict", "unreserved"},
	{"returning", "reserved"},
	{"returns", "unreserved"},
	{"revoke", "unreserved"},
	{"right", "type_function"},
	{"role", "unreserved"},
	{"rollback", "unreserved"},
	{"rollup", "unreserved"},
	{"row", "column_name"},
	{"rows", "unreserved"},
	{"rule", "unreserved"},
	{"sample", "unreserved"},
	{"savepoint", "unreserved"},
	{"schema", "unreserved"},
	{"schemas", "unreserved"},
	{"scope", "unreserved"},
	{"scroll", "unreserved"},
	{"search", "unreserved"},
	{"second", "unreserved"},
	{"seconds", "unreserved"},
	{"secret", "unreserved"},
	{"security", "unreserved"},
	{"select", "reserved"},
	{"semi", "type_function"},
	{"sequence", "unreserved"},
	{"sequences", "unreserved"},
	{"serializable", "unreserved"},
	{"server", "unreserved"},
	{"session", "unreserved"},
	{"set", "unreserved"},
	{"setof", "column_name"},
	{"sets", "unreserved"},
	{"share", "unreserved"},
	{"show", "reserved"},
	{"similar", "type_function"},
	{"simple", "unreserved"},
	{"skip", "unreserved"},
	{"smallint", "column_name"},
	{"snapshot", "unreserved"},
	{"some", "reserved"},
	{"sql", "unreserved"},
	{"stable", "unreserved"},
	{"standalone", "unreserved"},
	{"start", "unreserved"},
	{"statement", "unreserved"},
	{"statistics", "unreserved"},
	{"stdin", "unreserved"},
	{"stdout", "unreserved"},
	{"storage", "unreserved"},
	{"stored", "unreserved"},
	{"strict", "unreserved"},
	{"strip", "unreserved"},
	{"struct", "type_function"},
	{"subscription", "unreserved"},
	{"substring", "column_name"},
	{"summarize", "reserved"},
	{"symmetric", "reserved"},
	{"sysid", "unreserved"},
	{"system", "unreserved"},
	{"table", "reserved"},
	{"tables", "unreserved"},
	{"tablesample", "type_function"},
	{"tablespace", "unreserved"},
	{"temp", "unreserved"},
	{"template", "unreserved"},
	{"temporary", "unreserved"},
	{"text", "unreserved"},
	{"then", "reserved"},
	{"ties", "unreserved"},
	{"time", "column_name"},
	{"timestamp", "column_name"},
	{"to", "reserved"},
	{"trailing", "reserved"},
	{"transaction", "unreserved"},
	{"transform", "unreserved"},
	{"treat", "column_name"},
	{"trigger", "unreserved"},
	{"trim", "column_name"},
	{"truncate", "unreserved"},
	{"trusted", "unreserved"},
	{"try_cast", "type_function"},
	{"type", "unreserved"},
	{"types", "unreserved"},
	{"unbounded", "unreserved"},
	{"uncommitted", "unreserved"},
	{"unencrypted", "unreserved"},
	{"union", "reserved"},
	{"unique", "reserved"},
	{"unknown", "unreserved"},
	{"unlisten", "unreserved"},
	{"unlogged", "unreserved"},
	{"unpivot", "reserved"},
	{"until", "unreserved"},
	{"update", "unreserved"},
	{"use", "unreserved"},
	{"user", "unreserved"},
	{"using", "reserved"},
	{"vacuum", "unreserved"},
	{"valid", "unreserved"},
	{"validate", "unreserved"},
	{"validator", "unreserved"},
	{"value", "unreserved"},
	{"values", "column_name"},
	{"varchar", "column_name"},
	{"variable", "unreserved"},
	{"variadic", "reserved"},
	{"varying", "unreserved"},
	{"verbose", "type_function"},
	{"version", "unreserved"},
	{"view", "unreserved"},
	{"views", "unreserved"},
	{"virtual", "unreserved"},
	{"volatile", "unreserved"},
	{"week", "unreserved"},
	{"weeks", "unreserved"},
	{"when", "reserved"},
	{"where", "reserved"},
	{"whitespace", "unreserved"},
	{"window", "reserved"},
	{"with", "reserved"},
	{"within", "unreserved"},
	{"without", "unreserved"},
	{"work", "unreserved"},
	{"wrapper", "unreserved"},
	{"write", "unreserved"},
	{"xml", "unreserved"},
	{"xmlattributes", "column_name"},
	{"xmlconcat", "column_name"},
	{"xmlelement", "column_name"},
	{"xmlexists", "column_name"},
	{"xmlforest", "column_name"},
	{"xmlnamespaces", "column_name"},
	{"xmlparse", "column_name"},
	{"xmlpi", "column_name"},
	{"xmlroot", "column_name"},
	{"xmlserialize", "column_name"},
	{"xmltable", "column_name"},
	{"year", "unreserved"},
	{"years", "unreserved"},
	{"yes", "unreserved"},
	{"zone", "unreserved"},
}
