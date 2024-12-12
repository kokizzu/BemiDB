package main

import (
	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

const (
	// PG_SCHEMA_PG_CATALOG = "pg_catalog" Already defined in pg_schema_column.go
	PG_TABLE_PG_STATIO_USER_TABLES = "pg_statio_user_tables"
	PG_TABLE_PG_SHADOW             = "pg_shadow"
	PG_TABLE_PG_NAMESPACE          = "pg_namespace"
	PG_TABLE_PG_ROLES              = "pg_roles"
	PG_TABLE_PG_SHDESCRIPTION      = "pg_shdescription"

	PG_SCHEMA_INFORMATION_SCHEMA = "information_schema"
	PG_TABLE_TABLES              = "tables"

	PG_FUNCTION_PG_GET_KEYWORDS = "pg_get_keywords"
)

type QueryParserTable struct {
	config *Config
	utils  *QueryUtils
}

type FunctionCall struct {
	Schema   string
	Function string
	Alias 	string
}

func NewQueryParserTable(config *Config) *QueryParserTable {
	return &QueryParserTable{config: config, utils: NewQueryUtils(config)}
}

func (parser *QueryParserTable) NodeToSchemaTable(node *pgQuery.Node) SchemaTable {
	rangeVar := node.GetRangeVar()
	var alias string


		if rangeVar.Alias != nil {
			alias = rangeVar.Alias.Aliasname
		}

	return SchemaTable{
		Schema: rangeVar.Schemaname,
		Table:  rangeVar.Relname,
		Alias:  alias,
	}
}

func (parser *QueryParserTable) NodeToFunctionCalls(node *pgQuery.Node) []FunctionCall {
	var functionCalls []FunctionCall
	rangeFunction := node.GetRangeFunction()

	var alias string
	if rangeFunction.Alias != nil {
		alias = rangeFunction.Alias.Aliasname
	}

	for _, functionNode := range rangeFunction.Functions {
		for _, item := range functionNode.GetList().Items {
			funcCall := item.GetFuncCall()
			if funcCall == nil {
				continue
			}

			var functionCall FunctionCall

			switch len(funcCall.Funcname) {
			case 1:
				functionCall = FunctionCall{
					Function: funcCall.Funcname[0].GetString_().Sval,
					Alias:    alias,
				}
			case 2:
				functionCall = FunctionCall{
					Schema:   funcCall.Funcname[0].GetString_().Sval,
					Function: funcCall.Funcname[1].GetString_().Sval,
					Alias:    alias,
				}
			}

			functionCalls = append(functionCalls, functionCall)
		}
	}

	return functionCalls
}

// pg_catalog.pg_statio_user_tables
func (parser *QueryParserTable) IsPgStatioUserTablesTable(schemaTable SchemaTable) bool {
	return parser.isPgCatalogSchema(schemaTable) && schemaTable.Table == PG_TABLE_PG_STATIO_USER_TABLES
}

// pg_catalog.pg_statio_user_tables -> return nothing
func (parser *QueryParserTable) MakePgStatioUserTablesNode(alias string) *pgQuery.Node {
	columns := PG_STATIO_USER_TABLES_VALUE_BY_COLUMN.Keys()
	rowValues := PG_STATIO_USER_TABLES_VALUE_BY_COLUMN.Values()

	return parser.utils.MakeSubselectNode(columns, [][]string{rowValues}, alias)
}

// pg_catalog.pg_shadow
func (parser *QueryParserTable) IsPgShadowTable(schemaTable SchemaTable) bool {
	return parser.isPgCatalogSchema(schemaTable) && schemaTable.Table == PG_TABLE_PG_SHADOW
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

	return parser.utils.MakeSubselectNode(columns, rowsValues, alias)
}

// pg_catalog.pg_roles
func (parser *QueryParserTable) IsPgRolesTable(schemaTable SchemaTable) bool {
	return parser.isPgCatalogSchema(schemaTable) && schemaTable.Table == PG_TABLE_PG_ROLES
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

	return parser.utils.MakeSubselectNode(columns, rowsValues, alias)
}

// pg_catalog.pg_namespace
func (parser *QueryParserTable) IsPgNamespaceTable(schemaTable SchemaTable) bool {
	return parser.isPgCatalogSchema(schemaTable) && schemaTable.Table == PG_TABLE_PG_NAMESPACE
}

// pg_catalog.pg_shdescription
func (parser *QueryParserTable) IsPgShdescriptionTable(schemaTable SchemaTable) bool {
	return parser.isPgCatalogSchema(schemaTable) && schemaTable.Table == PG_TABLE_PG_SHDESCRIPTION
}

// pg_catalog.pg_shdescription -> return nothing
func (parser *QueryParserTable) MakePgShdescriptionNode(alias string) *pgQuery.Node {
	columns := PG_SHDESCRIPTION_VALUE_BY_COLUMN.Keys()
	rowValues := PG_SHDESCRIPTION_VALUE_BY_COLUMN.Values()

	return parser.utils.MakeSubselectNode(columns, [][]string{rowValues}, alias)
}

// Other system pg_* tables
func (parser *QueryParserTable) IsTableFromPgCatalog(schemaTable SchemaTable) bool {
	return parser.isPgCatalogSchema(schemaTable) &&
		(PG_SYSTEM_TABLES.Contains(schemaTable.Table) || PG_SYSTEM_VIEWS.Contains(schemaTable.Table))
}

// information_schema.tables
func (parser *QueryParserTable) IsInformationSchemaTablesTable(schemaTable SchemaTable) bool {
	return parser.IsTableFromInformationSchema(schemaTable) && schemaTable.Table == PG_TABLE_TABLES
}

// information_schema.tables -> VALUES(values...) t(columns...)
func (parser *QueryParserTable) MakeInformationSchemaTablesNode(database string, schemaAndTables []SchemaTable, alias string) *pgQuery.Node {
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

	return parser.utils.MakeSubselectNode(columns, rowsValues, alias)
}

// Other information_schema.* tables
func (parser *QueryParserTable) IsTableFromInformationSchema(schemaTable SchemaTable) bool {
	return schemaTable.Schema == PG_SCHEMA_INFORMATION_SCHEMA
}

// iceberg.table -> FROM iceberg_scan('path', skip_schema_inference = true)
func (parser *QueryParserTable) MakeIcebergTableNode(tablePath string) *pgQuery.Node {
	return pgQuery.MakeSimpleRangeFunctionNode([]*pgQuery.Node{
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
}

// pg_catalog.pg_get_keywords()
func (parser *QueryParserTable) IsPgGetKeywordsFunction(functionCall FunctionCall) bool {
	return functionCall.Schema == PG_SCHEMA_PG_CATALOG && functionCall.Function == PG_FUNCTION_PG_GET_KEYWORDS
}

// pg_catalog.pg_get_keywords() -> VALUES(values...) t(columns...)
func (parser *QueryParserTable) MakePgGetKeywordsNode(alias string) *pgQuery.Node {
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

	return parser.utils.MakeSubselectNode(columns, rows, alias)
}

func (parser *QueryParserTable) isPgCatalogSchema(schemaTable SchemaTable) bool {
	return schemaTable.Schema == PG_SCHEMA_PG_CATALOG || schemaTable.Schema == ""
}

var PG_SYSTEM_TABLES = NewSet([]string{
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
	"pg_shadow",
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
})

var PG_SYSTEM_VIEWS = NewSet([]string{
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
})

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

var PG_SHDESCRIPTION_VALUE_BY_COLUMN = NewOrderedMap([][]string{
	{"objoid", "0"},
	{"classoid", "0"},
	{"description", "NULL"},
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
	{"false", "reserved"},
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
	{"true", "reserved"},
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
