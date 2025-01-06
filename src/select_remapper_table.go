package main

import (
	"context"

	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

const (
	PG_SCHEMA_PUBLIC = "public"

	PG_TABLE_PG_INHERITS           = "pg_inherits"
	PG_TABLE_PG_SHDESCRIPTION      = "pg_shdescription"
	PG_TABLE_PG_STATIO_USER_TABLES = "pg_statio_user_tables"
	PG_TABLE_PG_SHADOW             = "pg_shadow"
	PG_TABLE_PG_NAMESPACE          = "pg_namespace"
	PG_TABLE_PG_ROLES              = "pg_roles"
	PG_TABLE_PG_CLASS              = "pg_class"
	PG_TABLE_PG_EXTENSION          = "pg_extension"
	PG_TABLE_PG_REPLICATION_SLOTS  = "pg_replication_slots"
	PG_TABLE_PG_DATABASE           = "pg_database"
	PG_TABLE_PG_STAT_GSSAPI        = "pg_stat_gssapi"
	PG_TABLE_PG_AUTH_MEMBERS       = "pg_auth_members"
	PG_TABLE_PG_USER               = "pg_user"
	PG_TABLE_PG_STAT_ACTIVITY      = "pg_stat_activity"
	PG_TABLE_PG_MATVIEWS           = "pg_matviews"
	PG_TABLE_PG_STAT_USER_TABLES   = "pg_stat_user_tables"

	PG_TABLE_TABLES = "tables"
)

type SelectRemapperTable struct {
	parserTable         *QueryParserTable
	parserWhere         *QueryParserWhere
	icebergSchemaTables []IcebergSchemaTable
	icebergReader       *IcebergReader
	duckdb              *Duckdb
	config              *Config
}

func NewSelectRemapperTable(config *Config, icebergReader *IcebergReader, duckdb *Duckdb) *SelectRemapperTable {
	remapper := &SelectRemapperTable{
		parserTable:   NewQueryParserTable(config),
		parserWhere:   NewQueryParserWhere(config),
		icebergReader: icebergReader,
		duckdb:        duckdb,
		config:        config,
	}
	remapper.reloadIceberSchemaTables()
	return remapper
}

// FROM / JOIN [TABLE]
func (remapper *SelectRemapperTable) RemapTable(node *pgQuery.Node) *pgQuery.Node {
	parser := remapper.parserTable
	qSchemaTable := parser.NodeToQuerySchemaTable(node)

	// pg_catalog.pg_* system tables
	if remapper.isTableFromPgCatalog(qSchemaTable) {
		switch qSchemaTable.Table {

		// pg_catalog.pg_shadow -> return hard-coded credentials
		case PG_TABLE_PG_SHADOW:
			tableNode := parser.MakePgShadowNode(remapper.config.User, remapper.config.EncryptedPassword, qSchemaTable.Alias)
			return remapper.overrideTable(node, tableNode)

		// pg_catalog.pg_roles -> return hard-coded role info
		case PG_TABLE_PG_ROLES:
			tableNode := parser.MakePgRolesNode(remapper.config.User, qSchemaTable.Alias)
			return remapper.overrideTable(node, tableNode)

		// pg_catalog.pg_class -> reload Iceberg tables
		case PG_TABLE_PG_CLASS:
			remapper.reloadIceberSchemaTables()
			return node

		// pg_catalog.pg_inherits -> return nothing
		case PG_TABLE_PG_INHERITS:
			tableNode := parser.MakeEmptyTableNode(PG_TABLE_PG_INHERITS, PG_INHERITS_COLUMNS, qSchemaTable.Alias)
			return remapper.overrideTable(node, tableNode)

		// pg_catalog.pg_shdescription -> return nothing
		case PG_TABLE_PG_SHDESCRIPTION:
			tableNode := parser.MakeEmptyTableNode(PG_TABLE_PG_SHDESCRIPTION, PG_SHDESCRIPTION_COLUMNS, qSchemaTable.Alias)
			return remapper.overrideTable(node, tableNode)

		// pg_catalog.pg_statio_user_tables -> return nothing
		case PG_TABLE_PG_STATIO_USER_TABLES:
			tableNode := parser.MakeEmptyTableNode(PG_TABLE_PG_STATIO_USER_TABLES, PG_STATIO_USER_TABLES_COLUMNS, qSchemaTable.Alias)
			return remapper.overrideTable(node, tableNode)

		// pg_catalog.pg_extension -> return hard-coded extension info
		case PG_TABLE_PG_EXTENSION:
			tableNode := parser.MakePgExtensionNode(qSchemaTable.Alias)
			return remapper.overrideTable(node, tableNode)

		// pg_replication_slots -> return nothing
		case PG_TABLE_PG_REPLICATION_SLOTS:
			tableNode := parser.MakeEmptyTableNode(PG_TABLE_PG_REPLICATION_SLOTS, PG_REPLICATION_SLOTS_COLUMNS, qSchemaTable.Alias)
			return remapper.overrideTable(node, tableNode)

		// pg_catalog.pg_database -> return hard-coded database info
		case PG_TABLE_PG_DATABASE:
			tableNode := parser.MakePgDatabaseNode(remapper.config.Database, qSchemaTable.Alias)
			return remapper.overrideTable(node, tableNode)

		// pg_catalog.pg_stat_gssapi -> return nothing
		case PG_TABLE_PG_STAT_GSSAPI:
			tableNode := parser.MakeEmptyTableNode(PG_TABLE_PG_STAT_GSSAPI, PG_STAT_GSSAPI_COLUMNS, qSchemaTable.Alias)
			return remapper.overrideTable(node, tableNode)

		// pg_catalog.pg_auth_members -> return empty table
		case PG_TABLE_PG_AUTH_MEMBERS:
			tableNode := parser.MakeEmptyTableNode(PG_TABLE_PG_AUTH_MEMBERS, PG_AUTH_MEMBERS_COLUMNS, qSchemaTable.Alias)
			return remapper.overrideTable(node, tableNode)

		// pg_catalog.pg_user -> return hard-coded user info
		case PG_TABLE_PG_USER:
			tableNode := parser.MakePgUserNode(remapper.config.User, qSchemaTable.Alias)
			return remapper.overrideTable(node, tableNode)

		// pg_stat_activity -> return empty table
		case PG_TABLE_PG_STAT_ACTIVITY:
			tableNode := parser.MakeEmptyTableNode(PG_TABLE_PG_STAT_ACTIVITY, PG_STAT_ACTIVITY_COLUMNS, qSchemaTable.Alias)
			return remapper.overrideTable(node, tableNode)

		// pg_matviews -> return empty table
		case PG_TABLE_PG_MATVIEWS:
			tableNode := parser.MakeEmptyTableNode(PG_TABLE_PG_MATVIEWS, PG_MATVIEWS_COLUMNS, qSchemaTable.Alias)
			return remapper.overrideTable(node, tableNode)

		// pg_stat_user_tables -> return hard-coded table info
		case PG_TABLE_PG_STAT_USER_TABLES:
			tableNode := parser.MakePgStatUserTablesNode(remapper.icebergSchemaTables, qSchemaTable.Alias)
			return remapper.overrideTable(node, tableNode)

		// pg_catalog.pg_* other system tables -> return as is
		default:
			return node
		}
	}

	// information_schema.* system tables
	if parser.IsTableFromInformationSchema(qSchemaTable) {
		switch qSchemaTable.Table {

		// information_schema.tables -> reload Iceberg tables
		case PG_TABLE_TABLES:
			remapper.reloadIceberSchemaTables()
			return node

		// information_schema.* other system tables -> return as is
		default:
			return node
		}
	}

	// iceberg.table -> FROM iceberg_scan('iceberg/schema/table/metadata/v1.metadata.json', skip_schema_inference = true)
	if qSchemaTable.Schema == "" {
		qSchemaTable.Schema = PG_SCHEMA_PUBLIC
	}
	schemaTable := qSchemaTable.ToIcebergSchemaTable()
	if !remapper.icebergSchemaTableExists(schemaTable) {
		remapper.reloadIceberSchemaTables()
		if !remapper.icebergSchemaTableExists(schemaTable) {
			return node // Let it return "Catalog Error: Table with name _ does not exist!"
		}
	}
	icebergPath := remapper.icebergReader.MetadataFilePath(schemaTable)
	tableNode := parser.MakeIcebergTableNode(icebergPath, qSchemaTable)
	return remapper.overrideTable(node, tableNode)
}

// FROM [PG_FUNCTION()]
func (remapper *SelectRemapperTable) RemapTableFunction(node *pgQuery.Node) *pgQuery.Node {
	parser := remapper.parserTable

	// pg_catalog.pg_get_keywords() -> hard-coded keywords
	if parser.IsPgGetKeywordsFunction(node) {
		return parser.MakePgGetKeywordsNode(node)
	}

	// pg_show_all_settings() -> duckdb_settings()
	if parser.IsPgShowAllSettingsFunction(node) {
		return parser.MakePgShowAllSettingsNode(node)
	}

	// pg_is_in_recovery() -> 'f'::bool
	if parser.IsPgIsInRecoveryFunction(node) {
		return parser.MakePgIsInRecoveryNode(node)
	}

	return node
}

// FROM PG_FUNCTION(PG_NESTED_FUNCTION())
func (remapper *SelectRemapperTable) RemapNestedTableFunction(funcCallNode *pgQuery.FuncCall) *pgQuery.FuncCall {
	// array_upper(values, 1) -> len(values)
	if remapper.parserTable.IsArrayUpperFunction(funcCallNode) {
		return remapper.parserTable.MakeArrayUpperNode(funcCallNode)
	}

	return funcCallNode
}

func (remapper *SelectRemapperTable) RemapWhereClauseForTable(qSchemaTable QuerySchemaTable, selectStatement *pgQuery.SelectStmt) *pgQuery.SelectStmt {
	if remapper.isTableFromPgCatalog(qSchemaTable) {
		switch qSchemaTable.Table {

		// FROM pg_catalog.pg_namespace -> FROM pg_catalog.pg_namespace WHERE nspname != 'main'
		case PG_TABLE_PG_NAMESPACE:
			withoutMainSchemaWhereCondition := remapper.parserWhere.MakeExpressionNode("nspname", "!=", "main")
			return remapper.parserWhere.AppendWhereCondition(selectStatement, withoutMainSchemaWhereCondition)

		// FROM pg_catalog.pg_statio_user_tables -> FROM pg_catalog.pg_statio_user_tables WHERE false
		case PG_TABLE_PG_STATIO_USER_TABLES:
			falseWhereCondition := remapper.parserWhere.MakeFalseConditionNode()
			return remapper.parserWhere.OverrideWhereCondition(selectStatement, falseWhereCondition)
		}
	}
	return selectStatement
}

func (remapper *SelectRemapperTable) overrideTable(node *pgQuery.Node, fromClause *pgQuery.Node) *pgQuery.Node {
	node = fromClause
	return node
}

func (remapper *SelectRemapperTable) reloadIceberSchemaTables() {
	icebergSchemaTables, err := remapper.icebergReader.SchemaTables()
	PanicIfError(err)

	ctx := context.Background()
	for _, icebergSchemaTable := range icebergSchemaTables {
		remapper.duckdb.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS "+icebergSchemaTable.String()+" (id INT)", nil)
	}

	remapper.icebergSchemaTables = icebergSchemaTables
}

func (remapper *SelectRemapperTable) icebergSchemaTableExists(schemaTable IcebergSchemaTable) bool {
	for _, icebergSchemaTable := range remapper.icebergSchemaTables {
		if icebergSchemaTable == schemaTable {
			return true
		}
	}
	return false
}

// System pg_* tables
func (remapper *SelectRemapperTable) isTableFromPgCatalog(qSchemaTable QuerySchemaTable) bool {
	return qSchemaTable.Schema == PG_SCHEMA_PG_CATALOG ||
		(qSchemaTable.Schema == "" &&
			(PG_SYSTEM_TABLES.Contains(qSchemaTable.Table) || PG_SYSTEM_VIEWS.Contains(qSchemaTable.Table)) &&
			!remapper.icebergSchemaTableExists(qSchemaTable.ToIcebergSchemaTable()))
}

var PG_INHERITS_COLUMNS = []string{
	"inhrelid",
	"inhparent",
	"inhseqno",
	"inhdetachpending",
}

var PG_SHDESCRIPTION_COLUMNS = []string{
	"objoid",
	"classoid",
	"description",
}

var PG_STATIO_USER_TABLES_COLUMNS = []string{
	"relid",
	"schemaname",
	"relname",
	"heap_blks_read",
	"heap_blks_hit",
	"idx_blks_read",
	"idx_blks_hit",
	"toast_blks_read",
	"toast_blks_hit",
	"tidx_blks_read",
	"tidx_blks_hit",
}

var PG_REPLICATION_SLOTS_COLUMNS = []string{
	"slot_name",
	"plugin",
	"slot_type",
	"datoid",
	"database",
	"temporary",
	"active",
	"active_pid",
	"xmin",
	"catalog_xmin",
	"restart_lsn",
	"confirmed_flush_lsn",
	"wal_status",
	"safe_wal_size",
	"two_phase",
	"conflicting",
}

var PG_STAT_GSSAPI_COLUMNS = []string{
	"pid",
	"gss_authenticated",
	"principal",
	"encrypted",
	"credentials_delegated",
}

var PG_AUTH_MEMBERS_COLUMNS = []string{
	"oid",
	"roleid",
	"member",
	"grantor",
	"admin_option",
	"inherit_option",
	"set_option",
}

var PG_STAT_ACTIVITY_COLUMNS = []string{
	"datid",
	"datname",
	"pid",
	"usesysid",
	"usename",
	"application_name",
	"client_addr",
	"client_hostname",
	"client_port",
	"backend_start",
	"xact_start",
	"query_start",
	"state_change",
	"wait_event_type",
	"wait_event",
	"state",
	"backend_xid",
	"backend_xmin",
	"query",
	"backend_type",
}

var PG_MATVIEWS_COLUMNS = []string{
	"schemaname",
	"matviewname",
	"matviewowner",
	"tablespace",
	"hasindexes",
	"ispopulated",
	"definition",
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
	"pg_matviews",
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
	"pg_user",
	"pg_range",
	"pg_replication_origin",
	"pg_replication_slots",
	"pg_rewrite",
	"pg_roles",
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
