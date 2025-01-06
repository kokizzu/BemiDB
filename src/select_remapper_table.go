package main

import (
	"context"

	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

type SelectRemapperTable struct {
	parserTable         *QueryParserTable
	parserWhere         *QueryParserWhere
	parserSelect        *QueryParserSelect
	icebergSchemaTables []IcebergSchemaTable
	icebergReader       *IcebergReader
	duckdb              *Duckdb
	config              *Config
}

func NewSelectRemapperTable(config *Config, icebergReader *IcebergReader, duckdb *Duckdb) *SelectRemapperTable {
	remapper := &SelectRemapperTable{
		parserTable:   NewQueryParserTable(config),
		parserWhere:   NewQueryParserWhere(config),
		parserSelect:  NewQueryParserSelect(config),
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

	schemaFunction := parser.SchemaFunction(node)

	if remapper.isFunctionFromPgCatalog(schemaFunction) {
		switch {

		// pg_catalog.pg_get_keywords() -> hard-coded keywords
		case schemaFunction.Function == PG_FUNCTION_PG_GET_KEYWORDS:
			return parser.MakePgGetKeywordsNode(node)

		// pg_catalog.pg_show_all_settings() -> duckdb_settings()
		case schemaFunction.Function == PG_FUNCTION_PG_SHOW_ALL_SETTINGS:
			return parser.MakePgShowAllSettingsNode(node)

		// pg_catalog.pg_is_in_recovery() -> 'f'::bool
		case schemaFunction.Function == PG_FUNCTION_PG_IS_IN_RECOVERY:
			return parser.MakePgIsInRecoveryNode(node)
		}
	}

	return node
}

// FROM PG_FUNCTION(PG_NESTED_FUNCTION())
func (remapper *SelectRemapperTable) RemapNestedTableFunction(functionCall *pgQuery.FuncCall) *pgQuery.FuncCall {
	schemaFunction := remapper.parserSelect.SchemaFunction(functionCall)

	switch {

	// array_upper(values, 1) -> len(values)
	case schemaFunction.Function == PG_FUNCTION_ARRAY_UPPER:
		return remapper.parserTable.MakeArrayUpperNode(functionCall)

	default:
		return functionCall
	}
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

func (remapper *SelectRemapperTable) isFunctionFromPgCatalog(schemaFunction PgSchemaFunction) bool {
	return schemaFunction.Schema == PG_SCHEMA_PG_CATALOG ||
		(schemaFunction.Schema == "" && PG_SYSTEM_FUNCTIONS.Contains(schemaFunction.Function))
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
