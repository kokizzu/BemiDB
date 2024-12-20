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

	PG_TABLE_TABLES = "tables"
)

type SelectRemapperTable struct {
	parserTable         *QueryParserTable
	icebergSchemaTables []IcebergSchemaTable
	icebergReader       *IcebergReader
	duckdb              *Duckdb
	config              *Config
}

func NewSelectRemapperTable(config *Config, icebergReader *IcebergReader, duckdb *Duckdb) *SelectRemapperTable {
	remapper := &SelectRemapperTable{
		parserTable:   NewQueryParserTable(config),
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
	if parser.IsTableFromPgCatalog(qSchemaTable) {
		switch qSchemaTable.Table {
		case PG_TABLE_PG_SHADOW:
			// pg_catalog.pg_shadow -> return hard-coded credentials
			tableNode := parser.MakePgShadowNode(remapper.config.User, remapper.config.EncryptedPassword, qSchemaTable.Alias)
			return remapper.overrideTable(node, tableNode)
		case PG_TABLE_PG_ROLES:
			// pg_catalog.pg_roles -> return hard-coded role info
			tableNode := parser.MakePgRolesNode(remapper.config.User, qSchemaTable.Alias)
			return remapper.overrideTable(node, tableNode)
		case PG_TABLE_PG_CLASS:
			// pg_catalog.pg_class -> reload Iceberg tables
			remapper.reloadIceberSchemaTables()
			return node
		case PG_TABLE_PG_INHERITS:
			// pg_catalog.pg_inherits -> return nothing
			tableNode := parser.MakeEmptyTableNode(PG_INHERITS_COLUMNS, qSchemaTable.Alias)
			return remapper.overrideTable(node, tableNode)
		case PG_TABLE_PG_SHDESCRIPTION:
			// pg_catalog.pg_shdescription -> return nothing
			tableNode := parser.MakeEmptyTableNode(PG_SHDESCRIPTION_COLUMNS, qSchemaTable.Alias)
			return remapper.overrideTable(node, tableNode)
		case PG_TABLE_PG_STATIO_USER_TABLES:
			// pg_catalog.pg_statio_user_tables -> return nothing
			tableNode := parser.MakeEmptyTableNode(PG_STATIO_USER_TABLES_COLUMNS, qSchemaTable.Alias)
			return remapper.overrideTable(node, tableNode)
		case PG_TABLE_PG_EXTENSION:
			// pg_catalog.pg_extension -> return hard-coded extension info
			tableNode := parser.MakePgExtensionNode(qSchemaTable.Alias)
			return remapper.overrideTable(node, tableNode)
		case PG_TABLE_PG_REPLICATION_SLOTS:
			// pg_replication_slots -> return nothing
			tableNode := parser.MakeEmptyTableNode(PG_REPLICATION_SLOTS_COLUMNS, qSchemaTable.Alias)
			return remapper.overrideTable(node, tableNode)
		case PG_TABLE_PG_DATABASE:
			// pg_catalog.pg_database -> return hard-coded database info
			tableNode := parser.MakePgDatabaseNode(remapper.config.Database, qSchemaTable.Alias)
			return remapper.overrideTable(node, tableNode)
		case PG_TABLE_PG_STAT_GSSAPI:
			// pg_catalog.pg_stat_gssapi -> return nothing
			tableNode := parser.MakeEmptyTableNode(PG_STAT_GSSAPI_COLUMNS, qSchemaTable.Alias)
			return remapper.overrideTable(node, tableNode)
		case PG_TABLE_PG_AUTH_MEMBERS:
			// pg_catalog.pg_auth_members -> return empty table
			tableNode := parser.MakeEmptyTableNode(PG_AUTH_MEMBERS_COLUMNS, qSchemaTable.Alias)
			return remapper.overrideTable(node, tableNode)
		default:
			// pg_catalog.pg_* other system tables -> return as is
			return node
		}
	}

	// information_schema.* system tables
	if parser.IsTableFromInformationSchema(qSchemaTable) {
		switch qSchemaTable.Table {
		case PG_TABLE_TABLES:
			// information_schema.tables -> reload Iceberg tables
			remapper.reloadIceberSchemaTables()
			return node
		default:
			// information_schema.* other system tables -> return as is
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
	tableNode := parser.MakeIcebergTableNode(icebergPath, qSchemaTable.Alias)
	return remapper.overrideTable(node, tableNode)
}

// FROM [PG_FUNCTION()]
func (remapper *SelectRemapperTable) RemapTableFunction(node *pgQuery.Node) *pgQuery.Node {
	// pg_catalog.pg_get_keywords() -> hard-coded keywords
	if remapper.parserTable.IsPgGetKeywordsFunction(node) {
		return remapper.parserTable.MakePgGetKeywordsNode(node)
	}

	// pg_show_all_settings() -> duckdb_settings()
	if remapper.parserTable.IsPgShowAllSettingsFunction(node) {
		return remapper.parserTable.MakePgShowAllSettingsNode(node)
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
