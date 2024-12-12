package main

import (
	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

const (
	PG_SCHEMA_PUBLIC = "public"
)

type SelectRemapperTable struct {
	parserTable         *QueryParserTable
	icebergSchemaTables []SchemaTable
	icebergReader       *IcebergReader
	config              *Config
}

func NewSelectRemapperTable(config *Config, icebergReader *IcebergReader) *SelectRemapperTable {
	icebergSchemaTables, err := icebergReader.SchemaTables()
	PanicIfError(err)

	return &SelectRemapperTable{
		parserTable:         NewQueryParserTable(config),
		icebergSchemaTables: icebergSchemaTables,
		icebergReader:       icebergReader,
		config:              config,
	}
}

// FROM / JOIN [TABLE]
func (remapper *SelectRemapperTable) RemapTable(node *pgQuery.Node) *pgQuery.Node {
	parser := remapper.parserTable
	schemaTable := parser.NodeToSchemaTable(node)

	// pg_catalog.pg_statio_user_tables -> return nothing
	if parser.IsPgStatioUserTablesTable(schemaTable) {
		tableNode := parser.MakePgStatioUserTablesNode()
		return remapper.overrideTable(node, tableNode)
	}

	// pg_catalog.pg_shadow -> return hard-coded credentials
	if parser.IsPgShadowTable(schemaTable) {
		tableNode := parser.MakePgShadowNode(remapper.config.User, remapper.config.EncryptedPassword)
		return remapper.overrideTable(node, tableNode)
	}

	// pg_catalog.pg_roles -> return hard-coded role info
	if parser.IsPgRolesTable(schemaTable) {
		tableNode := parser.MakePgRolesNode(remapper.config.User)
		return remapper.overrideTable(node, tableNode)
	}

	// pg_catalog.pg_shdescription -> return nothing
	if parser.IsPgShdescriptionTable(schemaTable) {
		tableNode := parser.MakePgShdescriptionNode()
		return remapper.overrideTable(node, tableNode)
	}

	// pg_catalog.pg_* other system tables
	if parser.IsTableFromPgCatalog(schemaTable) {
		return node
	}

	// information_schema.tables -> return Iceberg tables
	if parser.IsInformationSchemaTablesTable(schemaTable) {
		remapper.reloadIceberSchemaTables()
		if len(remapper.icebergSchemaTables) == 0 {
			return node
		}
		tableNode := parser.MakeInformationSchemaTablesNode(remapper.config.Database, remapper.icebergSchemaTables)
		return remapper.overrideTable(node, tableNode)
	}

	// information_schema.* other system tables
	if parser.IsTableFromInformationSchema(schemaTable) {
		return node
	}

	// iceberg.table -> FROM iceberg_scan('iceberg/schema/table/metadata/v1.metadata.json', skip_schema_inference = true)
	if schemaTable.Schema == "" {
		schemaTable.Schema = PG_SCHEMA_PUBLIC
	}
	if !remapper.icebergSchemaTableExists(schemaTable) {
		remapper.reloadIceberSchemaTables()
		if !remapper.icebergSchemaTableExists(schemaTable) {
			return node // Let it return "Catalog Error: Table with name _ does not exist!"
		}
	}
	icebergPath := remapper.icebergReader.MetadataFilePath(schemaTable)
	tableNode := parser.MakeIcebergTableNode(icebergPath)
	return remapper.overrideTable(node, tableNode)
}

// FROM [PG_FUNCTION()]
func (remapper *SelectRemapperTable) RemapTableFunction(node *pgQuery.Node) *pgQuery.Node {
	for _, functionCall := range remapper.parserTable.NodeToFunctionCalls(node) {
		// pg_catalog.pg_get_keywords() -> hard-coded keywords
		if remapper.parserTable.IsPgGetKeywordsFunction(functionCall) {
			return remapper.parserTable.MakePgGetKeywordsNode()
		}
	}

	return node
}

func (remapper *SelectRemapperTable) overrideTable(node *pgQuery.Node, fromClause *pgQuery.Node) *pgQuery.Node {
	node = fromClause
	return node
}

func (remapper *SelectRemapperTable) reloadIceberSchemaTables() {
	icebergSchemaTables, err := remapper.icebergReader.SchemaTables()
	PanicIfError(err)
	remapper.icebergSchemaTables = icebergSchemaTables
}

func (remapper *SelectRemapperTable) icebergSchemaTableExists(schemaTable SchemaTable) bool {
	for _, icebergSchemaTable := range remapper.icebergSchemaTables {
		if icebergSchemaTable == schemaTable {
			return true
		}
	}
	return false
}
