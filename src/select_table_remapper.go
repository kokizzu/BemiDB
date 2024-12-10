package main

import (
	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

type SelectTableRemapper struct {
	queryParser   *QueryParser
	icebergReader *IcebergReader
	config        *Config
}

func NewSelectTableRemapper(config *Config, queryParser *QueryParser, icebergReader *IcebergReader) *SelectTableRemapper {
	return &SelectTableRemapper{
		queryParser:   queryParser,
		icebergReader: icebergReader,
		config:        config,
	}
}

// FROM / JOIN [TABLE]
func (remapper *SelectTableRemapper) RemapTable(node *pgQuery.Node) *pgQuery.Node {
	parser := remapper.queryParser
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

	// pg_catalog.pg_* other system tables
	if parser.IsTableFromPgCatalog(schemaTable) {
		return node
	}

	// information_schema.tables -> return Iceberg tables
	if parser.IsInformationSchemaTablesTable(schemaTable) {
		icebergSchemaTables, err := remapper.icebergReader.SchemaTables()
		if err != nil {
			LogError(remapper.config, "Failed to get Iceberg schema tables:", err)
			return node
		}
		if len(icebergSchemaTables) == 0 {
			return node
		}
		tableNode := parser.MakeInformationSchemaTablesNode(remapper.config.Database, icebergSchemaTables)
		return remapper.overrideTable(node, tableNode)
	}

	// iceberg.table
	return node
}

func (remapper *SelectTableRemapper) overrideTable(node *pgQuery.Node, fromClause *pgQuery.Node) *pgQuery.Node {
	node = fromClause
	return node
}
