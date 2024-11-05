package main

import (
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

func TestHandleQuery(t *testing.T) {
	var responsesByQuery = map[string]map[string][]string{
		// PG functions
		"SELECT VERSION()": {
			"description": {"version"},
			"values":      {"PostgreSQL 17.0, compiled by Bemi"},
		},
		"SELECT pg_catalog.pg_get_userbyid(p.proowner) AS owner, 'Foo' AS foo FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace LIMIT 1": {
			"description": {"owner", "foo"},
			"values":      {"bemidb", "Foo"},
		},
		"SELECT QUOTE_IDENT('fooBar')": {
			"description": {"quote_ident"},
			"values":      {"\"fooBar\""},
		},
		// PG system tables
		"SELECT oid, typname AS typename FROM pg_type WHERE typname='geometry' OR typname='geography'": {
			"description": {"oid", "typename"},
			"values":      {},
		},
		// pg_namespace
		"SELECT DISTINCT(nspname) FROM pg_catalog.pg_namespace WHERE nspname != 'information_schema' AND nspname != 'pg_catalog'": {
			"description": {"nspname"},
			"values":      {"public"},
		},
		"SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname == 'main'": {
			"description": {"nspname"},
			"values":      {},
		},
		// pg_statio_user_tables
		"SELECT pg_total_relation_size(relid) AS total_size FROM pg_catalog.pg_statio_user_tables WHERE schemaname = 'public'": {
			"description": {"total_size"},
			"values":      {},
		},
		"SELECT pg_total_relation_size(relid) AS total_size FROM pg_catalog.pg_statio_user_tables WHERE schemaname = 'public' UNION SELECT NULL AS total_size FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'public'": {
			"description": {"total_size"},
			"values":      {},
		},
		// Information schema
		"SELECT * FROM information_schema.tables": {
			"description": {"table_catalog", "table_schema", "table_name", "table_type", "self_referencing_column_name", "reference_generation", "user_defined_type_catalog", "user_defined_type_schema", "user_defined_type_name", "is_insertable_into", "is_typed", "commit_action"},
			"values":      {"bemidb", "public", "test_table", "BASE TABLE", "NULL", "NULL", "NULL", "NULL", "NULL", "YES", "NO", "NULL"},
		},
		"SELECT table_catalog, table_schema, table_name AS table FROM information_schema.tables": {
			"description": {"table_catalog", "table_schema", "table"},
			"values":      {"bemidb", "public", "test_table"},
		},
		// Iceberg data
		"SELECT COUNT(*) AS count FROM public.test_table": {
			"description": {"count"},
			"values":      {"5"},
		},
		"SELECT COUNT(*) AS count FROM test_table": {
			"description": {"count"},
			"values":      {"5"},
		},
		"SELECT AVG(decimal_value) / 2 AS half_average FROM public.test_table": {
			"description": {"half_average"},
			"values":      {"3.5"},
		},
	}

	for query, responses := range responsesByQuery {
		t.Run(query, func(t *testing.T) {
			config := loadTestConfig()
			duckdb := NewDuckdb(config)
			icebergReader := NewIcebergReader(config)
			proxy := NewProxy(config, duckdb, icebergReader)

			messages, err := proxy.HandleQuery(query)

			if err != nil {
				t.Errorf("Expected no error, got %v", err)
				return
			}

			testRowDescription(t, messages[0], responses["description"])

			if len(responses["values"]) > 0 {
				testMessageTypes(t, messages, []pgproto3.Message{
					&pgproto3.RowDescription{},
					&pgproto3.DataRow{},
					&pgproto3.CommandComplete{},
					&pgproto3.ReadyForQuery{},
				})
				testDataRowValues(t, messages[1], responses["values"])
			} else {
				testMessageTypes(t, messages, []pgproto3.Message{
					&pgproto3.RowDescription{},
					&pgproto3.CommandComplete{},
					&pgproto3.ReadyForQuery{},
				})
			}

		})
	}
}

func testMessageTypes(t *testing.T, messages []pgproto3.Message, expectedTypes []pgproto3.Message) {
	if len(messages) != len(expectedTypes) {
		t.Errorf("Expected %v messages, got %v", len(expectedTypes), len(messages))
	}

	for i, expectedType := range expectedTypes {
		if reflect.TypeOf(messages[i]) != reflect.TypeOf(expectedType) {
			t.Errorf("Expected the %v message to be a %v", i, expectedType)
		}
	}
}

func testRowDescription(t *testing.T, rowDescriptionMessage pgproto3.Message, expectedColumnNames []string) {
	rowDescription := rowDescriptionMessage.(*pgproto3.RowDescription)

	if len(rowDescription.Fields) != len(expectedColumnNames) {
		t.Errorf("Expected %v row description fields, got %v", len(expectedColumnNames), len(rowDescription.Fields))
	}

	for i, expectedColumnName := range expectedColumnNames {
		if string(rowDescription.Fields[i].Name) != expectedColumnName {
			t.Errorf("Expected the %v row description field to be %v, got %v", i, expectedColumnName, string(rowDescription.Fields[i].Name))
		}
	}
}

func testDataRowValues(t *testing.T, dataRowMessage pgproto3.Message, expectedValues []string) {
	dataRow := dataRowMessage.(*pgproto3.DataRow)

	if len(dataRow.Values) != len(expectedValues) {
		t.Errorf("Expected %v data row values, got %v", len(expectedValues), len(dataRow.Values))
	}

	for i, expectedValue := range expectedValues {
		if string(dataRow.Values[i]) != expectedValue {
			t.Errorf("Expected the %v data row value to be %v, got %v", i, expectedValue, string(dataRow.Values[i]))
		}
	}
}
