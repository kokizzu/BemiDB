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
			"values":      {"2"},
		},
		"SELECT COUNT(*) AS count FROM test_table": {
			"description": {"count"},
			"values":      {"2"},
		},
		"SELECT bool_column FROM public.test_table WHERE bool_column = TRUE": {
			"description": {"bool_column"},
			"values":      {"true"},
		},
		"SELECT bool_column FROM public.test_table WHERE bool_column = FALSE": {
			"description": {"bool_column"},
			"values":      {"false"},
		},
		"SELECT bpchar_column FROM public.test_table WHERE bool_column = TRUE": {
			"description": {"bpchar_column"},
			"values":      {"bpchar"},
		},
		"SELECT bpchar_column FROM public.test_table WHERE bool_column = FALSE": {
			"description": {"bpchar_column"},
			"values":      {""},
		},
		"SELECT varchar_column FROM public.test_table WHERE varchar_column IS NOT NULL": {
			"description": {"varchar_column"},
			"values":      {"varchar"},
		},
		"SELECT varchar_column FROM public.test_table WHERE varchar_column IS NULL": {
			"description": {"varchar_column"},
			"values":      {""},
		},
		"SELECT text_column FROM public.test_table WHERE bool_column = TRUE": {
			"description": {"text_column"},
			"values":      {"text"},
		},
		"SELECT text_column FROM public.test_table WHERE bool_column = FALSE": {
			"description": {"text_column"},
			"values":      {""},
		},
		"SELECT int2_column FROM public.test_table WHERE bool_column = TRUE": {
			"description": {"int2_column"},
			"values":      {"32767"},
		},
		"SELECT int2_column FROM public.test_table WHERE bool_column = FALSE": {
			"description": {"int2_column"},
			"values":      {"-32767"},
		},
		"SELECT int4_column FROM public.test_table WHERE int4_column IS NOT NULL": {
			"description": {"int4_column"},
			"values":      {"2147483647"},
		},
		"SELECT int4_column FROM public.test_table WHERE int4_column IS NULL": {
			"description": {"int4_column"},
			"values":      {""},
		},
		"SELECT int8_column FROM public.test_table WHERE bool_column = TRUE": {
			"description": {"int8_column"},
			"values":      {"9223372036854775807"},
		},
		"SELECT int8_column FROM public.test_table WHERE bool_column = FALSE": {
			"description": {"int8_column"},
			"values":      {"-9223372036854775807"},
		},
		"SELECT float4_column FROM public.test_table WHERE float4_column IS NOT NULL": {
			"description": {"float4_column"},
			"values":      {"3.14"},
		},
		"SELECT float4_column FROM public.test_table WHERE float4_column IS NULL": {
			"description": {"float4_column"},
			"values":      {""},
		},
		"SELECT float8_column FROM public.test_table WHERE bool_column = TRUE": {
			"description": {"float8_column"},
			"values":      {"3.141592653589793"},
		},
		"SELECT float8_column FROM public.test_table WHERE bool_column = FALSE": {
			"description": {"float8_column"},
			"values":      {"-3.141592653589793"},
		},
		"SELECT numeric_column FROM public.test_table WHERE bool_column = TRUE": {
			"description": {"numeric_column"},
			"values":      {"12345.67"},
		},
		"SELECT numeric_column FROM public.test_table WHERE bool_column = FALSE": {
			"description": {"numeric_column"},
			"values":      {"-12345"},
		},
		"SELECT date_column FROM public.test_table WHERE date_column IS NOT NULL": {
			"description": {"date_column"},
			"values":      {"2021-01-01"},
		},
		"SELECT date_column FROM public.test_table WHERE date_column IS NULL": {
			"description": {"date_column"},
			"values":      {""},
		},
		"SELECT time_column FROM public.test_table WHERE bool_column = TRUE": {
			"description": {"time_column"},
			"values":      {"12:00:00.123456"},
		},
		"SELECT time_column FROM public.test_table WHERE bool_column = FALSE": {
			"description": {"time_column"},
			"values":      {"12:00:00.123"},
		},
		"SELECT time_ms_column FROM public.test_table WHERE time_ms_column IS NOT NULL": {
			"description": {"time_ms_column"},
			"values":      {"12:00:00.123"},
		},
		"SELECT time_ms_column FROM public.test_table WHERE time_ms_column IS NULL": {
			"description": {"time_ms_column"},
			"values":      {""},
		},
		"SELECT timetz_column FROM public.test_table WHERE bool_column = TRUE": {
			"description": {"timetz_column"},
			"values":      {"17:00:00.123456"},
		},
		"SELECT timetz_column FROM public.test_table WHERE bool_column = FALSE": {
			"description": {"timetz_column"},
			"values":      {"07:00:00.123"},
		},
		"SELECT timestamp_column FROM public.test_table WHERE bool_column = TRUE": {
			"description": {"timestamp_column"},
			"values":      {"2024-01-01 12:00:00.123456"},
		},
		"SELECT timestamp_column FROM public.test_table WHERE bool_column = FALSE": {
			"description": {"timestamp_column"},
			"values":      {"2024-01-01 12:00:00"},
		},
		"SELECT timestamp_ms_column FROM public.test_table WHERE timestamp_ms_column IS NOT NULL": {
			"description": {"timestamp_ms_column"},
			"values":      {"2024-01-01 12:00:00.123"},
		},
		"SELECT timestamp_ms_column FROM public.test_table WHERE timestamp_ms_column IS NULL": {
			"description": {"timestamp_ms_column"},
			"values":      {""},
		},
		"SELECT timestamptz_column FROM public.test_table WHERE bool_column = TRUE": {
			"description": {"timestamptz_column"},
			"values":      {"2024-01-01 17:00:00.123456"},
		},
		"SELECT timestamptz_column FROM public.test_table WHERE bool_column = FALSE": {
			"description": {"timestamptz_column"},
			"values":      {"2024-01-01 07:00:00.000123"},
		},
		"SELECT timestamptz_ms_column FROM public.test_table WHERE bool_column = TRUE": {
			"description": {"timestamptz_ms_column"},
			"values":      {"2024-01-01 17:00:00.123"},
		},
		"SELECT timestamptz_ms_column FROM public.test_table WHERE bool_column = FALSE": {
			"description": {"timestamptz_ms_column"},
			"values":      {"2024-01-01 07:00:00.12"},
		},
		"SELECT uuid_column FROM public.test_table WHERE uuid_column IS NOT NULL": {
			"description": {"uuid_column"},
			"values":      {"58a7c845-af77-44b2-8664-7ca613d92f04"},
		},
		"SELECT uuid_column FROM public.test_table WHERE uuid_column IS NULL": {
			"description": {"uuid_column"},
			"values":      {""},
		},
		"SELECT bytea_column FROM public.test_table WHERE bytea_column IS NOT NULL": {
			"description": {"bytea_column"},
			"values":      {"\\x1234"},
		},
		"SELECT bytea_column FROM public.test_table WHERE bytea_column IS NULL": {
			"description": {"bytea_column"},
			"values":      {""},
		},
		"SELECT interval_column FROM public.test_table WHERE interval_column IS NOT NULL": {
			"description": {"interval_column"},
			"values":      {"1 mon 2 days 01:00:01.000001"},
		},
		"SELECT interval_column FROM public.test_table WHERE interval_column IS NULL": {
			"description": {"interval_column"},
			"values":      {""},
		},
		"SELECT json_column FROM public.test_table WHERE json_column IS NOT NULL": {
			"description": {"json_column"},
			"values":      {"{\"key\": \"value\"}"},
		},
		"SELECT json_column FROM public.test_table WHERE json_column IS NULL": {
			"description": {"json_column"},
			"values":      {""},
		},
		"SELECT jsonb_column FROM public.test_table WHERE bool_column = TRUE": {
			"description": {"jsonb_column"},
			"values":      {"{\"key\": \"value\"}"},
		},
		"SELECT jsonb_column->'key' AS key FROM public.test_table WHERE jsonb_column->>'key' = 'value'": {
			"description": {"key"},
			"values":      {"\"value\""},
		},
		"SELECT jsonb_column FROM public.test_table WHERE bool_column = FALSE": {
			"description": {"jsonb_column"},
			"values":      {"{}"},
		},
		"SELECT tsvector_column FROM public.test_table WHERE tsvector_column IS NOT NULL": {
			"description": {"tsvector_column"},
			"values":      {"'sampl':1 'text':2 'tsvector':4"},
		},
		"SELECT tsvector_column FROM public.test_table WHERE tsvector_column IS NULL": {
			"description": {"tsvector_column"},
			"values":      {""},
		},
		"SELECT point_column FROM public.test_table WHERE point_column IS NOT NULL": {
			"description": {"point_column"},
			"values":      {"(37.347301483154,45.002101898193)"},
		},
		"SELECT point_column FROM public.test_table WHERE point_column IS NULL": {
			"description": {"point_column"},
			"values":      {""},
		},
		"SELECT inet_column FROM public.test_table WHERE inet_column IS NOT NULL": {
			"description": {"inet_column"},
			"values":      {"192.168.0.1"},
		},
		"SELECT inet_column FROM public.test_table WHERE inet_column IS NULL": {
			"description": {"inet_column"},
			"values":      {""},
		},
		"SELECT array_text_column FROM public.test_table WHERE array_text_column IS NOT NULL": {
			"description": {"array_text_column"},
			"values":      {"{one,two,three}"},
		},
		"SELECT array_text_column FROM public.test_table WHERE array_text_column IS NULL": {
			"description": {"array_text_column"},
			"values":      {""},
		},
		"SELECT array_int_column FROM public.test_table WHERE bool_column = TRUE": {
			"description": {"array_int_column"},
			"values":      {"{1,2,3}"},
		},
		"SELECT array_int_column FROM public.test_table WHERE bool_column = FALSE": {
			"description": {"array_int_column"},
			"values":      {""},
		},
		"SELECT user_defined_column FROM public.test_table WHERE user_defined_column IS NOT NULL": {
			"description": {"user_defined_column"},
			"values":      {"(Toronto)"},
		},
		"SELECT user_defined_column FROM public.test_table WHERE user_defined_column IS NULL": {
			"description": {"user_defined_column"},
			"values":      {""},
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
