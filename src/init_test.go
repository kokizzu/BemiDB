package main

import (
	"flag"
	"os"
)

var TEST_PG_SCHEMA_COLUMNS = []PgSchemaColumn{
	{
		ColumnName: "bool_column",
		DataType:   "boolean",
		UdtName:    "bool",
	},
	{
		ColumnName:             "bpchar_column",
		DataType:               "character",
		UdtName:                "bpchar",
		CharacterMaximumLength: "10",
	},
	{
		ColumnName:             "varchar_column",
		DataType:               "character varying",
		UdtName:                "varchar",
		CharacterMaximumLength: "255",
	},
	{
		ColumnName: "text_column",
		DataType:   "text",
		UdtName:    "text",
	},
	{
		ColumnName:       "int2_column",
		DataType:         "smallint",
		UdtName:          "int2",
		NumericPrecision: "16",
		NumericScale:     "0",
	},
	{
		ColumnName:       "int4_column",
		DataType:         "integer",
		UdtName:          "int4",
		NumericPrecision: "32",
		NumericScale:     "0",
	},
	{
		ColumnName:       "int8_column",
		DataType:         "bigint",
		UdtName:          "int8",
		NumericPrecision: "64",
		NumericScale:     "0",
	},
	{
		ColumnName: "xid_column",
		DataType:   "xid",
		UdtName:    "xid",
	},
	{
		ColumnName: "xid8_column",
		DataType:   "xid8",
		UdtName:    "xid8",
	},
	{
		ColumnName:       "float4_column",
		DataType:         "real",
		UdtName:          "float4",
		NumericPrecision: "24",
	},
	{
		ColumnName:       "float8_column",
		DataType:         "double precision",
		UdtName:          "float8",
		NumericPrecision: "53",
	},
	{
		ColumnName:       "numeric_column",
		DataType:         "numeric",
		UdtName:          "numeric",
		NumericPrecision: "10",
		NumericScale:     "2",
	},
	{
		ColumnName:        "date_column",
		DataType:          "date",
		UdtName:           "date",
		DatetimePrecision: "0",
	},
	{
		ColumnName:        "time_column",
		DataType:          "time without time zone",
		UdtName:           "time",
		DatetimePrecision: "6",
	},
	{
		ColumnName:        "time_ms_column",
		DataType:          "time without time zone",
		UdtName:           "time",
		DatetimePrecision: "3",
	},
	{
		ColumnName:        "timetz_column",
		DataType:          "time with time zone",
		UdtName:           "timetz",
		DatetimePrecision: "6",
	},
	{
		ColumnName:        "timetz_ms_column",
		DataType:          "time with time zone",
		UdtName:           "timetz",
		DatetimePrecision: "3",
	},
	{
		ColumnName:        "timestamp_column",
		DataType:          "timestamp without time zone",
		UdtName:           "timestamp",
		DatetimePrecision: "6",
	},
	{
		ColumnName:        "timestamp_ms_column",
		DataType:          "timestamp without time zone",
		UdtName:           "timestamp",
		DatetimePrecision: "3",
	},
	{
		ColumnName:        "timestamptz_column",
		DataType:          "timestamp with time zone",
		UdtName:           "timestamptz",
		DatetimePrecision: "6",
	},
	{
		ColumnName:        "timestamptz_ms_column",
		DataType:          "timestamp with time zone",
		UdtName:           "timestamptz",
		DatetimePrecision: "3",
	},
	{
		ColumnName: "uuid_column",
		DataType:   "uuid",
		UdtName:    "uuid",
	},
	{
		ColumnName: "bytea_column",
		DataType:   "bytea",
		UdtName:    "bytea",
	},
	{
		ColumnName:        "interval_column",
		DataType:          "interval",
		UdtName:           "interval",
		DatetimePrecision: "6",
	},
	{
		ColumnName: "tsvector_column",
		DataType:   "tsvector",
		UdtName:    "tsvector",
	},
	{
		ColumnName: "point_column",
		DataType:   "point",
		UdtName:    "point",
	},
	{
		ColumnName: "inet_column",
		DataType:   "inet",
		UdtName:    "inet",
	},
	{
		ColumnName: "json_column",
		DataType:   "json",
		UdtName:    "json",
	},
	{
		ColumnName: "jsonb_column",
		DataType:   "jsonb",
		UdtName:    "jsonb",
	},
	{
		ColumnName: "array_text_column",
		DataType:   "ARRAY",
		UdtName:    "_text",
	},
	{
		ColumnName: "array_int_column",
		DataType:   "ARRAY",
		UdtName:    "_int4",
	},
	{
		ColumnName: "user_defined_column",
		DataType:   "USER-DEFINED",
		UdtName:    "address",
	},
}

var TEST_LOADED_ROWS = [][]string{
	{
		"true",                                 // bool_column
		"bpchar",                               // bpchar_column
		"varchar",                              // varchar_column
		"text",                                 // text_column
		"32767",                                // int2_column
		"2147483647",                           // int4_column
		"9223372036854775807",                  // int8_column
		"4294967295",                           // xid_column
		"18446744073709551615",                 // xid8_column
		"3.14",                                 // float4_column
		"3.141592653589793",                    // float8_column
		"12345.67",                             // numeric_column
		"2021-01-01",                           // date_column
		"12:00:00.123456",                      // time_column
		"12:00:00.123",                         // time_ms_column
		"12:00:00.123456-05",                   // timetz_column
		"12:00:00.123-05",                      // timetz_ms_column
		"2024-01-01 12:00:00.123456",           // timestamp_column
		"2024-01-01 12:00:00.123",              // timestamp_ms_column
		"2024-01-01 12:00:00.123456-05",        // timestamptz_column
		"2024-01-01 12:00:00.123-05",           // timestamptz_ms_column
		"58a7c845-af77-44b2-8664-7ca613d92f04", // uuid_column
		"\\x1234",                              // bytea_column
		"1 mon 2 days 01:00:01.000001",         // interval_column
		"'sampl':1 'text':2 'tsvector':4",      // tsvector_column
		"(37.347301483154,45.002101898193)",    // point_column
		"192.168.0.1",                          // inet_column
		"{\"key\": \"value\"}",                 // json_column
		"{\"key\": \"value\"}",                 // jsonb_column
		"{one,two,three}",                      // array_text_column
		"{1,2,3}",                              // array_int_column
		"(Toronto)",                            // user_defined_column
	},
	{
		"false",                         // bool_column
		"",                              // bpchar_column
		PG_NULL_STRING,                  // varchar_column
		"",                              // text_column
		"-32767",                        // int2_column
		PG_NULL_STRING,                  // int4_column
		"-9223372036854775807",          // int8_column
		PG_NULL_STRING,                  // xid_column
		PG_NULL_STRING,                  // xid8_column
		PG_NULL_STRING,                  // float4_column
		"-3.141592653589793",            // float8_column
		"-12345.00",                     // numeric_column
		PG_NULL_STRING,                  // date_column
		"12:00:00.123",                  // time_column
		PG_NULL_STRING,                  // time_ms_column
		"12:00:00.12300+05",             // timetz_column
		"12:00:00.1+05",                 // timetz_ms_column
		"2024-01-01 12:00:00",           // timestamp_column
		PG_NULL_STRING,                  // timestamp_ms_column
		"2024-01-01 12:00:00.000123+05", // timestamptz_column
		"2024-01-01 12:00:00.12+05",     // timestamptz_ms_column
		PG_NULL_STRING,                  // uuid_column
		PG_NULL_STRING,                  // bytea_column
		PG_NULL_STRING,                  // interval_column
		PG_NULL_STRING,                  // tsvector_column
		PG_NULL_STRING,                  // point_column
		PG_NULL_STRING,                  // inet_column
		PG_NULL_STRING,                  // json_column
		"{}",                            // jsonb_column
		PG_NULL_STRING,                  // array_text_column
		"{}",                            // array_int_column
		PG_NULL_STRING,                  // user_defined_column
	},
}

func init() {
	config := loadTestConfig()
	icebergWriter := NewIcebergWriter(config)

	for i := range TEST_PG_SCHEMA_COLUMNS {
		TEST_PG_SCHEMA_COLUMNS[i].OrdinalPosition = IntToString(i + 1)
		TEST_PG_SCHEMA_COLUMNS[i].IsNullable = "YES"
	}

	i := 0
	icebergWriter.Write(
		SchemaTable{Schema: "public", Table: "test_table"},
		TEST_PG_SCHEMA_COLUMNS,
		func() [][]string {
			if i > 0 {
				return [][]string{}
			}

			i++
			return TEST_LOADED_ROWS
		},
	)
}

func loadTestConfig() *Config {
	setTestArgs([]string{})

	config := LoadConfig(true)
	config.StorageType = STORAGE_TYPE_LOCAL
	config.StoragePath = "../iceberg-test"
	config.LogLevel = "ERROR"

	return config
}

func setTestArgs(args []string) {
	os.Args = append([]string{"cmd"}, args...)
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	registerFlags()
	flag.Parse()
}
