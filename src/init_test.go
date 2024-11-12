package main

import (
	"flag"
	"os"
)

var TEST_PG_SCHEMA_COLUMNS = []PgSchemaColumn{
	{
		ColumnName:      "id",
		DataType:        "character varying",
		UdtName:         "varchar",
		IsNullable:      "NO",
		OrdinalPosition: "1",
	},
	{
		ColumnName:      "name",
		DataType:        "text",
		UdtName:         "text",
		IsNullable:      "NO",
		OrdinalPosition: "2",
	},
	{
		ColumnName:      "int_value",
		DataType:        "integer",
		UdtName:         "int4",
		IsNullable:      "NO",
		OrdinalPosition: "3",
	},
	{
		ColumnName:      "bigint_value",
		DataType:        "bigint",
		UdtName:         "int8",
		IsNullable:      "NO",
		OrdinalPosition: "4",
	},
	{
		ColumnName:       "decimal_value",
		DataType:         "numeric",
		UdtName:          "numeric",
		IsNullable:       "NO",
		OrdinalPosition:  "5",
		NumericPrecision: "10",
		NumericScale:     "2",
	},
	{
		ColumnName:        "timestamp_without_tz_value",
		DataType:          "timestamp without time zone",
		UdtName:           "timestamp",
		IsNullable:        "NO",
		OrdinalPosition:   "6",
		DatetimePrecision: "6",
	},
	{
		ColumnName:      "user_defined_value",
		DataType:        "USER-DEFINED",
		UdtName:         "user_defined",
		IsNullable:      "NO",
		OrdinalPosition: "7",
	},
}

var TEST_LOADED_ROWS = [][]string{
	{
		"1",
		"metric_1",
		"5",
		"9223372036854775807",
		"5.0",
		"2024-01-01 00:00:00.123456",
		"user_defined",
	},
	{
		"2",
		"metric_2",
		"10",
		"-9223372036854775808",
		"10.0",
		"2024-01-01 00:00:00",
		"user_defined",
	},
	{
		"3",
		"metric_1",
		"5",
		"0",
		"5.0",
		"2024-01-01 00:00:00.000123",
		"user_defined",
	},
	{
		"4",
		"metric_2",
		"10",
		"1",
		"10.0",
		"2024-01-01 00:00:00.123",
		"user_defined",
	},
	{
		"5",
		"metric_1",
		"5",
		"-1",
		"5.0",
		"2024-01-01 00:00:00.000000",
		"user_defined",
	},
}

func init() {
	config := loadTestConfig()
	icebergWriter := NewIcebergWriter(config)

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
	config.IcebergPath = "../iceberg-test"
	config.LogLevel = "ERROR"

	return config
}

func setTestArgs(args []string) {
	os.Args = append([]string{"cmd"}, args...)
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	registerFlags()
	flag.Parse()
}
