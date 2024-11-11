package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	PG_SCHEMA_TRUE  = "YES"
	PG_SCHEMA_FALSE = "FALSE"

	PARQUET_SCHEMA_REPETITION_TYPE_REQUIRED = "REQUIRED"
	PARQUET_SCHEMA_REPETITION_TYPE_OPTIONAL = "OPTIONAL"
	PARQUET_SCHEMA_REPETITION_TYPE_REPEATED = "REPEATED"
)

type PgSchemaColumn struct {
	ColumnName             string
	UdtName                string
	IsNullable             string
	OrdinalPosition        string
	CharacterMaximumLength string
	NumericPrecision       string
	NumericScale           string
	DatetimePrecision      string
}

type ParquetSchemaField struct {
	Name           string
	Type           string
	RepetitionType string
	FieldId        string
	Length         string
	ConvertedType  string
	Scale          string
	Precision      string
}

type IcebergSchemaField struct {
	Id       int
	Name     string
	Type     string
	Required bool
}

func (pgSchemaColumn PgSchemaColumn) ToStringParquetSchemaField() string {
	field := pgSchemaColumn.toParquetSchemaField()

	keyVals := []string{
		"name=" + field.Name,
		"type=" + field.Type,
		"repetitiontype=" + field.RepetitionType,
		"fieldid=" + field.FieldId,
	}

	if field.Length != "" {
		keyVals = append(keyVals, "length="+field.Length)
	}
	if field.ConvertedType != "" {
		keyVals = append(keyVals, "convertedtype="+field.ConvertedType)
	}
	if field.Scale != "" {
		keyVals = append(keyVals, "scale="+field.Scale)
	}
	if field.Precision != "" {
		keyVals = append(keyVals, "precision="+field.Precision)
	}

	return strings.Join(keyVals, ", ")
}

func (pgSchemaColumn PgSchemaColumn) ToMapIcebergSchemaField() map[string]interface{} {
	field := pgSchemaColumn.toIcebergSchemaField()

	return map[string]interface{}{
		"id":       field.Id,
		"name":     field.Name,
		"type":     field.Type,
		"required": field.Required,
	}
}

func (pgSchemaColumn *PgSchemaColumn) FormatParquetValue(value string) *string {
	if value == "" && pgSchemaColumn.IsNullable == PG_SCHEMA_TRUE {
		// Convert optional empty row string value to nil
		return nil
	}

	switch pgSchemaColumn.UdtName {
	case "timestamp", "timestamptz":
		if pgSchemaColumn.DatetimePrecision == "6" {
			parsedTime, err := time.Parse("2006-01-02 15:04:05.999999-07", value)
			PanicIfError(err)

			timestamp := strconv.FormatInt(parsedTime.UnixMicro(), 10)
			return &timestamp
		} else {
			parsedTime, err := time.Parse("2006-01-02 15:04:05.999-07", value)
			PanicIfError(err)

			timestamp := strconv.FormatInt(parsedTime.UnixMilli(), 10)
			return &timestamp
		}
	case "date":
		parsedTime, err := time.Parse("2006-01-02", value)
		PanicIfError(err)
		date := fmt.Sprintf("%d", parsedTime.Unix()/86400)
		return &date
	case "bpchar":
		trimmedValue := strings.TrimRight(value, " ")
		return &trimmedValue
	default:
		if strings.HasPrefix(pgSchemaColumn.UdtName, "_") {
			switch strings.TrimLeft(pgSchemaColumn.UdtName, "_") {
			case "int2", "int4", "int8", "float4", "float8", "numeric", "bool":
				return &value
			default:
				// Wrap array values in string double quotes and square brackets
				value = strings.Trim(value, "{}")
				values := strings.Split(value, ",")
				for i, v := range values {
					values[i] = "\"" + v + "\""
				}
				value = "[" + strings.Join(values, ",") + "]"
				return &value
			}
		}
	}

	return &value
}

func (pgSchemaColumn PgSchemaColumn) toIcebergSchemaField() IcebergSchemaField {
	icebergSchemaField := IcebergSchemaField{}

	id, err := strconv.Atoi(pgSchemaColumn.OrdinalPosition)
	if err != nil {
		panic(err)
	}

	icebergSchemaField.Id = id
	icebergSchemaField.Name = pgSchemaColumn.ColumnName

	if pgSchemaColumn.IsNullable == PG_SCHEMA_TRUE {
		icebergSchemaField.Required = false
	} else {
		icebergSchemaField.Required = true
	}

	switch pgSchemaColumn.UdtName {
	case "varchar", "char", "text", "jsonb", "json", "uuid", "bpchar":
		icebergSchemaField.Type = "string"
	case "int2", "int4":
		icebergSchemaField.Type = "int"
	case "int8":
		icebergSchemaField.Type = "long"
	case "float4", "float8":
		icebergSchemaField.Type = "float"
	case "numeric":
		icebergSchemaField.Type = "decimal(" + pgSchemaColumn.NumericPrecision + ", " + pgSchemaColumn.NumericScale + ")"
	case "bool":
		icebergSchemaField.Type = "boolean"
	case "date":
		icebergSchemaField.Type = "date"
	case "bytea":
		icebergSchemaField.Type = "binary"
	case "timestamp", "timestamptz":
		if pgSchemaColumn.DatetimePrecision == "9" {
			icebergSchemaField.Type = "timestamp_ns"
		} else {
			icebergSchemaField.Type = "timestamp"
		}
	case "time", "timetz":
		icebergSchemaField.Type = "time"
	default:
		if strings.HasPrefix(pgSchemaColumn.UdtName, "_") {
			icebergSchemaField.Type = "string"
		} else {
			panic("Unsupported PostgreSQL type: " + pgSchemaColumn.UdtName)
		}
	}

	return icebergSchemaField
}

func (pgSchemaColumn *PgSchemaColumn) toParquetSchemaField() ParquetSchemaField {
	parquetSchemaField := ParquetSchemaField{
		Name:    pgSchemaColumn.ColumnName,
		FieldId: pgSchemaColumn.OrdinalPosition,
	}

	if pgSchemaColumn.IsNullable == "YES" {
		parquetSchemaField.RepetitionType = PARQUET_SCHEMA_REPETITION_TYPE_OPTIONAL
	} else {
		parquetSchemaField.RepetitionType = PARQUET_SCHEMA_REPETITION_TYPE_REQUIRED
	}

	switch pgSchemaColumn.UdtName {
	case "varchar", "char", "text", "bytea", "jsonb", "json", "bpchar":
		parquetSchemaField.Type = "BYTE_ARRAY"
		parquetSchemaField.ConvertedType = "UTF8"
	case "int2", "int4":
		parquetSchemaField.Type = "INT32"
	case "int8":
		parquetSchemaField.Type = "INT64"
	case "float4", "float8":
		parquetSchemaField.Type = "FLOAT"
	case "numeric":
		parquetSchemaField.Type = "FIXED_LEN_BYTE_ARRAY"
		parquetSchemaField.ConvertedType = "DECIMAL"
		parquetSchemaField.Scale = pgSchemaColumn.NumericScale
		parquetSchemaField.Precision = pgSchemaColumn.NumericPrecision
		scale, err := strconv.Atoi(pgSchemaColumn.NumericScale)
		PanicIfError(err)
		precision, err := strconv.Atoi(pgSchemaColumn.NumericPrecision)
		PanicIfError(err)
		parquetSchemaField.Length = strconv.Itoa(scale + precision)
	case "bool":
		parquetSchemaField.Type = "BOOLEAN"
	case "uuid":
		parquetSchemaField.Type = "FIXED_LEN_BYTE_ARRAY"
		parquetSchemaField.Length = "36"
	case "date":
		parquetSchemaField.Type = "INT32"
		parquetSchemaField.ConvertedType = "DATE"
	case "timestamp", "timestamptz":
		parquetSchemaField.Type = "INT64"
		if pgSchemaColumn.DatetimePrecision == "6" {
			parquetSchemaField.ConvertedType = "TIMESTAMP_MICROS"
		} else {
			parquetSchemaField.ConvertedType = "TIMESTAMP_MILLIS"
		}
	case "time", "timetz":
		parquetSchemaField.Type = "INT64"
		if pgSchemaColumn.DatetimePrecision == "6" {
			parquetSchemaField.ConvertedType = "TIME_MICROS"
		} else {
			parquetSchemaField.ConvertedType = "TIME_MILLIS"
		}

	case "interval":
		parquetSchemaField.Type = "BYTE_ARRAY"
		parquetSchemaField.ConvertedType = "INTERVAL"
	default:
		if strings.HasPrefix(pgSchemaColumn.UdtName, "_") {
			parquetSchemaField.Type = "BYTE_ARRAY"
			parquetSchemaField.ConvertedType = "UTF8"
		} else {
			panic("Unsupported PostgreSQL type: " + pgSchemaColumn.UdtName)
		}
	}

	return parquetSchemaField
}
