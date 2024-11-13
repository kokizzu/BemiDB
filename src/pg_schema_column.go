package main

import (
	"encoding/csv"
	"strconv"
	"strings"
	"time"
)

const (
	PG_NULL_STRING = "BEMIDB_NULL"

	PG_SCHEMA_TRUE  = "YES"
	PG_SCHEMA_FALSE = "FALSE"

	PG_DATA_TYPE_ARRAY        = "ARRAY"
	PG_DATA_TYPE_USER_DEFINED = "USER-DEFINED"

	PARQUET_SCHEMA_REPETITION_TYPE_REQUIRED = "REQUIRED"
	PARQUET_SCHEMA_REPETITION_TYPE_OPTIONAL = "OPTIONAL"
	PARQUET_SCHEMA_REPETITION_TYPE_REPEATED = "REPEATED"

	// 0000-01-01 00:00:00 +0000 UTC
	EPOCH_TIME_MS = -62167219200000
)

type PgSchemaColumn struct {
	ColumnName             string
	DataType               string
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
	Id       int         `json:"id"`
	Name     string      `json:"name"`
	Type     interface{} `json:"type"`
	Required bool        `json:"required"`
}

func (pgSchemaColumn PgSchemaColumn) ToParquetSchemaFieldMap() map[string]interface{} {
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

	return map[string]interface{}{
		"Tag": strings.Join(keyVals, ", "),
	}
}

func (pgSchemaColumn PgSchemaColumn) ToIcebergSchemaFieldMap() IcebergSchemaField {
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

	primitiveType := pgSchemaColumn.icebergPrimitiveType()
	if pgSchemaColumn.DataType == PG_DATA_TYPE_ARRAY {
		icebergSchemaField.Type = map[string]interface{}{
			"type":             "list",
			"element":          primitiveType,
			"element-id":       pgSchemaColumn.OrdinalPosition,
			"element-required": false,
		}
	} else {
		icebergSchemaField.Type = primitiveType
	}

	return icebergSchemaField
}

func (pgSchemaColumn *PgSchemaColumn) FormatParquetValue(value string) interface{} {
	if value == PG_NULL_STRING {
		return nil
	}

	if pgSchemaColumn.DataType == PG_DATA_TYPE_ARRAY {
		var values []interface{}

		csvString := strings.Trim(value, "{}")
		if csvString == "" {
			return values
		}

		csvReader := csv.NewReader(strings.NewReader(csvString))
		stringValues, err := csvReader.Read()
		PanicIfError(err)

		for _, stringValue := range stringValues {
			values = append(values, pgSchemaColumn.parquetPrimitiveValue(stringValue))
		}

		return values
	}

	return pgSchemaColumn.parquetPrimitiveValue(value)
}

func (pgSchemaColumn *PgSchemaColumn) toParquetSchemaField() ParquetSchemaField {
	parquetSchemaField := ParquetSchemaField{
		Name:    pgSchemaColumn.ColumnName,
		FieldId: pgSchemaColumn.OrdinalPosition,
		Type:    pgSchemaColumn.parquetPrimitiveType(),
	}

	// Set RepetitionType
	if pgSchemaColumn.IsNullable == PG_SCHEMA_TRUE {
		parquetSchemaField.RepetitionType = PARQUET_SCHEMA_REPETITION_TYPE_OPTIONAL
	} else {
		parquetSchemaField.RepetitionType = PARQUET_SCHEMA_REPETITION_TYPE_REQUIRED
	}

	// Set other field properties
	switch pgSchemaColumn.UdtName {
	case "varchar", "char", "text", "bytea", "jsonb", "json", "bpchar", "tsvector", "interval":
		parquetSchemaField.ConvertedType = "UTF8"
	case "numeric":
		parquetSchemaField.ConvertedType = "DECIMAL"
		parquetSchemaField.Scale = pgSchemaColumn.NumericScale
		parquetSchemaField.Precision = pgSchemaColumn.NumericPrecision
		scale, err := strconv.Atoi(pgSchemaColumn.NumericScale)
		PanicIfError(err)
		precision, err := strconv.Atoi(pgSchemaColumn.NumericPrecision)
		PanicIfError(err)
		parquetSchemaField.Length = strconv.Itoa(scale + precision)
	case "uuid":
		parquetSchemaField.Length = "36"
	case "date":
		parquetSchemaField.ConvertedType = "DATE"
	case "timestamp", "timestamptz":
		if pgSchemaColumn.DatetimePrecision == "6" {
			parquetSchemaField.ConvertedType = "TIMESTAMP_MICROS"
		} else {
			parquetSchemaField.ConvertedType = "TIMESTAMP_MILLIS"
		}
	case "time", "timetz":
		if pgSchemaColumn.DatetimePrecision == "6" {
			parquetSchemaField.ConvertedType = "TIME_MICROS"
		} else {
			parquetSchemaField.ConvertedType = "TIME_MILLIS"
		}
	default:
		if pgSchemaColumn.DataType == PG_DATA_TYPE_ARRAY {
			parquetSchemaField.RepetitionType = PARQUET_SCHEMA_REPETITION_TYPE_REPEATED
		} else if pgSchemaColumn.DataType == PG_DATA_TYPE_USER_DEFINED {
			parquetSchemaField.ConvertedType = "UTF8"
		}
	}

	return parquetSchemaField
}

func (pgSchemaColumn *PgSchemaColumn) parquetPrimitiveValue(value string) interface{} {
	switch strings.TrimLeft(pgSchemaColumn.UdtName, "_") {
	case "varchar", "char", "text", "bytea", "jsonb", "json", "tsvector", "numeric", "uuid", "interval":
		return value
	case "bpchar":
		trimmedValue := strings.TrimRight(value, " ")
		return trimmedValue
	case "int2", "int4":
		intValue, err := strconv.Atoi(value)
		PanicIfError(err)
		return int32(intValue)
	case "int8":
		intValue, err := strconv.ParseInt(value, 10, 64)
		PanicIfError(err)
		return intValue
	case "float4":
		floatValue, err := strconv.ParseFloat(value, 32)
		PanicIfError(err)
		return float32(floatValue)
	case "float8":
		floatValue, err := strconv.ParseFloat(value, 64)
		PanicIfError(err)
		return float64(floatValue)
	case "bool":
		boolValue, err := strconv.ParseBool(value)
		PanicIfError(err)
		return boolValue
	case "timestamp":
		if pgSchemaColumn.DatetimePrecision == "6" {
			parsedTime, err := time.Parse("2006-01-02 15:04:05.999999", value)
			PanicIfError(err)
			return parsedTime.UnixMicro()
		} else {
			parsedTime, err := time.Parse("2006-01-02 15:04:05.999", value)
			PanicIfError(err)
			return parsedTime.UnixMilli()
		}
	case "timestamptz":
		if pgSchemaColumn.DatetimePrecision == "6" {
			parsedTime, err := time.Parse("2006-01-02 15:04:05.999999-07", value)
			PanicIfError(err)
			return parsedTime.UnixMicro()
		} else {
			parsedTime, err := time.Parse("2006-01-02 15:04:05.999-07", value)
			PanicIfError(err)
			return parsedTime.UnixMilli()
		}
	case "time":
		if pgSchemaColumn.DatetimePrecision == "6" {
			parsedTime, err := time.Parse("15:04:05.999999", value)
			PanicIfError(err)
			return int64(-EPOCH_TIME_MS*1000 + parsedTime.UnixMicro())
		} else {
			parsedTime, err := time.Parse("15:04:05.999", value)
			PanicIfError(err)
			return -EPOCH_TIME_MS + parsedTime.UnixMilli()
		}
	case "timetz":
		if pgSchemaColumn.DatetimePrecision == "6" {
			parsedTime, err := time.Parse("15:04:05.999999-07", value)
			PanicIfError(err)
			return int64(-EPOCH_TIME_MS*1000 + parsedTime.UnixMicro())
		} else {
			parsedTime, err := time.Parse("15:04:05.999-07", value)
			PanicIfError(err)
			return -EPOCH_TIME_MS + parsedTime.UnixMilli()
		}
	case "date":
		parsedTime, err := time.Parse("2006-01-02", value)
		PanicIfError(err)
		return parsedTime.Unix() / 86400
	default:
		if pgSchemaColumn.DataType == PG_DATA_TYPE_USER_DEFINED {
			return value
		}
	}

	panic("Unsupported PostgreSQL value: " + value)
}

func (pgSchemaColumn *PgSchemaColumn) parquetPrimitiveType() string {
	switch strings.TrimLeft(pgSchemaColumn.UdtName, "_") {
	case "varchar", "char", "text", "bpchar", "bytea", "interval", "jsonb", "json", "tsvector":
		return "BYTE_ARRAY"
	case "int2", "int4", "date":
		return "INT32"
	case "int8":
		return "INT64"
	case "float4", "float8":
		return "FLOAT"
	case "numeric", "uuid":
		return "FIXED_LEN_BYTE_ARRAY"
	case "bool":
		return "BOOLEAN"
	case "time", "timetz":
		if pgSchemaColumn.DatetimePrecision == "6" {
			return "INT64"
		} else {
			return "INT32"
		}
	case "timestamp", "timestamptz":
		return "INT64"
	default:
		if pgSchemaColumn.DataType == PG_DATA_TYPE_USER_DEFINED {
			return "BYTE_ARRAY"
		}
	}

	panic("Unsupported PostgreSQL type: " + pgSchemaColumn.UdtName)
}

func (pgSchemaColumn *PgSchemaColumn) icebergPrimitiveType() string {
	switch strings.TrimLeft(pgSchemaColumn.UdtName, "_") {
	case "varchar", "char", "text", "interval", "jsonb", "json", "bpchar", "tsvector":
		return "string"
	case "uuid":
		return "uuid"
	case "int2", "int4":
		return "int"
	case "int8":
		return "long"
	case "float4", "float8":
		return "float"
	case "numeric":
		return "decimal(" + pgSchemaColumn.NumericPrecision + ", " + pgSchemaColumn.NumericScale + ")"
	case "bool":
		return "boolean"
	case "date":
		return "date"
	case "bytea":
		return "binary"
	case "timestamp", "timestamptz":
		if pgSchemaColumn.DatetimePrecision == "9" {
			return "timestamp_ns"
		} else {
			return "timestamp"
		}
	case "time", "timetz":
		return "time"
	default:
		if pgSchemaColumn.DataType == PG_DATA_TYPE_USER_DEFINED {
			return "string"
		}
	}

	panic("Unsupported PostgreSQL type: " + pgSchemaColumn.UdtName)
}
