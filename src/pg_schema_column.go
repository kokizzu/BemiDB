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
	Name                string
	Type                string
	RepetitionType      string
	FieldId             string
	Length              string
	ConvertedType       string
	Scale               string
	Precision           string
	NestedType          string
	NestedConvertedType string
}

type IcebergSchemaField struct {
	Id       int         `json:"id"`
	Name     string      `json:"name"`
	Type     interface{} `json:"type"`
	Required bool        `json:"required"`
}

func (pgSchemaColumn PgSchemaColumn) ToParquetSchemaFieldMap() map[string]interface{} {
	field := pgSchemaColumn.toParquetSchemaField()

	tagKeyVals := []string{
		"name=" + field.Name,
		"type=" + field.Type,
		"repetitiontype=" + field.RepetitionType,
		"fieldid=" + field.FieldId,
	}

	if field.Length != "" {
		tagKeyVals = append(tagKeyVals, "length="+field.Length)
	}
	if field.ConvertedType != "" {
		tagKeyVals = append(tagKeyVals, "convertedtype="+field.ConvertedType)
	}
	if field.Scale != "" {
		tagKeyVals = append(tagKeyVals, "scale="+field.Scale)
	}
	if field.Precision != "" {
		tagKeyVals = append(tagKeyVals, "precision="+field.Precision)
	}

	result := map[string]interface{}{
		"Tag": strings.Join(tagKeyVals, ", "),
	}

	if field.NestedType != "" {
		nestedTagKeyVals := []string{
			"name=element",
			"type=" + field.NestedType,
		}

		if field.NestedConvertedType != "" {
			nestedTagKeyVals = append(nestedTagKeyVals, "convertedtype="+field.NestedConvertedType)
		}

		result["Fields"] = []map[string]interface{}{
			{"Tag": strings.Join(nestedTagKeyVals, ", ")},
		}
	}

	return result
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
	primitiveType, primitiveConvertedType := pgSchemaColumn.parquetPrimitiveTypes()

	parquetSchemaField := ParquetSchemaField{
		Name:          pgSchemaColumn.ColumnName,
		FieldId:       pgSchemaColumn.OrdinalPosition,
		Type:          primitiveType,
		ConvertedType: primitiveConvertedType,
	}

	// Set RepetitionType
	if pgSchemaColumn.IsNullable == PG_SCHEMA_TRUE {
		parquetSchemaField.RepetitionType = PARQUET_SCHEMA_REPETITION_TYPE_OPTIONAL
	} else {
		parquetSchemaField.RepetitionType = PARQUET_SCHEMA_REPETITION_TYPE_REQUIRED
	}

	// Set other field properties
	switch pgSchemaColumn.UdtName {
	case "numeric":
		parquetSchemaField.Scale = pgSchemaColumn.NumericScale
		parquetSchemaField.Precision = pgSchemaColumn.NumericPrecision
		scale, err := strconv.Atoi(pgSchemaColumn.NumericScale)
		PanicIfError(err)
		precision, err := strconv.Atoi(pgSchemaColumn.NumericPrecision)
		PanicIfError(err)
		parquetSchemaField.Length = strconv.Itoa(scale + precision)
	case "uuid":
		parquetSchemaField.Length = "36"
	default:
		if pgSchemaColumn.DataType == PG_DATA_TYPE_ARRAY {
			parquetSchemaField.NestedType = parquetSchemaField.Type
			parquetSchemaField.NestedConvertedType = parquetSchemaField.ConvertedType
			parquetSchemaField.Type = "LIST"
			parquetSchemaField.ConvertedType = ""
		}
	}

	return parquetSchemaField
}

func (pgSchemaColumn *PgSchemaColumn) parquetPrimitiveValue(value string) interface{} {
	switch strings.TrimLeft(pgSchemaColumn.UdtName, "_") {
	case "varchar", "char", "text", "bytea", "jsonb", "json", "numeric", "uuid", "interval",
		"point", "line", "lseg", "box", "path", "polygon", "circle",
		"cidr", "inet", "macaddr", "macaddr8",
		"tsvector", "ltree", "pg_snapshot":
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
	case "xid":
		intValue, err := strconv.ParseUint(value, 10, 32)
		PanicIfError(err)
		return intValue
	case "xid8":
		intValue, err := strconv.ParseUint(value, 10, 64)
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

func (pgSchemaColumn *PgSchemaColumn) parquetPrimitiveTypes() (primitiveType string, primitiveConvertedType string) {
	switch strings.TrimLeft(pgSchemaColumn.UdtName, "_") {
	case "varchar", "char", "text", "bpchar", "bytea", "interval", "jsonb", "json",
		"point", "line", "lseg", "box", "path", "polygon", "circle",
		"cidr", "inet", "macaddr", "macaddr8",
		"tsvector", "ltree", "pg_snapshot":
		return "BYTE_ARRAY", "UTF8"
	case "date":
		return "INT32", "DATE"
	case "int2", "int4":
		return "INT32", ""
	case "int8":
		return "INT64", ""
	case "float4":
		return "FLOAT", ""
	case "float8":
		return "DOUBLE", ""
	case "numeric":
		return "FIXED_LEN_BYTE_ARRAY", "DECIMAL"
	case "xid":
		return "INT32", "UINT_32"
	case "xid8":
		return "INT64", "UINT_64"
	case "uuid":
		return "FIXED_LEN_BYTE_ARRAY", ""
	case "bool":
		return "BOOLEAN", ""
	case "time", "timetz":
		if pgSchemaColumn.DatetimePrecision == "6" {
			return "INT64", "TIME_MICROS"
		} else {
			return "INT32", "TIME_MILLIS"
		}
	case "timestamp", "timestamptz":
		if pgSchemaColumn.DatetimePrecision == "6" {
			return "INT64", "TIMESTAMP_MICROS"
		} else {
			return "INT64", "TIMESTAMP_MILLIS"
		}
	default:
		if pgSchemaColumn.DataType == PG_DATA_TYPE_USER_DEFINED {
			return "BYTE_ARRAY", "UTF8"
		}
	}

	panic("Unsupported PostgreSQL type: " + pgSchemaColumn.UdtName)
}

func (pgSchemaColumn *PgSchemaColumn) icebergPrimitiveType() string {
	switch strings.TrimLeft(pgSchemaColumn.UdtName, "_") {
	case "varchar", "char", "text", "interval", "jsonb", "json", "bpchar",
		"point", "line", "lseg", "box", "path", "polygon", "circle",
		"cidr", "inet", "macaddr", "macaddr8",
		"tsvector", "ltree", "pg_snapshot":
		return "string"
	case "uuid":
		return "uuid"
	case "int2", "int4", "xid":
		return "int"
	case "int8", "xid8":
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
