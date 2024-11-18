package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	duckDb "github.com/marcboeker/go-duckdb"
	pgQuery "github.com/pganalyze/pg_query_go/v5"
)

const (
	FALLBACK_QUERY = "SELECT 1"
)

type Proxy struct {
	duckdb         *Duckdb
	selectRemapper *SelectRemapper
	config         *Config
}

////////////////////////////////////////////////////////////////////////////////////////////////////

type NullDecimal struct {
	Present bool
	Value   duckDb.Decimal
}

func (nullDecimal *NullDecimal) Scan(value interface{}) error {
	if value == nil {
		nullDecimal.Present = false
		return nil
	}

	nullDecimal.Present = true
	nullDecimal.Value = value.(duckDb.Decimal)
	return nil
}

func (nullDecimal NullDecimal) String() string {
	if nullDecimal.Present {
		return fmt.Sprintf("%v", nullDecimal.Value.Float64())
	}
	return ""
}

////////////////////////////////////////////////////////////////////////////////////////////////////

type NullUint32 struct {
	Present bool
	Value   uint32
}

func (nullUint32 *NullUint32) Scan(value interface{}) error {
	if value == nil {
		nullUint32.Present = false
		return nil
	}

	nullUint32.Present = true
	nullUint32.Value = value.(uint32)
	return nil
}

func (nullUint32 NullUint32) String() string {
	if nullUint32.Present {
		return fmt.Sprintf("%v", nullUint32.Value)
	}
	return ""
}

////////////////////////////////////////////////////////////////////////////////////////////////////

type NullUint64 struct {
	Present bool
	Value   uint64
}

func (nullUint64 *NullUint64) Scan(value interface{}) error {
	if value == nil {
		nullUint64.Present = false
		return nil
	}

	nullUint64.Present = true
	nullUint64.Value = value.(uint64)
	return nil
}

func (nullUint64 NullUint64) String() string {
	if nullUint64.Present {
		return fmt.Sprintf("%v", nullUint64.Value)
	}
	return ""
}

////////////////////////////////////////////////////////////////////////////////////////////////////

type NullArray struct {
	Present bool
	Value   []interface{}
}

func (nullArray *NullArray) Scan(value interface{}) error {
	if value == nil {
		nullArray.Present = false
		return nil
	}

	nullArray.Present = true
	nullArray.Value = value.([]interface{})
	return nil
}

func (nullArray NullArray) String() string {
	if nullArray.Present {
		var stringVals []string
		for _, v := range nullArray.Value {
			switch v.(type) {
			case []uint8:
				stringVals = append(stringVals, fmt.Sprintf("%s", v))
			default:
				stringVals = append(stringVals, fmt.Sprintf("%v", v))
			}
		}
		buffer := &bytes.Buffer{}
		csvWriter := csv.NewWriter(buffer)
		err := csvWriter.Write(stringVals)
		if err != nil {
			return ""
		}
		csvWriter.Flush()
		return "{" + strings.TrimRight(buffer.String(), "\n") + "}"
	}
	return ""
}

////////////////////////////////////////////////////////////////////////////////////////////////////

func NewProxy(config *Config, duckdb *Duckdb, icebergReader *IcebergReader) *Proxy {
	ctx := context.Background()

	schemas, err := icebergReader.Schemas()
	PanicIfError(err)
	for _, schema := range schemas {
		_, err := duckdb.ExecContext(
			ctx,
			"CREATE SCHEMA IF NOT EXISTS \"$schema\"",
			map[string]string{"schema": schema},
		)
		PanicIfError(err)
	}

	icebergSchemaTables, err := icebergReader.SchemaTables()
	PanicIfError(err)
	for _, icebergSchemaTable := range icebergSchemaTables {
		metadataFilePath := icebergReader.MetadataFilePath(icebergSchemaTable)
		_, err := duckdb.ExecContext(
			ctx,
			"CREATE VIEW IF NOT EXISTS \"$schema\".\"$table\" AS SELECT * FROM iceberg_scan('$metadataFilePath', skip_schema_inference = true)",
			map[string]string{"schema": icebergSchemaTable.Schema, "table": icebergSchemaTable.Table, "metadataFilePath": metadataFilePath},
		)
		PanicIfError(err)
	}

	return &Proxy{
		duckdb:         duckdb,
		selectRemapper: &SelectRemapper{config: config, icebergReader: icebergReader},
		config:         config,
	}
}

func (proxy *Proxy) HandleQuery(originalQuery string) ([]pgproto3.Message, error) {
	LogDebug(proxy.config, "Original query:", originalQuery)
	query, err := proxy.remapQuery(originalQuery)
	if err != nil {
		LogError(proxy.config, "Couldn't map query:", originalQuery+"\n"+err.Error())
		return nil, err
	}

	rows, err := proxy.duckdb.QueryContext(context.Background(), query)
	if err != nil {
		LogError(proxy.config, "Couldn't handle query via DuckDB:", query+"\n"+err.Error())

		if err.Error() == "Binder Error: UNNEST requires a single list as input" {
			// https://github.com/duckdb/duckdb/issues/11693
			return proxy.HandleQuery(FALLBACK_QUERY)
		}

		return nil, err
	}
	defer rows.Close()

	cols, err := rows.ColumnTypes()
	if err != nil {
		LogError(proxy.config, "Couldn't get column types", query+"\n"+err.Error())
		return nil, err
	}

	var messages []pgproto3.Message
	messages = append(messages, proxy.generateRowDescription(cols))
	for rows.Next() {
		dataRow, err := proxy.generateDataRow(rows, cols)
		if err != nil {
			LogError(proxy.config, "Couldn't get data row", query+"\n"+err.Error())
			return nil, err
		}
		messages = append(messages, dataRow)
	}
	messages = append(messages, &pgproto3.CommandComplete{CommandTag: []byte(FALLBACK_QUERY)})
	messages = append(messages, &pgproto3.ReadyForQuery{TxStatus: 'I'})
	return messages, nil
}

func (proxy *Proxy) remapQuery(query string) (string, error) {
	queryTree, err := pgQuery.Parse(query)
	if err != nil {
		LogError(proxy.config, "Error parsing query:", query+"\n"+err.Error())
		return "", err
	}

	selectStatement := queryTree.Stmts[0].Stmt.GetSelectStmt()
	if selectStatement != nil {
		queryTree = proxy.selectRemapper.RemapQueryTree(queryTree)
		return pgQuery.Deparse(queryTree)
	}

	LogDebug(proxy.config, queryTree)
	return "", errors.New("Unsupported query type")
}

func (proxy *Proxy) generateRowDescription(cols []*sql.ColumnType) *pgproto3.RowDescription {
	description := pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{}}

	for _, col := range cols {
		description.Fields = append(description.Fields, pgproto3.FieldDescription{
			Name:                 []byte(col.Name()),
			TableOID:             0,
			TableAttributeNumber: 0,
			DataTypeOID:          pgtype.TextOID,
			DataTypeSize:         -1,
			TypeModifier:         -1,
			Format:               0,
		})
	}
	return &description
}

func (proxy *Proxy) generateDataRow(rows *sql.Rows, cols []*sql.ColumnType) (*pgproto3.DataRow, error) {
	valuePtrs := make([]interface{}, len(cols))
	for i, col := range cols {
		switch col.ScanType().String() {
		case "int32":
			var value sql.NullInt32
			valuePtrs[i] = &value
		case "int64", "*big.Int":
			var value sql.NullInt64
			valuePtrs[i] = &value
		case "uint32": // xid
			var value NullUint32
			valuePtrs[i] = &value
		case "uint64": // xid8
			var value NullUint64
			valuePtrs[i] = &value
		case "float64", "float32":
			var value sql.NullFloat64
			valuePtrs[i] = &value
		case "string", "[]uint8": // []uint8 is for uuid
			var value sql.NullString
			valuePtrs[i] = &value
		case "bool":
			var value sql.NullBool
			valuePtrs[i] = &value
		case "time.Time":
			var value sql.NullTime
			valuePtrs[i] = &value
		case "duckdb.Decimal":
			var value NullDecimal
			valuePtrs[i] = &value
		case "[]interface {}":
			var value NullArray
			valuePtrs[i] = &value
		default:
			panic("Unsupported queried type: " + col.ScanType().String())
		}
	}

	err := rows.Scan(valuePtrs...)
	if err != nil {
		return nil, err
	}

	var values [][]byte
	for i, valuePtr := range valuePtrs {
		switch value := valuePtr.(type) {
		case *sql.NullInt32:
			if value.Valid {
				values = append(values, []byte(strconv.Itoa(int(value.Int32))))
			} else {
				values = append(values, nil)
			}
		case *sql.NullInt64:
			if value.Valid {
				values = append(values, []byte(strconv.Itoa(int(value.Int64))))
			} else {
				values = append(values, nil)
			}
		case *NullUint32:
			if value.Present {
				values = append(values, []byte(value.String()))
			} else {
				values = append(values, nil)
			}
		case *NullUint64:
			if value.Present {
				values = append(values, []byte(value.String()))
			} else {
				values = append(values, nil)
			}
		case *sql.NullFloat64:
			if value.Valid {
				values = append(values, []byte(fmt.Sprintf("%v", value.Float64)))
			} else {
				values = append(values, nil)
			}
		case *sql.NullString:
			if value.Valid {
				values = append(values, []byte(value.String))
			} else {
				values = append(values, nil)
			}
		case *sql.NullBool:
			if value.Valid {
				values = append(values, []byte(fmt.Sprintf("%v", value.Bool)))
			} else {
				values = append(values, nil)
			}
		case *sql.NullTime:
			if value.Valid {
				switch cols[i].DatabaseTypeName() {
				case "DATE":
					values = append(values, []byte(value.Time.Format("2006-01-02")))
				case "TIME":
					values = append(values, []byte(value.Time.Format("15:04:05.999999")))
				case "TIMESTAMP":
					values = append(values, []byte(value.Time.Format("2006-01-02 15:04:05.999999")))
				default:
					panic("Unsupported type: " + cols[i].DatabaseTypeName())
				}
			} else {
				values = append(values, nil)
			}
		case *NullDecimal:
			if value.Present {
				values = append(values, []byte(value.String()))
			} else {
				values = append(values, nil)
			}
		case *NullArray:
			if value.Present {
				values = append(values, []byte(value.String()))
			} else {
				values = append(values, nil)
			}
		case *string:
			values = append(values, []byte(*value))
		default:
			panic("Unsupported type: " + cols[i].ScanType().Name())
		}
	}
	dataRow := pgproto3.DataRow{Values: values}

	return &dataRow, nil
}
