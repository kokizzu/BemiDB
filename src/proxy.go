package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"

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

type NullDecimal struct {
	Valid bool
	duckDb.Decimal
}

func (nullDecimal *NullDecimal) Scan(value interface{}) error {
	if value == nil {
		nullDecimal.Valid = false
		return nil
	}

	nullDecimal.Valid = true
	nullDecimal.Decimal = value.(duckDb.Decimal)
	return nil
}

func (nullDecimal NullDecimal) String() string {
	if nullDecimal.Valid {
		return fmt.Sprintf("%v", nullDecimal.Decimal.Float64())
	}
	return ""
}

func NewProxy(config *Config, duckdb *Duckdb, icebergReader *IcebergReader) *Proxy {
	schemaTables, err := icebergReader.SchemaTables()
	PanicIfError(err)
	for _, schemaTable := range schemaTables {
		metadataFilePath := icebergReader.MetadataFilePath(schemaTable)
		query := "CREATE VIEW IF NOT EXISTS " + schemaTable.Schema + "." + schemaTable.Table + " AS SELECT * FROM iceberg_scan('" + metadataFilePath + "')"
		LogDebug(config, "Querying DuckDB:", query)
		duckdb.Db.ExecContext(context.Background(), query)
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

	LogDebug(proxy.config, "Querying DuckDB:", query)
	rows, err := proxy.duckdb.Db.QueryContext(context.Background(), query)
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
		case "float64", "float32":
			var value sql.NullFloat64
			valuePtrs[i] = &value
		case "string":
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
		default:
			panic("Unsupported type: " + col.ScanType().String())
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
				default:
					panic("Unsupported type: " + cols[i].DatabaseTypeName())
				}
			} else {
				values = append(values, nil)
			}
		case *NullDecimal:
			if value.Valid {
				values = append(values, []byte(value.String()))
			} else {
				values = append(values, nil)
			}
		default:
			panic("Unsupported type: " + cols[i].ScanType().Name())
		}
	}
	dataRow := pgproto3.DataRow{Values: values}

	return &dataRow, nil
}
