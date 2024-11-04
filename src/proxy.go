package main

import (
	"context"
	"database/sql"
	"errors"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
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

func NewProxy(config *Config, duckdb *Duckdb, icebergReader *IcebergReader) *Proxy {
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
	values := make([][]byte, len(cols))
	valuePtrs := make([]interface{}, len(cols))

	for i := range values {
		valuePtrs[i] = &values[i]
	}

	err := rows.Scan(valuePtrs...)
	if err != nil {
		return nil, err
	}

	dataRow := pgproto3.DataRow{Values: values}
	// Convert values to text format
	for i := range values {
		dataRow.Values[i] = []byte(string(values[i]))
	}

	return &dataRow, nil
}
