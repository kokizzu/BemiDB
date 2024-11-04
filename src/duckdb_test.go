package main

import (
	"testing"
)

func TestNewDuckdb(t *testing.T) {
	t.Run("Creates a new DuckDB instance", func(t *testing.T) {
		config := loadTestConfig()

		duckdb := NewDuckdb(config)

		defer duckdb.Close()
		if duckdb.Db == nil {
			t.Errorf("Expected DuckDB instance to be created")
		}

		row := duckdb.Db.QueryRow("SELECT 1")
		var result int
		err := row.Scan(&result)
		if err != nil {
			t.Errorf("Expected query to succeed")
		}
		if result != 1 {
			t.Errorf("Expected query result to be 1, got %d", result)
		}
	})
}
