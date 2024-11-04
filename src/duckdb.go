package main

import (
	"bufio"
	"context"
	"database/sql"
	"os"
	"strings"

	_ "github.com/marcboeker/go-duckdb"
)

var DEFAULT_BOOT_QUERIES = []string{
	"INSTALL iceberg",
	"LOAD iceberg",
	"CREATE SCHEMA public",
	"USE public",
}

type Duckdb struct {
	Db     *sql.DB
	config *Config
}

func NewDuckdb(config *Config) *Duckdb {
	bootQueries := readDuckdbInitFile(config)
	if bootQueries == nil {
		bootQueries = DEFAULT_BOOT_QUERIES
	}

	ctx := context.Background()
	db, err := sql.Open("duckdb", "")
	PanicIfError(err)

	for _, query := range bootQueries {
		LogDebug(config, "Querying DuckDB:", query)
		_, err := db.ExecContext(ctx, query)
		PanicIfError(err)
	}

	switch config.StorageType {
	case STORAGE_TYPE_AWS_S3:
		query := "CREATE SECRET aws_s3_secret (TYPE S3, KEY_ID $access_key_id, SECRET $secret_access_key, REGION $region, SCOPE $s3_bucket)"
		_, err = db.ExecContext(ctx, replaceNamedStringArgs(query, map[string]string{
			"access_key_id":     config.Aws.AccessKeyId,
			"secret_access_key": config.Aws.SecretAccessKey,
			"region":            config.Aws.Region,
			"s3_bucket":         "s3://" + config.Aws.S3Bucket,
		}))
		LogDebug(config, "Querying DuckDB:", query)
		PanicIfError(err)
	}

	return &Duckdb{
		Db:     db,
		config: config,
	}
}

func (duckdb *Duckdb) Close() {
	duckdb.Db.Close()
}

func replaceNamedStringArgs(query string, args map[string]string) string {
	for key, value := range args {
		query = strings.ReplaceAll(query, "$"+key, "'"+value+"'")
	}
	return query
}

func readDuckdbInitFile(config *Config) []string {
	_, err := os.Stat(config.InitSqlFilepath)
	if err != nil {
		if os.IsNotExist(err) {
			LogDebug(config, "DuckDB: No init file found at", config.InitSqlFilepath)
			return nil
		}
		PanicIfError(err)
	}

	LogInfo(config, "DuckDB: Reading init file", config.InitSqlFilepath)
	file, err := os.Open(config.InitSqlFilepath)
	PanicIfError(err)
	defer file.Close()

	lines := []string{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	PanicIfError(scanner.Err())
	return lines
}
