package main

import (
	"flag"
	"os"
	"slices"
	"strings"
)

const (
	ENV_PORT              = "BEMIDB_PORT"
	ENV_DATABASE          = "BEMIDB_DATABASE"
	ENV_USER              = "BEMIDB_USER"
	ENV_PASSWORD          = "BEMIDB_PASSWORD"
	ENV_INIT_SQL_FILEPATH = "BEMIDB_INIT_SQL"
	ENV_STORAGE_PATH      = "BEMIDB_STORAGE_PATH"
	ENV_LOG_LEVEL         = "BEMIDB_LOG_LEVEL"
	ENV_STORAGE_TYPE      = "BEMIDB_STORAGE_TYPE"

	ENV_AWS_REGION            = "AWS_REGION"
	ENV_AWS_S3_BUCKET         = "AWS_S3_BUCKET"
	ENV_AWS_ACCESS_KEY_ID     = "AWS_ACCESS_KEY_ID"
	ENV_AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY"

	ENV_PG_DATABASE_URL  = "PG_DATABASE_URL"
	ENV_PG_SYNC_INTERVAL = "PG_SYNC_INTERVAL"
	ENV_PG_SCHEMA_PREFIX = "PG_SCHEMA_PREFIX"

	DEFAULT_PORT              = "54321"
	DEFAULT_DATABASE          = "bemidb"
	DEFAULT_USER              = ""
	DEFAULT_PASSWORD          = ""
	DEFAULT_INIT_SQL_FILEPATH = "./init.sql"
	DEFAULT_STORAGE_PATH      = "iceberg"
	DEFAULT_LOG_LEVEL         = "INFO"
	DEFAULT_DB_STORAGE_TYPE   = "LOCAL"

	STORAGE_TYPE_LOCAL = "LOCAL"
	STORAGE_TYPE_S3    = "S3"
)

type AwsConfig struct {
	Region          string
	S3Bucket        string
	AccessKeyId     string
	SecretAccessKey string
}

type PgConfig struct {
	DatabaseUrl  string
	SyncInterval string // optional
	SchemaPrefix string // optional
}

type Config struct {
	Port              string
	Database          string
	User              string
	EncryptedPassword string
	InitSqlFilepath   string
	LogLevel          string
	StorageType       string
	StoragePath       string
	Aws               AwsConfig
	Pg                PgConfig
}

var _config Config
var _password string

func init() {
	registerFlags()
}

func registerFlags() {
	flag.StringVar(&_config.Port, "port", os.Getenv(ENV_PORT), "Port for BemiDB to listen on. Default: \""+DEFAULT_PORT+"\"")
	flag.StringVar(&_config.Database, "database", os.Getenv(ENV_DATABASE), "Database name. Default: \""+DEFAULT_DATABASE+"\"")
	flag.StringVar(&_config.User, "user", os.Getenv(ENV_USER), "Database user. Default: \""+DEFAULT_USER+"\"")
	flag.StringVar(&_password, "password", os.Getenv(ENV_PASSWORD), "Database password. Default: \""+DEFAULT_PASSWORD+"\"")
	flag.StringVar(&_config.StoragePath, "storage-path", os.Getenv(ENV_STORAGE_PATH), "Path to the storage folder. Default: \""+DEFAULT_STORAGE_PATH+"\"")
	flag.StringVar(&_config.InitSqlFilepath, "init-sql", os.Getenv(ENV_INIT_SQL_FILEPATH), "Path to the initialization SQL file. Default: \""+DEFAULT_INIT_SQL_FILEPATH+"\"")
	flag.StringVar(&_config.LogLevel, "log-level", os.Getenv(ENV_LOG_LEVEL), "Log level: \"DEBUG\", \"INFO\", \"ERROR\". Default: \""+DEFAULT_LOG_LEVEL+"\"")
	flag.StringVar(&_config.StorageType, "storage-type", os.Getenv(ENV_STORAGE_TYPE), "Storage type: \"LOCAL\", \"S3\". Default: \""+DEFAULT_DB_STORAGE_TYPE+"\"")
	flag.StringVar(&_config.Pg.SchemaPrefix, "pg-schema-prefix", os.Getenv(ENV_PG_SCHEMA_PREFIX), "(Optional) Prefix for PostgreSQL schema names")
	flag.StringVar(&_config.Pg.SyncInterval, "pg-sync-interval", os.Getenv(ENV_PG_SYNC_INTERVAL), "(Optional) Interval between syncs. Valid units: \"ns\", \"us\" (or \"µs\"), \"ms\", \"s\", \"m\", \"h\"")
	flag.StringVar(&_config.Pg.DatabaseUrl, "pg-database-url", os.Getenv(ENV_PG_DATABASE_URL), "PostgreSQL database URL")
	flag.StringVar(&_config.Aws.Region, "aws-region", os.Getenv(ENV_AWS_REGION), "AWS region")
	flag.StringVar(&_config.Aws.S3Bucket, "aws-s3-bucket", os.Getenv(ENV_AWS_S3_BUCKET), "AWS S3 bucket name")
	flag.StringVar(&_config.Aws.AccessKeyId, "aws-access-key-id", os.Getenv(ENV_AWS_ACCESS_KEY_ID), "AWS access key ID")
	flag.StringVar(&_config.Aws.SecretAccessKey, "aws-secret-access-key", os.Getenv(ENV_AWS_SECRET_ACCESS_KEY), "AWS secret access key")
}

func parseFlags() {
	flag.Parse()

	if _config.Port == "" {
		_config.Port = DEFAULT_PORT
	}
	if _config.Database == "" {
		_config.Database = DEFAULT_DATABASE
	}
	if _config.User == "" {
		_config.User = DEFAULT_USER
	}
	if _password == "" {
		_password = DEFAULT_PASSWORD
	}
	if _password != "" {
		if _config.User == "" {
			panic("Password is set without a user")
		}
		_config.EncryptedPassword = StringToScramSha256(_password)
		_password = ""
	}
	if _config.StoragePath == "" {
		_config.StoragePath = DEFAULT_STORAGE_PATH
	}
	if _config.InitSqlFilepath == "" {
		_config.InitSqlFilepath = DEFAULT_INIT_SQL_FILEPATH
	}
	if _config.LogLevel == "" {
		_config.LogLevel = DEFAULT_LOG_LEVEL
	} else if !slices.Contains(LOG_LEVELS, _config.LogLevel) {
		panic("Invalid log level " + _config.LogLevel + ". Must be one of " + strings.Join(LOG_LEVELS, ", "))
	}
	if _config.StorageType == "" {
		_config.StorageType = DEFAULT_DB_STORAGE_TYPE
	} else if !slices.Contains(STORAGE_TYPES, _config.StorageType) {
		panic("Invalid storage type " + _config.StorageType + ". Must be one of " + strings.Join(STORAGE_TYPES, ", "))
	}

	if _config.StorageType == STORAGE_TYPE_S3 {
		if _config.Aws.Region == "" {
			panic("AWS region is required")
		}
		if _config.Aws.S3Bucket == "" {
			panic("AWS S3 bucket name is required")
		}
		if _config.Aws.AccessKeyId == "" {
			panic("AWS access key ID is required")
		}
		if _config.Aws.SecretAccessKey == "" {
			panic("AWS secret access key is required")
		}
	}
}

func LoadConfig(reRegisterFlags ...bool) *Config {
	if reRegisterFlags != nil && reRegisterFlags[0] {
		flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
		registerFlags()
	}
	parseFlags()
	return &_config
}
