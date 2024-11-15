package main

import (
	"github.com/xitongsys/parquet-go/parquet"
)

const (
	PARQUET_PARALLEL_NUMBER  = 4
	PARQUET_ROW_GROUP_SIZE   = 128 * 1024 * 1024 // 128 MB
	PARQUET_COMPRESSION_TYPE = parquet.CompressionCodec_ZSTD

	VERSION_HINT_FILE_NAME = "version-hint.text"
)

var STORAGE_TYPES = []string{STORAGE_TYPE_LOCAL, STORAGE_TYPE_S3}

type ParquetFileStats struct {
	ColumnSizes     map[int]int64
	ValueCounts     map[int]int64
	NullValueCounts map[int]int64
	LowerBounds     map[int][]byte
	UpperBounds     map[int][]byte
	SplitOffsets    []int64
}

type ParquetFile struct {
	Uuid        string
	Path        string
	Size        int64
	RecordCount int64
	Stats       ParquetFileStats
}

type ManifestFile struct {
	SnapshotId int64
	Path       string
	Size       int64
}

type ManifestListFile struct {
	Path string
}

type MetadataFile struct {
	Version int64
	Path    string
}

type Storage interface {
	// Read
	IcebergSchemas() (schemas []string, err error)
	IcebergSchemaTables() (schemaTables []SchemaTable, err error)
	IcebergMetadataFilePath(schemaTable SchemaTable) (path string)

	// Write
	DeleteSchema(schema string) (err error)
	DeleteSchemaTable(schemaTable SchemaTable) (err error)
	CreateDataDir(schemaTable SchemaTable) (dataDirPath string)
	CreateMetadataDir(schemaTable SchemaTable) (metadataDirPath string)
	CreateParquet(dataDirPath string, pgSchemaColumns []PgSchemaColumn, loadRows func() [][]string) (parquetFile ParquetFile, err error)
	CreateManifest(metadataDirPath string, parquetFile ParquetFile) (manifestFile ManifestFile, err error)
	CreateManifestList(metadataDirPath string, parquetFile ParquetFile, manifestFile ManifestFile) (manifestListFile ManifestListFile, err error)
	CreateMetadata(metadataDirPath string, pgSchemaColumns []PgSchemaColumn, parquetFile ParquetFile, manifestFile ManifestFile, manifestListFile ManifestListFile) (metadataFile MetadataFile, err error)
	CreateVersionHint(metadataDirPath string, metadataFile MetadataFile) (err error)
}

func NewStorage(config *Config) Storage {
	switch config.StorageType {
	case STORAGE_TYPE_LOCAL:
		return NewLocalStorage(config)
	case STORAGE_TYPE_S3:
		return NewS3Storage(config)
	}

	return nil
}
