package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/google/uuid"
	"github.com/xitongsys/parquet-go-source/local"
)

type StorageLocal struct {
	config      *Config
	storageBase *StorageBase
}

func NewLocalStorage(config *Config) *StorageLocal {
	return &StorageLocal{config: config, storageBase: &StorageBase{config: config}}
}

// Read ----------------------------------------------------------------------------------------------------------------

func (storage *StorageLocal) IcebergMetadataFilePath(schemaTable SchemaTable) string {
	return storage.tablePath(schemaTable) + "/metadata/v1.metadata.json"
}

func (storage *StorageLocal) IcebergSchemaTables() (schemaTables []SchemaTable, err error) {
	_, sourceFilePath, _, ok := runtime.Caller(0)
	if !ok {
		panic("Failed to get source file path")
	}
	projectDir := filepath.Dir(sourceFilePath)
	schemasPath := filepath.Join(projectDir, storage.config.IcebergPath)

	schemas, err := storage.nestedDirectories(schemasPath)
	if err != nil {
		return nil, err
	}

	for _, schema := range schemas {
		tablesPath := filepath.Join(schemasPath, schema)
		tables, err := storage.nestedDirectories(tablesPath)
		if err != nil {
			return nil, err
		}

		for _, table := range tables {
			schemaTables = append(schemaTables, SchemaTable{Schema: schema, Table: table})
		}
	}

	return schemaTables, nil
}

// Write ---------------------------------------------------------------------------------------------------------------

func (storage *StorageLocal) DeleteSchemaTable(schemaTable SchemaTable) error {
	tablePath := storage.tablePath(schemaTable)

	_, err := os.Stat(tablePath)
	if !os.IsNotExist(err) {
		err := os.RemoveAll(tablePath)
		return err
	}

	return nil
}

func (storage *StorageLocal) CreateDataDir(schemaTable SchemaTable) string {
	tablePath := storage.tablePath(schemaTable)
	dataPath := filepath.Join(tablePath, "data")
	err := os.MkdirAll(dataPath, os.ModePerm)
	PanicIfError(err)
	return dataPath
}

func (storage *StorageLocal) CreateMetadataDir(schemaTable SchemaTable) string {
	tablePath := storage.tablePath(schemaTable)
	metadataPath := filepath.Join(tablePath, "metadata")
	err := os.MkdirAll(metadataPath, os.ModePerm)
	PanicIfError(err)
	return metadataPath
}

func (storage *StorageLocal) CreateParquet(dataDirPath string, pgSchemaColumns []PgSchemaColumn, loadRows func() [][]string) (parquetFile ParquetFile, err error) {
	uuid := uuid.New().String()
	fileName := fmt.Sprintf("00000-0-%s.parquet", uuid)
	filePath := filepath.Join(dataDirPath, fileName)

	fileWriter, err := local.NewLocalFileWriter(filePath)
	if err != nil {
		return ParquetFile{}, fmt.Errorf("Failed to open Parquet file for writing: %v", err)
	}

	recordCount, err := storage.storageBase.WriteParquetFile(fileWriter, pgSchemaColumns, loadRows)
	if err != nil {
		return ParquetFile{}, err
	}
	LogDebug(storage.config, "Parquet file with", recordCount, "record(s) created at:", filePath)

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return ParquetFile{}, fmt.Errorf("Failed to get Parquet file info: %v", err)
	}
	fileSize := fileInfo.Size()

	fileReader, err := local.NewLocalFileReader(filePath)
	if err != nil {
		return ParquetFile{}, fmt.Errorf("Failed to open Parquet file for reading: %v", err)
	}
	parquetStats, err := storage.storageBase.ReadParquetStats(fileReader)
	if err != nil {
		return ParquetFile{}, err
	}

	return ParquetFile{
		Uuid:        uuid,
		Path:        filePath,
		Size:        fileSize,
		RecordCount: recordCount,
		Stats:       parquetStats,
	}, nil
}

func (storage *StorageLocal) CreateManifest(metadataDirPath string, parquetFile ParquetFile) (manifestFile ManifestFile, err error) {
	fileName := fmt.Sprintf("%s-m0.avro", parquetFile.Uuid)
	filePath := filepath.Join(metadataDirPath, fileName)

	manifestFile, err = storage.storageBase.WriteManifestFile(storage.fileSystemPrefix(), filePath, parquetFile)
	if err != nil {
		return ManifestFile{}, err
	}
	LogDebug(storage.config, "Manifest file created at:", filePath)

	return manifestFile, nil
}

func (storage *StorageLocal) CreateManifestList(metadataDirPath string, parquetFile ParquetFile, manifestFile ManifestFile) (manifestListFile ManifestListFile, err error) {
	fileName := fmt.Sprintf("snap-%d-0-%s.avro", manifestFile.SnapshotId, parquetFile.Uuid)
	filePath := filepath.Join(metadataDirPath, fileName)

	err = storage.storageBase.WriteManifestListFile(storage.fileSystemPrefix(), filePath, parquetFile, manifestFile)
	if err != nil {
		return ManifestListFile{}, err
	}
	LogDebug(storage.config, "Manifest list file created at:", filePath)

	return ManifestListFile{Path: filePath}, nil
}

func (storage *StorageLocal) CreateMetadata(metadataDirPath string, pgSchemaColumns []PgSchemaColumn, parquetFile ParquetFile, manifestFile ManifestFile, manifestListFile ManifestListFile) (metadataFile MetadataFile, err error) {
	version := int64(1)
	fileName := fmt.Sprintf("v%d.metadata.json", version)
	filePath := filepath.Join(metadataDirPath, fileName)

	err = storage.storageBase.WriteMetadataFile(storage.fileSystemPrefix(), filePath, pgSchemaColumns, parquetFile, manifestFile, manifestListFile)
	if err != nil {
		return MetadataFile{}, err
	}
	LogDebug(storage.config, "Metadata file created at:", filePath)

	return MetadataFile{Version: version, Path: filePath}, nil
}

func (storage *StorageLocal) CreateVersionHint(metadataDirPath string, metadataFile MetadataFile) (err error) {
	filePath := filepath.Join(metadataDirPath, VERSION_HINT_FILE_NAME)

	err = storage.storageBase.WriteVersionHintFile(filePath, metadataFile)
	if err != nil {
		return err
	}
	LogDebug(storage.config, "Version hint file created at:", filePath)

	return nil
}

func (storage *StorageLocal) tablePath(schemaTable SchemaTable) string {
	_, sourceFilePath, _, ok := runtime.Caller(0)
	if !ok {
		panic("Failed to get source file path")
	}
	projectDir := filepath.Dir(sourceFilePath)
	return filepath.Join(projectDir, storage.config.IcebergPath, schemaTable.Schema, schemaTable.Table)
}

func (storage *StorageLocal) fileSystemPrefix() string {
	return ""
}

func (storage *StorageLocal) nestedDirectories(path string) (dirs []string, err error) {
	files, err := os.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("Failed to read directory: %v", err)
	}

	for _, file := range files {
		if file.IsDir() {
			dirs = append(dirs, file.Name())
		}
	}

	return dirs, nil
}
