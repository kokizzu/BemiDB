package main

type IcebergReader struct {
	config  *Config
	storage Storage
}

func NewIcebergReader(config *Config) *IcebergReader {
	storage := NewStorage(config)
	return &IcebergReader{config: config, storage: storage}
}

func (reader *IcebergReader) SchemaTables() (schemaTables []SchemaTable, err error) {
	return reader.storage.IcebergSchemaTables()
}

func (reader *IcebergReader) MetadataFilePath(schemaTable SchemaTable) string {
	return reader.storage.IcebergMetadataFilePath(schemaTable)
}
