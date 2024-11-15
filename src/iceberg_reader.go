package main

type IcebergReader struct {
	config  *Config
	storage Storage
}

func NewIcebergReader(config *Config) *IcebergReader {
	storage := NewStorage(config)
	return &IcebergReader{config: config, storage: storage}
}

func (reader *IcebergReader) Schemas() (icebergSchemas []string, err error) {
	return reader.storage.IcebergSchemas()
}

func (reader *IcebergReader) SchemaTables() (icebergSchemaTables []SchemaTable, err error) {
	return reader.storage.IcebergSchemaTables()
}

func (reader *IcebergReader) MetadataFilePath(icebergSchemaTable SchemaTable) string {
	return reader.storage.IcebergMetadataFilePath(icebergSchemaTable)
}
