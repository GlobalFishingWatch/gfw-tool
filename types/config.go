package types

type BQ2GCSExportDataToGCSConfig struct {
	Query                string
	ProjectId            string
	TemporalDataset      string
	DestinationFormat    string
	CompressObjects      bool
	Bucket               string
	BucketDirectory      string
	BucketDstObjectName  string
	HeadersEnable        bool
	ExportHeadersAsAFile bool
}

type BQ2ESImportConfig struct {
	Query                  string
	ElasticSearchUrl       string
	ProjectId              string
	IndexName              string
	ImportMode             string
	Normalize              string
	NormalizedPropertyName string
	NormalizeEndpoint      string
	OnError                string
}

type BQCreateTableConfig struct {
	Query     string
	ProjectId string
	DatasetId string
	TableName string
}

type BQCreateTemporalTableConfig struct {
	Query         string
	ProjectId     string
	TempDatasetId string
	TempTableName string
	TTL           int
}

type BQRawQueryConfig struct {
	Query              string
	ProjectId          string
	DestinationTable   string
	DestinationDataset string
	WriteDisposition   string
}

type GCS2BQExportDataToBigQueryConfig struct {
	ProjectId          string
	BucketUri          string
	SourceDataFormat   string
	DatasetName        string
	TableName          string
	Mode               string
	Schema             string
	PartitionTimeField string
	ClusteredFields    string
}

type GCSMergeMultipleObjectsConfig struct {
	ProjectId            string
	SourceBucket         string
	SourceDirectory      string
	DestinationBucket    string
	DestinationDirectory string
	DestinationFormat    string
	CompressObject       bool
	MergedObjectName     string
	DstObjectName        string
}

type GCSUploadObjectConfig struct {
	DstBucket     string
	DstDirectory  string
	DstObjectName string
	Content       string
}

type GCSCopyBucketDirectoryConfig struct {
	SrcBucket    string
	SrcDirectory string
	DstBucket    string
	DstDirectory string
}

type GCSCopyObjectConfig struct {
	SrcBucket     string
	SrcDirectory  string
	SrcObjectName string
	DstBucket     string
	DstDirectory  string
	DstObjectName string
}

type GCSDeleteObjectConfig struct {
	ProjectId  string
	BucketName string
	ObjectName string
}

type ElasticsearchAddAliasConfig struct {
	IndexName        string
	Alias            string
	ElasticSearchUrl string
}

type ElasticsearchCreateIndexConfig struct {
	IndexName        string
	Mapping          string
	Settings         string
	ElasticSearchUrl string
}

type ElasticsearchDeleteIndexConfig struct {
	IndexName        string
	ElasticSearchUrl string
}

type ElasticsearchDeleteIndicesByPrefixConfig struct {
	Prefix           string
	NoDeleteIndex    string
	ElasticSearchUrl string
}

type PostgresCreateIndexConfig struct {
	IndexName string
	TableName string
	Column    string
}

type PostgresCreateViewConfig struct {
	TableName string
	ViewName  string
}

type PostgresDeleteTableConfig struct {
	TableName string
}

type PostgresDeleteViewConfig struct {
	ViewName string
}

type PostgresExecuteRawSqlConfig struct {
	Sql string
}

type PostgresConfig struct {
	Addr     string
	User     string
	Password string
	Database string
}

type BQ2PSQLExportConfig struct {
	Query     string
	ProjectId string
	TableName string
	Schema    string
}

type BQ2PSQLExportCSVConfig struct {
	Query                string
	ProjectId            string
	TemporalDataset      string
	TemporalBucket       string
	DestinationTableName string
}

type CloudSqlConfig struct {
	Database string
	Instance string
	Table    string
	Columns  string
}
