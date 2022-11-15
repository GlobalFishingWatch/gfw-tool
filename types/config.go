package types

type BQExportDataToGCSConfig struct {
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

type GCSExportDataToBigQueryConfig struct {
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
