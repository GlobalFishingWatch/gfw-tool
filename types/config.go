package types

type BQExportDataToGCSConfig struct {
	Query                string
	ProjectId            string
	TemporalDataset      string
	Bucket               string
	BucketDirectory      string
	BucketDstObjectName  string
	HeadersEnable        bool
	ExportHeadersAsAFile bool
}

type GCSMergeMultipleCsvConfig struct {
	ProjectId            string
	SourceBucket         string
	SourceDirectory      string
	DestinationBucket    string
	DestinationDirectory string
	MergedObjectName     string
	DstObjectName        string
}

type GCSUploadObjectConfig struct {
	DstBucket     string
	DstDirectory  string
	DstObjectName string
	Content       string
}
