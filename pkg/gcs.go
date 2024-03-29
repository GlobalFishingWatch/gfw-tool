package pkg

import (
	"github.com/GlobalFishingWatch/gfw-tool/internal/action/gcs"
	"github.com/GlobalFishingWatch/gfw-tool/types"
)

func GCSMergeMultipleObjects(params types.GCSMergeMultipleObjectsConfig) {
	gcs.MergeMultipleObjects(params)
}

func GCSUploadObject(params types.GCSUploadObjectConfig) {
	gcs.UploadObject(params)
}

func GCSCopyBucketDirectory(params types.GCSCopyBucketDirectoryConfig) {
	gcs.CopyBucketDirectory(params)
}

func GCSCopyObject(params types.GCSCopyObjectConfig) {
	gcs.CopyObject(params)
}

func GCSGCSDeleteObject(params types.GCSDeleteObjectConfig) {
	gcs.GCSDeleteObject(params)
}
