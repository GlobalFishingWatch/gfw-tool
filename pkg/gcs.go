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
