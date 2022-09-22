package pkg

import (
	"github.com/GlobalFishingWatch/gfw-tool/internal/action/gcs"
	"github.com/GlobalFishingWatch/gfw-tool/types"
)

func GCSMergeMultipleCsv(params types.GCSMergeMultipleCsvConfig) {
	gcs.MergeMultipleCsv(params)
}

func GCSUploadObject(params types.GCSUploadObjectConfig) {
	gcs.UploadObject(params)
}
