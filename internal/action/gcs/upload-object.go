package gcs

import (
	"context"
	"github.com/GlobalFishingWatch/gfw-tool/internal/common"
	"github.com/GlobalFishingWatch/gfw-tool/types"
)

func UploadObject(params types.GCSUploadObjectConfig) {
	ctx := context.Background()
	const temporalDirectory = "./temp"
	common.OSWriteFileFromString(temporalDirectory, params.DstObjectName, params.Content)
	common.GCSUploadLocalFileToABucket(
		ctx,
		params.DstBucket,
		temporalDirectory,
		params.DstObjectName,
		params.DstDirectory,
		params.DstObjectName,
	)
}
