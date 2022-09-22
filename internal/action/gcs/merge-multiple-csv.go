package gcs

import (
	"context"
	"github.com/GlobalFishingWatch/gfw-tool/internal/common"
	"github.com/GlobalFishingWatch/gfw-tool/types"
)

func MergeMultipleCsv(params types.GCSMergeMultipleCsvConfig) {
	ctx := context.Background()

	if params.DestinationBucket == "" {
		params.DestinationBucket = params.SourceBucket
	}

	objects := common.ListGCSBucketObjects(
		ctx,
		params.SourceBucket,
		params.SourceDirectory,
	)

	common.MergeObjects(ctx, params.SourceBucket, objects, params.SourceDirectory+"/"+params.MergedObjectName+".csv")
	common.CopyGCSObject(
		ctx,
		params.SourceBucket,
		params.SourceDirectory,
		params.MergedObjectName,
		params.DestinationBucket,
		params.DestinationDirectory,
		params.DstObjectName+".csv",
	)
	common.DeleteObject(ctx, params.SourceBucket, params.SourceDirectory+"/"+params.MergedObjectName)
}
