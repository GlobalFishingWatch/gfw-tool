package gcs

import (
	"context"
	"github.com/GlobalFishingWatch/gfw-tool/internal/common"
	"github.com/GlobalFishingWatch/gfw-tool/types"
	"strings"
)

func MergeMultipleObjects(params types.GCSMergeMultipleObjectsConfig) {
	ctx := context.Background()

	if params.DestinationBucket == "" {
		params.DestinationBucket = params.SourceBucket
	}

	objects := common.GCSListBucketObjects(
		ctx,
		params.SourceBucket,
		params.SourceDirectory,
	)

	destinationFormat := strings.ToLower(params.DestinationFormat)

	if params.CompressObject == true {
		destinationFormat = destinationFormat + ".gz"
	}

	common.GCSMergeObjects(ctx, params.SourceBucket, objects, params.SourceDirectory+"/"+params.MergedObjectName+"."+destinationFormat)
	common.GCSCopyObject(
		ctx,
		params.SourceBucket,
		params.SourceDirectory,
		params.MergedObjectName+"."+destinationFormat,
		params.DestinationBucket,
		params.DestinationDirectory,
		params.DstObjectName+"."+destinationFormat,
	)
	common.GCSDeleteObject(ctx, params.SourceBucket, params.SourceDirectory+"/"+params.MergedObjectName+"."+destinationFormat)
}
