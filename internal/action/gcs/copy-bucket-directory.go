package gcs

import (
	"context"
	"github.com/GlobalFishingWatch/gfw-tool/internal/common"
	"github.com/GlobalFishingWatch/gfw-tool/types"
	"strings"
)

func CopyBucketDirectory(params types.GCSCopyBucketDirectoryConfig) {
	ctx := context.Background()
	objectNames := common.GCSListBucketObjects(ctx, params.SrcBucket, params.SrcDirectory)
	for _, name := range objectNames {
		nameSplit := strings.Split(name, "/")
		nameWithoutPath := nameSplit[len(nameSplit)-1]
		common.GCSCopyObject(
			ctx,
			params.SrcBucket,
			params.SrcDirectory,
			nameWithoutPath,
			params.DstBucket,
			params.DstDirectory,
			nameWithoutPath,
		)
	}
}
