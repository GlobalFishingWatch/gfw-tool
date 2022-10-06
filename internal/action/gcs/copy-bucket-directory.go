package gcs

import (
	"context"
	"github.com/GlobalFishingWatch/gfw-tool/internal/common"
	"github.com/GlobalFishingWatch/gfw-tool/types"
	"strings"
)

func CopyBucketDirectory(params types.GCSCopyBucketDirectoryConfig) {
	ctx := context.Background()
	objectNames := common.ListGCSBucketObjects(ctx, params.SrcBucket, params.SrcDirectory)
	for _, name := range objectNames {
		nameSplit := strings.Split(name, "/")
		nameWithoutPath := nameSplit[len(nameSplit)-1]
		common.CopyGCSObject(
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
