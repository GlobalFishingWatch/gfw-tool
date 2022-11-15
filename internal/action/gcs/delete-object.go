package gcs

import (
	"context"
	"github.com/GlobalFishingWatch/gfw-tool/internal/common"
	"github.com/GlobalFishingWatch/gfw-tool/types"
)

func DeleteObject(params types.GCSDeleteObjectConfig) {
	ctx := context.Background()
	common.DeleteObject(
		ctx,
		params.BucketName,
		params.ObjectName,
	)
}
