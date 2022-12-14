package gcs

import (
	"context"
	"github.com/GlobalFishingWatch/gfw-tool/internal/common"
	"github.com/GlobalFishingWatch/gfw-tool/types"
)

func GCSDeleteObject(params types.GCSDeleteObjectConfig) {
	ctx := context.Background()
	common.GCSDeleteObject(
		ctx,
		params.BucketName,
		params.ObjectName,
	)
}
