package gcs

import (
	"context"
	"github.com/GlobalFishingWatch/gfw-tool/internal/common"
	"github.com/GlobalFishingWatch/gfw-tool/types"
)

func CopyObject(params types.GCSCopyObjectConfig) {
	ctx := context.Background()

	common.GCSCopyObject(
		ctx,
		params.SrcBucket,
		params.SrcDirectory,
		params.SrcObjectName,
		params.DstBucket,
		params.DstDirectory,
		params.DstObjectName,
	)
}
