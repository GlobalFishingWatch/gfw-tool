package bq2gcs

import (
	"context"
	"fmt"
	"github.com/GlobalFishingWatch/gfw-tool/internal/common"
	"github.com/GlobalFishingWatch/gfw-tool/types"
)

func ExportDataFromBigQueryQueryToGCS(params types.BQExportDataToGCSConfig) {
	ctx := context.Background()
	if params.ExportHeadersAsAFile {
		temporalHeadersQuery := fmt.Sprintf(`%s LIMIT 0`, params.Query)
		temporalHeadersTableId := common.CreateTemporalTableFromQuery(
			ctx, params.ProjectId,
			params.TemporalDataset,
			temporalHeadersQuery,
			"_headers",
		)
		common.ExportTemporalTableToCsvInGCS(
			ctx,
			params.ProjectId,
			params.TemporalDataset,
			temporalHeadersTableId,
			params.Bucket,
			params.BucketDirectory,
			true,
		)

		common.CopyGCSObject(
			ctx,
			params.Bucket,
			params.BucketDirectory,
			"000000000000.csv",
			params.Bucket,
			params.BucketDirectory,
			params.BucketDstObjectName+".csv",
		)

	}

	temporalTableId := common.CreateTemporalTableFromQuery(
		ctx,
		params.ProjectId,
		params.TemporalDataset,
		params.Query, "",
	)
	common.ExportTemporalTableToCsvInGCS(
		ctx,
		params.ProjectId,
		params.TemporalDataset,
		temporalTableId,
		params.Bucket,
		params.BucketDirectory,
		params.HeadersEnable,
	)
}
