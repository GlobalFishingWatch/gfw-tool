package bq2gcs

import (
	"context"
	"fmt"
	"github.com/GlobalFishingWatch/gfw-tool/internal/common"
	"github.com/GlobalFishingWatch/gfw-tool/types"
	"log"
)

func ExportDataFromBigQueryQueryToGCS(params types.BQExportDataToGCSConfig) {
	ctx := context.Background()

	validateParams(params)

	if params.ExportHeadersAsAFile && params.DestinationFormat == "CSV" {
		temporalHeadersQuery := fmt.Sprintf(`%s LIMIT 0`, params.Query)
		temporalHeadersTableId := common.CreateTemporalTableFromQuery(
			ctx,
			params.ProjectId,
			params.TemporalDataset,
			"",
			temporalHeadersQuery,
			0,
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
		"",
		params.Query,
		0,
		"",
	)

	if params.DestinationFormat == "CSV" {
		common.ExportTemporalTableToCsvInGCS(
			ctx,
			params.ProjectId,
			params.TemporalDataset,
			temporalTableId,
			params.Bucket,
			params.BucketDirectory,
			params.HeadersEnable,
		)
	} else if params.DestinationFormat == "JSON" {
		common.ExportTemporalTableToJSONInGCS(
			ctx,
			params.ProjectId,
			params.TemporalDataset,
			temporalTableId,
			params.Bucket,
			params.BucketDirectory,
			params.CompressObjects,
		)
	} else {
		log.Fatal("Destination format not allowed")
	}

}

func validateParams(params types.BQExportDataToGCSConfig) {
	if params.DestinationFormat != "CSV" && params.DestinationFormat != "JSON" {
		log.Fatal("Destination format should be JSON or CSV")
	}
	if params.DestinationFormat != "CSV" && params.ExportHeadersAsAFile == true {
		log.Fatal("Export headers as a file flags is just available for destination format CSV")
	}
	if params.DestinationFormat != "JSON" && params.CompressObjects == true {
		log.Fatal("Compress objects is just available for JSON format")
	}
}
