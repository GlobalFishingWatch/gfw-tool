package bq2gcs

import (
	"context"
	"fmt"
	"log"

	"github.com/GlobalFishingWatch/gfw-tool/internal/common"
	"github.com/GlobalFishingWatch/gfw-tool/types"
)

func ExportDataFromBigQueryQueryToGCS(params types.BQ2GCSExportDataToGCSConfig) {
	ctx := context.Background()

	validateParams(params)

	if params.ExportHeadersAsAFile && params.DestinationFormat == "CSV" {
		temporalHeadersQuery := fmt.Sprintf(`%s LIMIT 0`, params.Query)
		temporalHeadersTableId := common.BigQueryCreateTemporalTableFromQuery(
			ctx,
			params.ProjectId,
			params.TemporalDataset,
			"",
			temporalHeadersQuery,
			0,
			"_headers",
			params.Labels,
		)

		common.BigQueryExportTemporalTableToCsvInGCS(
			ctx,
			params.ProjectId,
			params.TemporalDataset,
			temporalHeadersTableId,
			params.Bucket,
			params.BucketDirectory,
			true,
			params.Labels,
		)

		common.GCSCopyObject(
			ctx,
			params.Bucket,
			params.BucketDirectory,
			"000000000000.csv",
			params.Bucket,
			params.BucketDirectory,
			params.BucketDstObjectName+".csv",
		)

	}

	temporalTableId := common.BigQueryCreateTemporalTableFromQuery(
		ctx,
		params.ProjectId,
		params.TemporalDataset,
		"",
		params.Query,
		0,
		"",
		params.Labels,
	)

	if params.DestinationFormat == "CSV" {
		common.BigQueryExportTemporalTableToCsvInGCS(
			ctx,
			params.ProjectId,
			params.TemporalDataset,
			temporalTableId,
			params.Bucket,
			params.BucketDirectory,
			params.HeadersEnable,
			params.Labels,
		)
	} else if params.DestinationFormat == "JSON" {
		common.BigQueryExportTemporalTableToJSONInGCS(
			ctx,
			params.ProjectId,
			params.TemporalDataset,
			temporalTableId,
			params.Bucket,
			params.BucketDirectory,
			params.CompressObjects,
			params.Labels,
		)
	} else {
		log.Fatal("Destination format not allowed")
	}

}

func validateParams(params types.BQ2GCSExportDataToGCSConfig) {
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
