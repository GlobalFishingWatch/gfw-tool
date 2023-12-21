package gcs2bq

import (
	"context"
	"log"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/GlobalFishingWatch/gfw-tool/internal/common"
	"github.com/GlobalFishingWatch/gfw-tool/types"
)

func Export(params types.GCS2BQExportDataToBigQueryConfig) {
	ctx := context.Background()
	table := common.BigQueryGetTable(
		ctx,
		params.ProjectId,
		params.DatasetName,
		params.TableName,
	)
	switch strings.ToLower(params.Mode) {
	case MODE_CREATE:
		executeCreateMode(ctx, table, params)
	case MODE_AUTODETECT:
		executeAutodetectMode(ctx, table, params)
	case MODE_APPEND:
		executeAppendMode(ctx, table, params)
	default:
		log.Fatal("→ BQ →→ --mode not allowed")
	}
}

func executeCreateMode(
	ctx context.Context,
	table *bigquery.Table,
	params types.GCS2BQExportDataToBigQueryConfig,
) {
	log.Println("→ BQ →→ Executing Create mode")

	existsTable := common.BigQueryCheckIfTableExists(ctx, table)
	log.Printf("→ BQ →→ The table with name %s exists %t:", params.TableName, existsTable)
	if existsTable == true {
		executeAppendMode(ctx, table, params)
		return
	}

	log.Printf("→ BQ →→ Schema to create the new table %s", params.Schema)
	if params.Schema == "" {
		log.Fatalf("→ BQ →→ Schema required for create mode")
	}

	var clusteredFields []string
	if params.ClusteredFields != "" {
		clusteredFields = strings.Split(params.ClusteredFields, ",")
	} else {
		clusteredFields = make([]string, 0)
	}

	common.BigQueryCreateTable(ctx, table, params.Schema, params.PartitionTimeField, clusteredFields, params.Labels)
	gcsRef := common.BigQueryGetStorageRef(
		params.BucketUri,
		params.SourceDataFormat,
	)
	loader := table.LoaderFrom(gcsRef)
	runLoader(ctx, loader)
}

func executeAutodetectMode(ctx context.Context, table *bigquery.Table, params types.GCS2BQExportDataToBigQueryConfig) {
	log.Println("→ BQ →→ Executing Autodetect mode")

	existsTable := common.BigQueryCheckIfTableExists(ctx, table)
	log.Printf("→ BQ →→ The table with name %s exists %t:", params.TableName, existsTable)
	if existsTable == true {
		log.Fatalf("→ BQ →→ This table exists and you are trying to recreate the table")
	}
	gcsRef := common.BigQueryGetStorageRef(
		params.BucketUri,
		params.SourceDataFormat,
	)
	gcsRef.FileConfig.AutoDetect = true
	gcsRef.FileConfig.Schema = nil
	loader := table.LoaderFrom(gcsRef)
	loader.CreateDisposition = bigquery.CreateIfNeeded
	runLoader(ctx, loader)
}

func executeAppendMode(ctx context.Context, table *bigquery.Table, params types.GCS2BQExportDataToBigQueryConfig) {
	log.Println("→ BQ →→ Executing Append mode")
	existsTable := common.BigQueryCheckIfTableExists(ctx, table)
	log.Printf("→ BQ →→ The table with name %s exists %t:", params.TableName, existsTable)
	if existsTable == false {
		log.Fatalf("→ BQ →→ This table does not exist and you are trying to append data")
	}
	gcsRef := common.BigQueryGetStorageRef(
		params.BucketUri,
		params.SourceDataFormat,
	)
	loader := table.LoaderFrom(gcsRef)
	runLoader(ctx, loader)
}

func runLoader(ctx context.Context, loader *bigquery.Loader) {
	log.Println("→ GCS →→ Running loader")
	loader.WriteDisposition = bigquery.WriteAppend
	job, err := loader.Run(ctx)
	if err != nil {
		log.Fatalf("→ GCS →→ Running loaders error %s", err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		log.Fatalf("→ GCS →→ Waiting loaders error %s", err)

	}
	if status.Err() != nil {
		log.Fatalf("→ GCS →→ Error after running loader %s", status.Err())
	}
}
