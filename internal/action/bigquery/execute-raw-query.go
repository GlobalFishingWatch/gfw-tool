package bigquery

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/GlobalFishingWatch/gfw-tool/internal/common"
	"github.com/GlobalFishingWatch/gfw-tool/types"
	"google.golang.org/api/iterator"
)

func ExecuteRawQuery(params types.BQRawQueryConfig) []map[string]interface{} {
	ctx := context.Background()

	if params.DestinationDataset != "" {
		executeDestinationQuery(ctx, params)
		return nil
	} else {
		results := executeQuery(ctx, params)
		return results
	}
}

func executeDestinationQuery(ctx context.Context, params types.BQRawQueryConfig) {

	log.Printf("→ BQ →→ Executing query with destination table %s.%s", params.DestinationDataset, params.DestinationTable)
	dstTable := common.BigQueryGetTable(ctx, params.ProjectId, params.DestinationDataset, params.DestinationTable)
	metadata := &bigquery.TableMetadata{
		Location: "US",
	}
	if params.Schema != "" {
		var schema []*bigquery.FieldSchema
		fields := strings.Split(params.Schema, ",")
		for _, f := range fields {
			parts := strings.Split(f, ":")
			repeated := false
			if len(parts) > 2 {
				repeated = true
			}
			schema = append(schema, &bigquery.FieldSchema{
				Name:     parts[0],
				Type:     bigquery.FieldType(parts[1]),
				Repeated: repeated,
			})
		}
		metadata.Schema = schema
	}

	if params.TimePartitioning != "" {
		metadata.TimePartitioning = &bigquery.TimePartitioning{
			Field: params.PartitionTimeField,
			Type:  bigquery.TimePartitioningType(params.TimePartitioning),
		}
	}
	err := dstTable.Create(ctx, metadata)
	if err != nil {
		log.Fatalf("→ BQ →→ Error creating table %e", err)
	}
	client := common.BigQueryCreateClient(ctx, params.ProjectId)
	query := client.Query(params.Query)
	query.QueryConfig.Dst = dstTable
	query.QueryConfig.WriteDisposition = bigquery.TableWriteDisposition(params.WriteDisposition)

	job, err := query.Run(context.Background())
	if err != nil {
		log.Fatalf("→ BQ →→ Error running query %e", err)
	}
	for {
		log.Println("→ BQ →→ Checking status of job")
		status, err := job.Status(context.Background())
		if err != nil {
			log.Fatalf("→ BQ →→ Error obtaining status %e", err)
		}
		log.Println("→ BQ →→ Done:", status.Done())
		if status.Done() {
			if len(status.Errors) > 0 {
				log.Fatalf("→ BQ →→ Error importing data %v", status.Errors)
			}
			break
		}
		time.Sleep(5 * time.Second)
	}
	log.Println("→ BQ →→ Query run correctly")
}

func executeQuery(ctx context.Context, params types.BQRawQueryConfig) []map[string]interface{} {
	client := common.BigQueryCreateClient(ctx, params.ProjectId)
	query := client.Query(params.Query)
	query.AllowLargeResults = true

	if params.DestinationTable == "" {
		log.Println("→ BQ →→ Adding nil as destination table")
		query.QueryConfig.Dst = nil
		query.Dst = nil
	}

	log.Println("→ BQ →→ Executing query")
	it, err := query.Read(ctx)
	if err != nil {
		log.Fatalf("→ BQ →→ %s", err)
	}
	var rows []map[string]bigquery.Value
	for {
		row := make(map[string]bigquery.Value)
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		rows = append(rows, row)
	}

	log.Println("→ BQ →→ Parsing bigquery.values to bytes")
	result, err := json.Marshal(rows)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("→ BQ →→ Parsing bytes to json")
	var resultParsed []map[string]interface{}
	err = json.Unmarshal(result, &resultParsed)
	if err != nil {
		log.Fatal(err)
	}

	return resultParsed
}
