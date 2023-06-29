package bq2psql

import (
	"context"
	"fmt"
	"log"
	"sync"

	"cloud.google.com/go/bigquery"
	"github.com/GlobalFishingWatch/gfw-tool/internal/common"
	"github.com/GlobalFishingWatch/gfw-tool/types"
	"github.com/GlobalFishingWatch/gfw-tool/utils"
)

var currentBatch = 0

func ExportBigQueryToPostgres(params types.BQ2PSQLExportConfig, postgresConfig types.PostgresConfig) {
	ctx := context.Background()

	ch := make(chan map[string]bigquery.Value, 100)

	log.Println("Creating table to check if exists before the query")
	if len(params.Schema) > 0 {
		common.PostgresCreateTable(ctx, postgresConfig, params.TableName, params.Schema)
	}

	log.Println("→ Getting results from BigQuery")
	getResultsFromBigQuery(ctx, params.ProjectId, params.Query, params.Labels, ch)

	log.Println("→ Importing results to Postgres")

	var wg sync.WaitGroup
	for i := 0; i < 1; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup, ch chan map[string]bigquery.Value) {
			importToPostgres(ctx, postgresConfig, ch, params.TableName)
			wg.Done()
		}(&wg, ch)
	}
	wg.Wait()
}

// BigQuery Functions
func getResultsFromBigQuery(ctx context.Context, projectId string, queryRequested string, labels map[string]string, ch chan map[string]bigquery.Value) {
	iterator := common.BigQueryMakeQuery(ctx, projectId, queryRequested, true, labels)
	go common.BigQueryParseResultsToJson(iterator, ch)
}

// Postgres functions
func importToPostgres(ctx context.Context, postgresConfig types.PostgresConfig, ch chan map[string]bigquery.Value, tableName string) {
	log.Println("→ PG →→ Importing data to Postgres")

	const Batch = 1000

	var (
		numItems int
		columns  string
		values   string
		keys     []string
		query    string
	)

	numItems = 0

	for doc := range ch {

		if numItems == 0 {
			columns, keys = common.BigQueryGetColumnNamesFromRecord(doc)
		}
		values = values + common.BigQueryGetValuesFromRecord(keys, doc)
		query = fmt.Sprintf("INSERT INTO %v %v VALUES %v", tableName, columns, values)
		numItems++
		if numItems == Batch {
			currentBatch++
			log.Printf("Batch %v, Rows Imported: %v", currentBatch, currentBatch+Batch)
			query = utils.TrimSuffix(query, ",") + ";"
			common.PostgresExecuteSQLCommand(ctx, postgresConfig, query, 0)
			numItems = 0
			query = ""
			values = ""
		}

	}

	if numItems > 0 {
		common.PostgresExecuteSQLCommand(ctx, postgresConfig, query, 0)
	}

	log.Println("→ PG →→ Import process finished")
}
