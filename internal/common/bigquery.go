package common

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
	uuid "github.com/satori/go.uuid"
	"log"
	"time"
)

func CreateBigQueryClient(ctx context.Context, projectId string) *bigquery.Client {
	log.Println("→ BQ →→ Creating Big Query Client")

	client, err := bigquery.NewClient(ctx, projectId)
	if err != nil {
		log.Fatalf("→ BQ →→ bigquery.NewClient: %v", err)
	}

	return client
}

func MakeQuery(
	ctx context.Context,
	projectId string,
	sqlQuery string,
) *bigquery.RowIterator {
	log.Println("→ BQ →→ Making query to get data from bigQuery")
	bqClient := CreateBigQueryClient(ctx, projectId)
	query := bqClient.Query(sqlQuery)
	query.AllowLargeResults = true
	it, err := query.Read(ctx)
	if err != nil {
		log.Fatalf("→ BQ →→ Error counting rows: %v", err)
	}
	return it
}

func GetColumnNamesFromTableSchema(
	schema bigquery.Schema,
) []string {
	var columnNames = make([]string, 0)
	for i := 0; i < len(schema); i++ {
		columnNames = append(columnNames, schema[i].Name)
	}
	return columnNames
}

func CreateTemporalTableFromQuery(
	ctx context.Context,
	projectId string,
	datasetId string,
	sqlStatement string,
	subfix string,
) string {
	log.Println("→ BQ →→ Creating temporal table")

	bqClient := CreateBigQueryClient(ctx, projectId)
	defer bqClient.Close()

	log.Printf("→ BQ →→ Query: %s", sqlStatement)
	query := bqClient.Query(sqlStatement)
	query.AllowLargeResults = true
	currentTime := time.Now()
	temporalTableName := fmt.Sprintf("%s_%s%s", uuid.NewV4(), currentTime.Format("2006_01_02_15_04"), subfix)
	log.Printf("→ BQ →→ Temporal table name: %s", temporalTableName)
	dstTable := bqClient.Dataset(datasetId).Table(string(temporalTableName))
	err := dstTable.Create(ctx, &bigquery.TableMetadata{})
	if err != nil {
		log.Fatal("→ BQ →→ Error creating temporary table", err)
	}
	query.QueryConfig.Dst = dstTable
	log.Println("→ BQ →→ Exporting query to intermediate table")

	job, err := query.Run(context.Background())
	CheckBigQueryJob(job, err)

	config, err := job.Config()
	if err != nil {
		log.Fatal("→ BQ →→ Error obtaining config", err)
	}
	tempTable := config.(*bigquery.QueryConfig).Dst
	log.Println("→ BQ →→ Temp table", tempTable.TableID)
	return tempTable.TableID
}

func ExportTemporalTableToCsvInGCS(
	ctx context.Context,
	projectId string,
	dataset string,
	temporalTable string,
	bucket string,
	directory string,
	headersEnable bool,
) []string {

	bqClient := CreateBigQueryClient(ctx, projectId)
	defer bqClient.Close()

	temporalDataset := bqClient.DatasetInProject(projectId, dataset)
	table := temporalDataset.Table(temporalTable)
	uri := fmt.Sprintf(`gs://%s/%s/*.csv`, bucket, directory)
	gcsRef := bigquery.NewGCSReference(uri)
	gcsRef.DestinationFormat = "CSV"
	extractor := table.ExtractorTo(gcsRef)
	if headersEnable == true {
		extractor.DisableHeader = false
	} else {
		extractor.DisableHeader = true
	}
	job, err := extractor.Run(ctx)
	CheckBigQueryJob(job, err)
	config, err := job.Config()
	if err != nil {
		log.Fatal("→ BQ →→ Error obtaining config", err)
	}
	tempBucket := config.(*bigquery.ExtractConfig).Dst
	log.Println("→ GCS →→ Temporal URIs", tempBucket.URIs)
	return tempBucket.URIs
}

func ExportTemporalTableToJSONInGCS(
	ctx context.Context,
	projectId string,
	dataset string,
	temporalTable string,
	bucket string,
	directory string,
	compressObjects bool,
) []string {
	bqClient := CreateBigQueryClient(ctx, projectId)
	defer bqClient.Close()

	temporalDataset := bqClient.DatasetInProject(projectId, dataset)
	table := temporalDataset.Table(temporalTable)
	uri := fmt.Sprintf(`gs://%s/%s/*.json`, bucket, directory)
	if compressObjects == true {
		uri = fmt.Sprintf(`gs://%s/%s/*.json.gz`, bucket, directory)
	}
	gcsRef := bigquery.NewGCSReference(uri)
	gcsRef.DestinationFormat = "NEWLINE_DELIMITED_JSON"

	if compressObjects == true {
		gcsRef.Compression = "GZIP"
	}

	extractor := table.ExtractorTo(gcsRef)
	job, err := extractor.Run(ctx)
	CheckBigQueryJob(job, err)
	config, err := job.Config()
	if err != nil {
		log.Fatal("→ BQ →→ Error obtaining config", err)
	}
	tempBucket := config.(*bigquery.ExtractConfig).Dst
	log.Println("→ GCS →→ Temporal URIs", tempBucket.URIs)
	return tempBucket.URIs
}

func CheckBigQueryJob(job *bigquery.Job, err error) {
	if err != nil {
		log.Fatal("→ BQ →→ Error creating job", err)
	}
	for {
		log.Println("→ BQ →→ Checking status of job")
		status, err := job.Status(context.Background())
		if err != nil {
			log.Fatal("→ BQ →→ Error obtaining status", err)
		}
		log.Println("→ BQ →→ Done:", status.Done())
		if status.Done() {
			if len(status.Errors) > 0 {
				log.Fatal("→ BQ →→ Error", status.Errors)
			}
			break
		}
		time.Sleep(15 * time.Second)
	}
}
