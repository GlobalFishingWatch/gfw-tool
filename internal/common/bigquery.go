package common

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/GlobalFishingWatch/gfw-tool/utils"
	uuid "github.com/satori/go.uuid"
	"google.golang.org/api/iterator"
)

func BigQueryCreateClient(ctx context.Context, projectId string) *bigquery.Client {
	log.Println("→ BQ →→ Creating Big Query Client")

	client, err := bigquery.NewClient(ctx, projectId)
	if err != nil {
		log.Fatalf("→ BQ →→ bigquery.NewClient: %v", err)
	}

	return client
}

func BigQueryMakeQuery(
	ctx context.Context,
	projectId string,
	sqlQuery string,
	exportToTemporalTable bool,
	labels map[string]string,
) *bigquery.RowIterator {
	log.Println("→ BQ →→ Making query to get data from bigQuery")
	client := BigQueryCreateClient(ctx, projectId)

	query := client.Query(sqlQuery)
	query.AllowLargeResults = true
	if labels != nil {
		query.QueryConfig.Labels = labels
	}

	if exportToTemporalTable == true {
		currentTime := time.Now()
		datasetId := "0_ttl24h"
		temporalTableName := fmt.Sprintf("%s_%s", uuid.NewV4(), currentTime.Format("2006-01-02"))
		dstTable := client.Dataset(datasetId).Table(string(temporalTableName))
		tableMetadata := bigquery.TableMetadata{ExpirationTime: time.Now().Add(24 * time.Hour)}
		if labels != nil {
			tableMetadata.Labels = labels
		}
		err := dstTable.Create(ctx, &tableMetadata)
		if err != nil {
			log.Fatal("→ BQ →→ Error creating temporary table", err)
		}
		query.QueryConfig.Dst = dstTable
		log.Println("→ BQ →→ Exporting query to intermediate table")

	}

	it, err := query.Read(ctx)
	if err != nil {
		log.Fatalf("→ BQ →→ Error counting rows: %v", err)
	}
	return it
}

func BigQueryGetColumnNamesFromTableSchema(
	schema bigquery.Schema,
) []string {
	var columnNames = make([]string, 0)
	for i := 0; i < len(schema); i++ {
		columnNames = append(columnNames, schema[i].Name)
	}

	return columnNames
}

func BigQueryGetColumnNamesFromRecord(
	doc map[string]bigquery.Value,
) (string, []string) {
	var columns = "("
	keys := make([]string, 0, len(doc))

	for k := range doc {
		if reflect.ValueOf(doc[k]).Kind() == reflect.Slice {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for k := 0; k < len(keys); k++ {
		columns = columns + utils.CamelCaseToSnakeCase(keys[k]) + ","
	}

	columns = utils.TrimSuffix(columns, ",")
	columns = columns + ") "
	return columns, keys
}

func BigQueryExportTableToACSV(
	ctx context.Context,
	projectId string,
	dataset string,
	temporalTable string,
	temporalBucket string,
) {
	client := BigQueryCreateClient(ctx, projectId)
	defer client.Close()
	temporalDataset := client.DatasetInProject(projectId, dataset)
	table := temporalDataset.Table(temporalTable)
	uri := fmt.Sprintf(`gs://%s/bq2psql-tool/%s/*.csv.gz`, temporalBucket, temporalTable)
	gcsRef := bigquery.NewGCSReference(uri)
	gcsRef.Compression = "GZIP"
	gcsRef.DestinationFormat = "CSV"
	extractor := table.ExtractorTo(gcsRef)
	extractor.DisableHeader = true
	job, err := extractor.Run(ctx)
	BigQueryCheckJob(job, err)
}

func BigQueryGetValuesFromRecord(keys []string, doc map[string]bigquery.Value) string {
	var values = "("

	for k := 0; k < len(keys); k++ {
		column := keys[k]
		value := doc[column]
		var myType = reflect.ValueOf(value).Kind()
		if myType == reflect.Slice {
			continue
		} else if myType == reflect.String || myType == reflect.Struct {
			valueString := strings.Replace(fmt.Sprintf("%v", value), "'", `''`, -1)
			values = values + fmt.Sprintf("'%v'", valueString) + ","
		} else if myType == reflect.Int || myType == reflect.Float64 {
			values = values + fmt.Sprintf("%v", value) + ","
		} else {
			values = values + "null,"
		}
	}

	values = utils.TrimSuffix(values, ",")
	values = values + "),"
	return values
}

func BigQueryCreateTemporalTableFromQuery(
	ctx context.Context,
	projectId string,
	datasetId string,
	tableName string,
	sqlStatement string,
	ttl int,
	suffix string,
	labels map[string]string,
) string {
	log.Println("→ BQ →→ Creating temporal table")

	bqClient := BigQueryCreateClient(ctx, projectId)
	defer bqClient.Close()

	log.Printf("→ BQ →→ Query: %s", sqlStatement)
	query := bqClient.Query(sqlStatement)
	query.AllowLargeResults = true
	currentTime := time.Now()

	temporalTableName := ""
	if tableName != "" {
		temporalTableName = tableName
	} else {
		temporalTableName = fmt.Sprintf("%s_%s%s", uuid.NewV4(), currentTime.Format("2006_01_02_15_04"), suffix)
	}

	log.Printf("→ BQ →→ Temporal table name: %s", temporalTableName)
	dstTable := BigQueryGetTable(
		ctx,
		projectId,
		datasetId,
		temporalTableName,
	)

	var tableMetadata *bigquery.TableMetadata

	var ttlParsed time.Duration
	if ttl == 0 {
		ttlParsed = 12 * time.Hour
	} else {
		ttlParsed = time.Duration(ttl) * time.Hour
	}

	tableMetadata = &bigquery.TableMetadata{ExpirationTime: time.Now().Add(ttlParsed)}

	if labels != nil {
		tableMetadata.Labels = labels
	}
	err := dstTable.Create(ctx, tableMetadata)
	if err != nil {
		log.Fatal("→ BQ →→ Error creating temporary table", err)
	}
	query.QueryConfig.Dst = dstTable
	log.Println("→ BQ →→ Exporting query to intermediate table")

	job, err := query.Run(context.Background())
	BigQueryCheckJob(job, err)

	config, err := job.Config()
	if err != nil {
		log.Fatal("→ BQ →→ Error obtaining config", err)
	}
	tempTable := config.(*bigquery.QueryConfig).Dst
	log.Println("→ BQ →→ Temp table", tempTable.TableID)
	return tempTable.TableID
}

func BigQueryCreateTable(
	ctx context.Context,
	table *bigquery.Table,
	schema string,
	partitionTimeField string,
	clusteredFields []string,
	labels map[string]string,
) {
	schemaParsed, err := bigquery.SchemaFromJSON([]byte(schema))
	if err != nil {
		log.Fatalf("→ BQ →→ Error getting Schema from JSON %s", err)
	}

	metaData := &bigquery.TableMetadata{}

	if schema != "" {
		metaData.Schema = schemaParsed
	}
	if labels != nil {
		metaData.Labels = labels
	}

	if partitionTimeField != "" {
		log.Printf("→ BQ →→ Adding time field [%s] to partition the table", partitionTimeField)
		metaData.TimePartitioning = &bigquery.TimePartitioning{
			Field: partitionTimeField,
		}
	}

	if len(clusteredFields) > 0 && clusteredFields[0] != "" {
		log.Printf("→ BQ →→ Adding clustering fields [%s] to clustering the table", clusteredFields)
		metaData.Clustering = &bigquery.Clustering{
			Fields: clusteredFields,
		}
	}

	if err := table.Create(ctx, metaData); err != nil {
		log.Fatalf("→ BQ →→ Error creating table %s", err)
	}
}

func BigQueryDeleteTable(ctx context.Context, projectId string, dataset string, temporalTable string) {
	client := BigQueryCreateClient(ctx, projectId)
	defer client.Close()
	temporalDataset := client.DatasetInProject(projectId, dataset)
	table := temporalDataset.Table(temporalTable)
	if err := table.Delete(ctx); err != nil {
		log.Fatalf("→ BQ →→Error deleteing temporal table %s", temporalTable)
	}
}

func BigQueryGetTable(
	ctx context.Context,
	projectId string,
	datasetName string,
	tableName string,
) *bigquery.Table {
	bigQueryClient := BigQueryCreateClient(ctx, projectId)
	table := bigQueryClient.Dataset(datasetName).Table(tableName)
	return table
}

func BigQueryExportTemporalTableToCsvInGCS(
	ctx context.Context,
	projectId string,
	dataset string,
	temporalTable string,
	bucket string,
	directory string,
	headersEnable bool,
	labels map[string]string,
) []string {

	bqClient := BigQueryCreateClient(ctx, projectId)
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
	if labels != nil {
		extractor.ExtractConfig.Labels = labels
	}
	job, err := extractor.Run(ctx)
	BigQueryCheckJob(job, err)
	config, err := job.Config()
	if err != nil {
		log.Fatal("→ BQ →→ Error obtaining config", err)
	}
	tempBucket := config.(*bigquery.ExtractConfig).Dst
	log.Println("→ GCS →→ Temporal URIs", tempBucket.URIs)
	return tempBucket.URIs
}

func BigQueryExportTemporalTableToJSONInGCS(
	ctx context.Context,
	projectId string,
	dataset string,
	temporalTable string,
	bucket string,
	directory string,
	compressObjects bool,
	labels map[string]string,
) []string {
	bqClient := BigQueryCreateClient(ctx, projectId)
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
	if labels != nil {
		extractor.ExtractConfig.Labels = labels
	}
	job, err := extractor.Run(ctx)
	BigQueryCheckJob(job, err)
	config, err := job.Config()
	if err != nil {
		log.Fatal("→ BQ →→ Error obtaining config", err)
	}
	tempBucket := config.(*bigquery.ExtractConfig).Dst
	log.Println("→ GCS →→ Temporal URIs", tempBucket.URIs)
	return tempBucket.URIs
}

func BigQueryCheckIfTableExists(
	ctx context.Context,
	table *bigquery.Table,
) bool {
	log.Println("→ BQ →→ Checking if the table exists")
	_, err := table.Metadata(ctx)
	if err != nil {
		return false
	}
	return true
}

func BigQueryGetStorageRef(
	bucketUri string,
	sourceDataFormat string,
) *bigquery.GCSReference {
	log.Printf("→ GCS →→ Getting gcsRef from uri %s, format %s", bucketUri, sourceDataFormat)
	gcsRef := bigquery.NewGCSReference(bucketUri)

	var dataFormat bigquery.DataFormat
	if sourceDataFormat == "JSON" {
		dataFormat = bigquery.JSON
	}

	gcsRef.FileConfig = bigquery.FileConfig{SourceFormat: dataFormat}
	return gcsRef
}

func BigQueryCheckJob(job *bigquery.Job, err error) {
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

func BigQueryParseResultsToJson(it *bigquery.RowIterator, ch chan map[string]bigquery.Value) {
	log.Println("→ BQ →→ Parsing results to JSON")

	for {
		var values []bigquery.Value
		err := it.Next(&values)

		if err == iterator.Done {
			close(ch)
			break
		}
		if err != nil {
			log.Fatalf("→ BQ →→ Error: %v", err)
		}

		var dataMapped = toMapJson(values, it.Schema)
		ch <- dataMapped
	}
}

// Private Functions

func toMapJson(values []bigquery.Value, schema bigquery.Schema) map[string]bigquery.Value {
	var columnNames = BigQueryGetColumnNamesFromTableSchema(schema)
	var dataMapped = make(map[string]bigquery.Value)
	for i := 0; i < len(columnNames); i++ {
		if schema[i].Type == "RECORD" {
			if values[i] == nil {
				dataMapped[columnNames[i]] = values[i]
				continue
			}
			valuesNested := values[i].([]bigquery.Value)
			var valuesParsed = make([]map[string]bigquery.Value, len(valuesNested))
			var aux = make(map[string]bigquery.Value)
			for c := 0; c < len(valuesNested); c++ {
				if reflect.TypeOf(valuesNested[c]).Kind() != reflect.Interface &&
					reflect.TypeOf(valuesNested[c]).Kind() != reflect.Slice {
					var columnNamesNested = BigQueryGetColumnNamesFromTableSchema(schema[i].Schema)
					aux[columnNamesNested[c]] = valuesNested[c]
					dataMapped[columnNames[i]] = aux
				} else {
					valuesParsed[c] = toMapJson(valuesNested[c].([]bigquery.Value), schema[i].Schema)
					dataMapped[columnNames[i]] = valuesParsed
				}
			}
		} else {
			dataMapped[columnNames[i]] = values[i]
		}
	}
	return dataMapped
}
