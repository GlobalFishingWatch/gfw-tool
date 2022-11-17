package bq2psql

import (
	"cloud.google.com/go/storage"
	"context"
	"encoding/json"
	"fmt"
	"github.com/GlobalFishingWatch/gfw-tool/internal/common"
	"github.com/GlobalFishingWatch/gfw-tool/types"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	sqladmin "google.golang.org/api/sqladmin/v1beta4"
	"io/ioutil"
	"log"
	"strings"
	"time"
)

func ExportCsvBigQueryToPostgres(params types.BQ2PSQLExportCSVConfig, cloudSqlConfig types.CloudSqlConfig) {
	ctx := context.Background()

	if cloudSqlConfig.Database == "" {
		cloudSqlConfig.Database = "postgres"
	}

	// Create a temporal table
	log.Println("→ Creating temporal table from query result")
	temporalTableName := common.CreateTemporalTableFromQuery(
		ctx,
		params.ProjectId,
		params.TemporalDataset,
		"",
		params.Query,
		24,
		"",
	)

	// Export events to csv
	log.Println("→ Exporting results from temporal table to gcs")
	common.BigQueryExportTableToACSV(ctx, params.ProjectId, params.TemporalDataset, temporalTableName, params.TemporalBucket)

	// Delete intermediate table
	log.Println("→ Deleting temporal table")
	common.BigQueryDeleteTable(ctx, params.ProjectId, params.TemporalDataset, temporalTableName)

	// List objects, import data and delete object
	log.Println("→ Listing objects and importing to Postgres")
	listObjects(ctx, params.ProjectId, params.TemporalBucket, temporalTableName, cloudSqlConfig)
}

func listObjects(ctx context.Context, projectId string, bucketName string, temporalTable string, cloudSqlConfig types.CloudSqlConfig) {
	client, err := storage.NewClient(ctx)
	defer client.Close()

	if err != nil {
		log.Fatal("→ GCS →→ Error creating GCS client")
	}
	bkt := common.GetBucket(ctx, bucketName)
	prefix := fmt.Sprintf(`bq2psql-tool/%s/`, temporalTable)
	query := &storage.Query{
		Prefix: prefix,
	}
	var names []string
	it := bkt.Objects(ctx, query)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatal("→ GCS →→ Error listing objects ", err)
		}
		names = append(names, attrs.Name)
	}

	for i := 0; i < len(names); i++ {
		uri := fmt.Sprintf(`gs://%s/%s`, bucketName, names[i])
		importFileToCloudSQL(ctx, projectId, cloudSqlConfig, uri)
		obj := bkt.Object(names[i])
		if err := obj.Delete(ctx); err != nil {
			log.Fatalf("Cannot delete object with name %s", names[i])
		}
	}
}

func importFileToCloudSQL(ctx context.Context, projectId string, cloudSqlConfig types.CloudSqlConfig, uri string) {
	columns := strings.Split(cloudSqlConfig.Columns, ",")
	sqlAdminService, err := sqladmin.NewService(ctx)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Importing data to database: %s", cloudSqlConfig.Database)
	importContext := &sqladmin.InstancesImportRequest{
		ImportContext: &sqladmin.ImportContext{
			Database:   cloudSqlConfig.Database,
			ImportUser: "postgres",
			FileType:   "CSV",
			Uri:        uri,
			CsvImportOptions: &sqladmin.ImportContextCsvImportOptions{
				Table:   cloudSqlConfig.Table,
				Columns: columns,
			},
		},
	}
	var operation *sqladmin.Operation
	for {
		log.Printf("→ PSSQL →→ Importing file (%s) to cloud sql (%s) and columns %s", uri, cloudSqlConfig.Table, strings.Join(columns, ","))
		log.Printf("→ PSSQL →→ Project: %s, Instance: %s", projectId, cloudSqlConfig.Instance)
		call := sqlAdminService.Instances.Import(projectId, cloudSqlConfig.Instance, importContext)
		operation, err = call.Do()
		if err != nil {
			newErr, ok := err.(*googleapi.Error)
			if !ok {
				log.Fatal("→ PSQL →→Error ingesting ", err, newErr)
			} else if newErr.Code == 409 || newErr.Code >= 500 {
				log.Printf("→ PSQL →→ Retrying file %s in 2 min", cloudSqlConfig.Table, newErr.Body)
				time.Sleep(2 * time.Minute)
				continue
			} else {
				log.Fatal("→ PSQL →→ Error google ingesting ", err, newErr)
			}
		}
		break
	}
	for {
		client, err := google.DefaultClient(oauth2.NoContext, "https://www.googleapis.com/auth/cloud-platform")
		if err != nil {
			log.Fatal(err)
		}
		resp, err := client.Get(operation.SelfLink)
		if err != nil {
			log.Fatal("→ PSQL →→ Error obtaining status of import", err)
		}
		body, err := ioutil.ReadAll(resp.Body)
		var respJson map[string]interface{}
		err = json.Unmarshal(body, &respJson)
		if err != nil {
			log.Fatal("→ PSQL →→ Error unmarshal response", err)
		}
		log.Printf("→ PSQL  →→Status: %s", respJson["status"])
		if respJson["status"] == "PENDING" || respJson["status"] == "RUNNING" {
			time.Sleep(5 * time.Second)
			continue
		} else if respJson["status"] == "DONE" {
			if respJson["error"] != nil {
				if strings.Contains(fmt.Sprintf("%s", respJson["error"]), "cleanup after import is completed") {
					log.Println("→ PSQL →→ Cleenup error")
					break
				}
				log.Fatal("→ PSQL →→ Error importing", respJson["error"])
				panic(respJson["error"])
			} else {
				break
			}
		}
	}
}
