package bq2psql

import (
	"log"

	"github.com/GlobalFishingWatch/gfw-tool/internal/action/bq2psql"
	"github.com/GlobalFishingWatch/gfw-tool/types"
	"github.com/GlobalFishingWatch/gfw-tool/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var exportCSVViper *viper.Viper

func init() {

	exportCSVViper = viper.New()

	Bq2Psql.AddCommand(exportCmd)

	exportCsvCmd.Flags().StringP("project-id", "", "", "Project id related to BigQuery database (required)")
	exportCsvCmd.MarkFlagRequired("project-id")
	exportCsvCmd.Flags().StringP("query", "", "", "Query to find data in BigQuery (required)")
	exportCsvCmd.MarkFlagRequired("query")

	exportCsvCmd.Flags().StringP("temporal-dataset", "", "", "The name of dataset to the temporal table")
	exportCsvCmd.MarkFlagRequired("temporal-dataset")
	exportCsvCmd.Flags().StringP("temporal-bucket", "", "", "The name of the bucket to upload the CSV")
	exportCsvCmd.MarkFlagRequired("temporal-bucket")

	exportCsvCmd.Flags().StringP("postgres-instance", "", "", "")
	exportCsvCmd.MarkFlagRequired("postgres-instance")
	exportCsvCmd.Flags().StringP("postgres-table", "", "", "")
	exportCsvCmd.MarkFlagRequired("postgres-table")
	exportCsvCmd.Flags().StringP("postgres-table-columns", "", "", "")
	exportCsvCmd.MarkFlagRequired("postgres-table-columns")

	exportCsvCmd.Flags().StringSlice("labels", []string{}, "Labels to apply to BQ separated by comma. Example: project=api,environment=production")

	exportCSVViper.BindPFlags(exportCsvCmd.Flags())
}

var exportCsvCmd = &cobra.Command{
	Use:   "export-csv",
	Short: "Export data from BigQuery to Postgres",
	Long: `Export data from BigQuery to Postgres
Format:
	bq2psql export-cvs --project-id= --query= --table-name= --table-schema= --postgres-address= --postgres-user= --postgres-password= --postgres-database= --view-name=
Example:
	bq2psql export-cvs \
	  --project-id=world-fishing-827 \
	  --query="SELECT * FROM vessels" \
	  --table-name="vessels_2021_02_01" \
	  --table-schema="flag VARCHAR(3), first_transmission_date VARCHAR, last_transmission_date VARCHAR, id VARCHAR, mmsi VARCHAR, imo VARCHAR, callsign VARCHAR, shipname VARCHAR" \
	  --postgres-address="localhost:5432" \
	  --postgres-user="postgres" \
	  --postgres-password="XaD2sd$34Sdas1$ae" \
	  --postgres-database="postgres" 
`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("→ Executing Import command")

		params := types.BQ2PSQLExportCSVConfig{
			Query:                viper.GetString("import-csv-query"),
			ProjectId:            viper.GetString("import-csv-project-id"),
			TemporalDataset:      viper.GetString("import-csv-temporal-dataset"),
			TemporalBucket:       viper.GetString("import-csv-temporal-bucket"),
			DestinationTableName: viper.GetString("import-postgres-table-name"),
			Labels:               utils.ConvertSliceToMap(exportCSVViper.GetStringSlice("labels")),
		}

		postgresConfig := types.CloudSqlConfig{
			Instance: viper.GetString("import-csv-postgres-instance"),
			Table:    viper.GetString("import-csv-postgres-table"),
			Columns:  viper.GetString("import-csv-postgres-table-columns"),
			Database: "",
		}

		bq2psql.ExportCsvBigQueryToPostgres(params, postgresConfig)
		log.Println("→ Executing Import command finished")
	},
}
