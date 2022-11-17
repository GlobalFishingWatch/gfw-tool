package bq2psql

import (
	"github.com/GlobalFishingWatch/gfw-tool/internal/action/bq2psql"
	"github.com/GlobalFishingWatch/gfw-tool/types"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
)

var exportViper *viper.Viper

func init() {

	exportViper = viper.New()

	Bq2Psql.AddCommand(exportCmd)

	exportCmd.Flags().StringP("project-id", "p", "", "Project id related to BigQuery database (required)")
	exportCmd.MarkFlagRequired("project-id")
	exportCmd.Flags().StringP("query", "q", "", "Query to find data in BigQuery (required)")
	exportCmd.MarkFlagRequired("query")
	exportCmd.Flags().StringP("table-name", "t", "", "The name of the new table")
	exportCmd.MarkFlagRequired("table-name")
	exportCmd.Flags().StringP("table-schema", "", "", "The schema to create the table")

	exportCmd.Flags().StringP("postgres-address", "", "", "The address of the database")
	exportCmd.MarkFlagRequired("postgres-address")
	exportCmd.Flags().StringP("postgres-user", "", "", "The destination credentials user")
	exportCmd.MarkFlagRequired("postgres-user")
	exportCmd.Flags().StringP("postgres-password", "", "", "The destination credentials password")
	exportCmd.MarkFlagRequired("postgres-password")
	exportCmd.Flags().StringP("postgres-database", "", "", "The destination database name")
	exportCmd.MarkFlagRequired("postgres-database")

	exportViper.BindPFlags(exportCmd.Flags())
}

var exportCmd = &cobra.Command{
	Use:   "export",
	Short: "Export data from BigQuery to Postgres",
	Long: `Export data from BigQuery to Postgres
Format:
	bq2psql export --project-id= --query= --table-name= --table-schema= --postgres-address= --postgres-user= --postgres-password= --postgres-database= --view-name=
Example:
	bq2psql export \
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

		params := types.BQ2PSQLExportConfig{
			Query:     exportViper.GetString("import-query"),
			ProjectId: exportViper.GetString("import-project-id"),
			TableName: exportViper.GetString("import-table-name"),
			Schema:    exportViper.GetString("import-table-schema"),
		}

		postgresConfig := types.PostgresConfig{
			Addr:     exportViper.GetString("import-postgres-address"),
			User:     exportViper.GetString("import-postgres-user"),
			Password: exportViper.GetString("import-postgres-password"),
			Database: exportViper.GetString("import-postgres-database"),
		}

		bq2psql.ExportBigQueryToPostgres(params, postgresConfig)
		log.Println("→ Executing Import command finished")
	},
}
