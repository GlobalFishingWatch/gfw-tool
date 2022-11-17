package bq2gcs

import (
	"github.com/GlobalFishingWatch/gfw-tool/internal/action/bq2gcs"
	"github.com/GlobalFishingWatch/gfw-tool/types"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
)

var exportBqToGcsViper *viper.Viper

func init() {
	exportBqToGcsViper = viper.New()

	Bq2Gcs.AddCommand(exportBQtoGCS)

	exportBQtoGCS.Flags().StringP("project-id", "", "", "The destination project id")
	exportBQtoGCS.MarkFlagRequired("project-id")

	exportBQtoGCS.Flags().StringP("bq-query", "", "", "The query to execute to export the data")
	exportBQtoGCS.MarkFlagRequired("bq-query")

	exportBQtoGCS.Flags().StringP("bq-temporal-dataset", "", "0_ttl24h", "The dataset to create a temporal table")

	exportBQtoGCS.Flags().StringP("gcs-destination-format", "", "CSV", "CSV or JSON")

	exportBQtoGCS.Flags().StringP("gcs-compress-objects", "", "false", "Enable to compress destination objects (GZIP)")

	exportBQtoGCS.Flags().StringP("gcs-bucket", "", "", "The destination bucket")
	exportBQtoGCS.MarkFlagRequired("gcs-bucket")

	exportBQtoGCS.Flags().StringP("gcs-bucket-directory", "", "", "The destination bucket directory")
	exportBQtoGCS.MarkFlagRequired("gcs-bucket-directory")

	exportBQtoGCS.Flags().StringP("gcs-bucket-destination-object-name", "", "", "The destination bucket object name")
	exportBQtoGCS.MarkFlagRequired("gcs-bucket-destination-object-name")

	exportBQtoGCS.Flags().StringP("gcs-headers-enable", "", "false", "Enable or disable to include headers in the CSVs")

	exportBQtoGCS.Flags().StringP("gcs-export-headers-as-a-file", "", "true", "Export empty CSV and its first row include the headers")

	exportBqToGcsViper.BindPFlags(exportBQtoGCS.Flags())
}

var exportBQtoGCS = &cobra.Command{
	Use:   "export-bq-to-gcs",
	Short: "Export data from BigQuery (query) to a GCS bucket (CSV or JSON)",
	Long: `Export data from Biquery (query) to a GCS Bucket (CSV or JSON)
Format:
	gfw-tools bq2gcs export-bq-to-gcs \ 
		--project-id= \
		--bq-query= \
		--gcs-bucket= \
		--gcs-bucket-directory= \
		--gcs-bucket-destination-object-name=
`,
	Run: func(cmd *cobra.Command, args []string) {
		params := types.BQ2GCSExportDataToGCSConfig{
			ProjectId:            exportBqToGcsViper.GetString("project-id"),
			Query:                exportBqToGcsViper.GetString("bq-query"),
			TemporalDataset:      exportBqToGcsViper.GetString("bq-temporal-dataset"),
			Bucket:               exportBqToGcsViper.GetString("gcs-bucket"),
			BucketDirectory:      exportBqToGcsViper.GetString("gcs-bucket-directory"),
			BucketDstObjectName:  exportBqToGcsViper.GetString("gcs-bucket-destination-object-name"),
			DestinationFormat:    exportBqToGcsViper.GetString("gcs-destination-format"),
			CompressObjects:      exportBqToGcsViper.GetBool("gcs-compress-objects"),
			HeadersEnable:        exportBqToGcsViper.GetBool("gcs-headers-enable"),
			ExportHeadersAsAFile: exportBqToGcsViper.GetBool("gcs-export-headers-as-a-file"),
		}
		log.Printf("→ Config: [%s]", params)

		log.Printf("→ Executing export data from bq to gcs (%s) command", params.DestinationFormat)
		bq2gcs.ExportDataFromBigQueryQueryToGCS(params)
		log.Printf("→ Executing export data from bq to gcs (%s) finished", params.DestinationFormat)
	},
}
