package gcs2bq

import (
	"github.com/GlobalFishingWatch/gfw-tool/internal/action/gcs2bq"
	"github.com/GlobalFishingWatch/gfw-tool/types"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
)

var exportGCStoBQViper *viper.Viper

func init() {

	exportGCStoBQViper = viper.New()

	Gcs2Bq.AddCommand(exportGCStoBQCmd)

	exportGCStoBQCmd.Flags().StringP("project-id", "", "", "The id of the project")
	exportGCStoBQCmd.MarkFlagRequired("project-id")

	exportGCStoBQCmd.Flags().StringP("bucket-uri", "", "", "The bucket uri to get data from gcs")
	exportGCStoBQCmd.MarkFlagRequired("bucket-uri")

	exportGCStoBQCmd.Flags().StringP("source-data-format", "", "JSON", "The format source file. Values accepted: JSON. Default: JSON")
	exportGCStoBQCmd.MarkFlagRequired("source-data-format")

	exportGCStoBQCmd.Flags().StringP("dataset-name", "", "", "The name of the destination dataset")
	exportGCStoBQCmd.MarkFlagRequired("dataset-name")

	exportGCStoBQCmd.Flags().StringP("table-name", "", "", "The name of the destination table")
	exportGCStoBQCmd.MarkFlagRequired("table-name")

	exportGCStoBQCmd.Flags().StringP("mode", "", "", "The mode used to execute the command. Modes available: [create, autodetect, append].")
	exportGCStoBQCmd.MarkFlagRequired("mode")

	exportGCStoBQCmd.Flags().StringP("schema", "", "", "The schema used to create the destination table. Required if mode = create")

	exportGCStoBQCmd.Flags().StringP("partition-time-field", "", "", "The time field used to partition the table. Available in create mode")

	exportGCStoBQCmd.Flags().StringP("clustered-fields", "", "", "The field(s) used to clustered the table. For multiples values use comma as separator. Example: imo,call_sign")

	exportGCStoBQViper.BindPFlags(exportGCStoBQCmd.Flags())

}

var exportGCStoBQCmd = &cobra.Command{
	Use:   "export",
	Short: "export data from GCS to BigQuery",
	Long: `export data from GCS to BigQuery
Format:
	gfw-tool gcs2bq export 
		--project_id=[id] 
		--bucket-uri=[uri]
		--dataset-name=[dataset] 
		--table-name=[table]
		--mode=[mode]
Example:
	gfw-tool gcs2bq export \
		--project-id=world-fishing-827 \
		--bucket-uri=gs://test-spire/foo_object_name \
		--source-data-format=JSON \
		--dataset=scratch \
		--table=messages_api_stream \
		--mode=create \
		--schema="[{ \"name\": \"id\", \"type\": \"STRING\" }, { \"name\": \"nmea\", \"type\": \"STRING\" }]"
	`,
	Run: func(cmd *cobra.Command, args []string) {

		params := types.GCSExportDataToBigQueryConfig{
			ProjectId:          exportGCStoBQViper.GetString("project-id"),
			BucketUri:          exportGCStoBQViper.GetString("bucket-uri"),
			SourceDataFormat:   exportGCStoBQViper.GetString("source-data-format"),
			DatasetName:        exportGCStoBQViper.GetString("dataset-name"),
			TableName:          exportGCStoBQViper.GetString("table-name"),
			Mode:               exportGCStoBQViper.GetString("mode"),
			Schema:             exportGCStoBQViper.GetString("schema"),
			PartitionTimeField: exportGCStoBQViper.GetString("partition-time-field"),
			ClusteredFields:    exportGCStoBQViper.GetString("clustered-fields"),
		}
		log.Printf("→ Config: [%s]", params)

		log.Println("→ Executing import command")
		gcs2bq.Export(params)
		log.Println("→ import command completed")
	},
}
