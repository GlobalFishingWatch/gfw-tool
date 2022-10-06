package gcs

import (
	"github.com/GlobalFishingWatch/gfw-tool/internal/action/gcs"
	"github.com/GlobalFishingWatch/gfw-tool/types"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
)

var mergeMultipleCsvViper *viper.Viper

func init() {

	mergeMultipleCsvViper = viper.New()

	Gcs.AddCommand(mergeMultipleCsv)

	mergeMultipleCsv.Flags().StringP("project-id", "", "", "The destination project id")
	mergeMultipleCsv.MarkFlagRequired("project-id")

	mergeMultipleCsv.Flags().StringP("gcs-destination-format", "", "CSV", "CSV or JSON")

	mergeMultipleCsv.Flags().StringP("gcs-compress-objects", "", "false", "Enable to compress destination objects (GZIP)")

	mergeMultipleCsv.Flags().StringP("gcs-source-bucket", "", "", "The source bucket")
	mergeMultipleCsv.MarkFlagRequired("gcs-source-bucket")

	mergeMultipleCsv.Flags().StringP("gcs-source-bucket-directory", "", "", "The source bucket directory")
	mergeMultipleCsv.MarkFlagRequired("gcs-source-bucket-directory")

	mergeMultipleCsv.Flags().StringP("gcs-destination-bucket", "", "", "The destination bucket")

	mergeMultipleCsv.Flags().StringP("gcs-destination-bucket-directory", "", "", "The destination bucket directory")

	mergeMultipleCsv.Flags().StringP("gcs-source-merged-object-name", "", "", "The final object created after merging all the CSVs")
	mergeMultipleCsv.MarkFlagRequired("gcs-source-merged-object-name")

	mergeMultipleCsv.Flags().StringP("gcs-destination-object-name", "", "", "The destination csv filename")
	mergeMultipleCsv.MarkFlagRequired("gcs-destination-object-name")

	mergeMultipleCsvViper.BindPFlags(mergeMultipleCsv.Flags())

}

var mergeMultipleCsv = &cobra.Command{
	Use:   "merge-multiple-objects",
	Short: "Merge all the CSVs exported in a bucket into another one and delete the previous",
	Long: `Merge all the CSVs exported in a bucket into another one and delete the previous
Format:
	gfw-tools gcs merge-multiple-csv \ 
		--project-id= \
		--gcs-source-bucket= \
		--gcs-source-bucket-directory= \
		--gcs-source-merged-object-name= \
		--gcs-destination-bucket= \
		--gcs-destination-bucket-directory= \
		--gcs-destination-object-name= 
`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("→ Executing merge csv command")
		params := types.GCSMergeMultipleObjectsConfig{
			ProjectId:            mergeMultipleCsvViper.GetString("project-id"),
			SourceBucket:         mergeMultipleCsvViper.GetString("gcs-source-bucket"),
			SourceDirectory:      mergeMultipleCsvViper.GetString("gcs-source-bucket-directory"),
			DestinationBucket:    mergeMultipleCsvViper.GetString("gcs-destination-bucket"),
			DestinationDirectory: mergeMultipleCsvViper.GetString("gcs-destination-bucket-directory"),
			DestinationFormat:    mergeMultipleCsvViper.GetString("gcs-destination-format"),
			CompressObject:       mergeMultipleCsvViper.GetBool("gcs-compress-objects"),
			MergedObjectName:     mergeMultipleCsvViper.GetString("gcs-source-merged-object-name"),
			DstObjectName:        mergeMultipleCsvViper.GetString("gcs-destination-object-name"),
		}
		log.Printf("→ Config [%s]", params)

		gcs.MergeMultipleObjects(params)
		log.Println("→ Executing merge csv finished")
	},
}
