package gcs

import (
	"github.com/GlobalFishingWatch/gfw-tool/internal/action/gcs"
	"github.com/GlobalFishingWatch/gfw-tool/types"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
)

var gcsGCSDeleteObjectViper *viper.Viper

func init() {

	gcsCopyBucketDirectoryViper = viper.New()

	Gcs.AddCommand(deleteObjectCmd)

	deleteObjectCmd.Flags().StringP("project-id", "", "", "The id of the project")
	deleteObjectCmd.MarkFlagRequired("project-id")

	deleteObjectCmd.Flags().StringP("bucket-name", "", "", "The bucket uri to get data from gcs.")
	deleteObjectCmd.MarkFlagRequired("bucket-name")

	deleteObjectCmd.Flags().StringP("object-name", "", "", "The object name to delete")
	deleteObjectCmd.MarkFlagRequired("object-name")

	gcsCopyBucketDirectoryViper.BindPFlags(deleteObjectCmd.Flags())
}

var deleteObjectCmd = &cobra.Command{
	Use:   "delete-object",
	Short: "Delete object",
	Long: `Delete object
Format:
	gcs-tools delete-object --project-id=[id] --bucket-name=[name] --object-name=[name]
Example:
	gcs-tools delete-object \
		--project-id=world-fishing-827 \
		--bucket-name=test-alvaro-spire \
		--object-name=scheduled__2020-09-23T08:56:00+00:00`,

	Run: func(cmd *cobra.Command, args []string) {
		log.Println("→ Executing Delete Object command")
		params := types.GCSDeleteObjectConfig{
			ProjectId:  gcsCopyBucketDirectoryViper.GetString("project-id"),
			BucketName: gcsCopyBucketDirectoryViper.GetString("bucket-name"),
			ObjectName: gcsCopyBucketDirectoryViper.GetString("object-name"),
		}
		log.Printf("→ Config [%s]", params)

		gcs.DeleteObject(params)
		log.Println("→ Execution Delete Object command completed")
	},
}
