package gcs

import (
	"github.com/GlobalFishingWatch/gfw-tool/internal/action/gcs"
	"github.com/GlobalFishingWatch/gfw-tool/types"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
)

var gcsUploadObjectViper *viper.Viper

func init() {

	gcsUploadObjectViper = viper.New()

	Gcs.AddCommand(uploadObject)

	uploadObject.Flags().StringP("gcs-destination-bucket", "", "", "The destination project id")
	uploadObject.MarkFlagRequired("gcs-destination-bucket")

	uploadObject.Flags().StringP("gcs-destination-directory", "", "", "The destination bucket")
	uploadObject.MarkFlagRequired("gcs-destination-directory")

	uploadObject.Flags().StringP("gcs-destination-object-name", "", "", "The bucket directory")
	uploadObject.MarkFlagRequired("gcs-destination-object-name")

	uploadObject.Flags().StringP("gcs-destination-object-content", "", "", "The bucket directory")
	uploadObject.MarkFlagRequired("gcs-destination-object-content")

	gcsUploadObjectViper.BindPFlags(uploadObject.Flags())
}

var uploadObject = &cobra.Command{
	Use:   "upload-object",
	Short: "Upload a string as a file in GCS",
	Long: `Upload a string as a file in GCS
Format:
	gfw-tools gcs upload-object \
		--gcs-destination-bucket= \
		--gcs-destination-bucket-directory= \
		--gcs-destination-object-name= \ 
		--gcs-destination-object-content=
`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("→ Executing upload object command")

		params := types.GCSUploadObjectConfig{
			DstBucket:     gcsUploadObjectViper.GetString("gcs-destination-bucket"),
			DstDirectory:  gcsUploadObjectViper.GetString("gcs-destination-directory"),
			DstObjectName: gcsUploadObjectViper.GetString("gcs-destination-object-name"),
			Content:       gcsUploadObjectViper.GetString("gcs-destination-object-content"),
		}
		log.Printf("→ Config [%s]", params)

		gcs.UploadObject(params)
		log.Println("→ Executing upload object command")
	},
}
