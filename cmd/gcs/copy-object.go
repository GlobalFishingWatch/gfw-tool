package gcs

import (
	"github.com/GlobalFishingWatch/gfw-tool/internal/action/gcs"
	"github.com/GlobalFishingWatch/gfw-tool/types"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
)

var gcsCopyObjectViper *viper.Viper

func init() {

	gcsCopyObjectViper = viper.New()

	Gcs.AddCommand(copyObject)

	copyObject.Flags().StringP("gcs-source-bucket", "", "", "The source bucket")
	copyObject.MarkFlagRequired("gcs-source-bucket")

	copyObject.Flags().StringP("gcs-source-directory", "", "", "The source directory")
	copyObject.MarkFlagRequired("gcs-source-directory")

	copyObject.Flags().StringP("gcs-source-object-name", "", "", "The source object name")
	copyObject.MarkFlagRequired("gcs-source-object-name")

	copyObject.Flags().StringP("gcs-destination-bucket", "", "", "The destination bucket")
	copyObject.MarkFlagRequired("gcs-destination-bucket")

	copyObject.Flags().StringP("gcs-destination-directory", "", "", "The destination bucket")
	copyObject.MarkFlagRequired("gcs-destination-directory")

	copyObject.Flags().StringP("gcs-destination-object-name", "", "", "The destination object name")
	copyObject.MarkFlagRequired("gcs-destination-object-name")

	gcsCopyObjectViper.BindPFlags(copyObject.Flags())
}

var copyObject = &cobra.Command{
	Use:   "copy-object",
	Short: "Copy an object from one location to another in the same or another bucket",
	Long: `
Format:
	gfw-tools gcs copy-object \
		--gcs-source-bucket= \
		--gcs-source-directory= \
		--gcs-source-object-name= \
		--gcs-destination-bucket= \ 
		--gcs-destination-directory= \
		--gcs-destination-object-name=
`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("→ Executing copy object command")

		params := types.GCSCopyObjectConfig{
			SrcBucket:     gcsCopyObjectViper.GetString("gcs-source-bucket"),
			SrcDirectory:  gcsCopyObjectViper.GetString("gcs-source-directory"),
			SrcObjectName: gcsCopyObjectViper.GetString("gcs-source-object-name"),
			DstBucket:     gcsCopyObjectViper.GetString("gcs-destination-bucket"),
			DstDirectory:  gcsCopyObjectViper.GetString("gcs-destination-directory"),
			DstObjectName: gcsCopyObjectViper.GetString("gcs-destination-object-name"),
		}
		log.Printf("→ Config [%s]", params)

		gcs.CopyObject(params)
		log.Println("→ Executing copy object command")
	},
}
