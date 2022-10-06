package gcs

import (
	"github.com/GlobalFishingWatch/gfw-tool/internal/action/gcs"
	"github.com/GlobalFishingWatch/gfw-tool/types"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
)

var gcsCopyBucketDirectoryViper *viper.Viper

func init() {

	gcsCopyBucketDirectoryViper = viper.New()

	Gcs.AddCommand(copyBucketDirectory)

	copyBucketDirectory.Flags().StringP("gcs-source-bucket", "", "", "The source bucket")
	copyBucketDirectory.MarkFlagRequired("gcs-source-bucket")

	copyBucketDirectory.Flags().StringP("gcs-source-directory", "", "", "The source directory")
	copyBucketDirectory.MarkFlagRequired("gcs-source-directory")

	copyBucketDirectory.Flags().StringP("gcs-destination-bucket", "", "", "The destination bucket")
	copyBucketDirectory.MarkFlagRequired("gcs-destination-bucket")

	copyBucketDirectory.Flags().StringP("gcs-destination-directory", "", "", "The destination bucket")
	copyBucketDirectory.MarkFlagRequired("gcs-destination-directory")

	gcsCopyBucketDirectoryViper.BindPFlags(copyBucketDirectory.Flags())
}

var copyBucketDirectory = &cobra.Command{
	Use:   "copy-bucket-directory",
	Short: "Copy a all content or a specific directory from one bucket to another",
	Long: `
Format:
	gfw-tools gcs copy-bucket-directory \
		--gcs-source-bucket= \
		--gcs-source-directory= \
		--gcs-destination-bucket= \ 
		--gcs-destination-directory=
`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("→ Executing copy bucket directory command")

		params := types.GCSCopyBucketDirectoryConfig{
			SrcBucket:    gcsCopyBucketDirectoryViper.GetString("gcs-source-bucket"),
			SrcDirectory: gcsCopyBucketDirectoryViper.GetString("gcs-source-directory"),
			DstBucket:    gcsCopyBucketDirectoryViper.GetString("gcs-destination-bucket"),
			DstDirectory: gcsCopyBucketDirectoryViper.GetString("gcs-destination-directory"),
		}
		log.Printf("→ Config [%s]", params)

		gcs.CopyBucketDirectory(params)
		log.Println("→ Executing copy bucket directory  command")
	},
}
