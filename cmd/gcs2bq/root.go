package gcs2bq

import (
	"github.com/spf13/cobra"
)

var Gcs2Bq = &cobra.Command{
	Use:   "gcs2bq",
	Short: "A CLI to manage Google Cloud Storage to Big Query commands",
}
