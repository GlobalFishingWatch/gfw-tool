package bq2gcs

import (
	"github.com/spf13/cobra"
)

var Bq2Gcs = &cobra.Command{
	Use:   "bq2gcs",
	Short: "A CLI to manage BigQuery to Cloud Storage commands",
}
