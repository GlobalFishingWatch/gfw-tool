package gcs

import (
	"github.com/spf13/cobra"
)

var Gcs = &cobra.Command{
	Use:   "gcs",
	Short: "A CLI to manage Google Cloud Storage commands",
}
