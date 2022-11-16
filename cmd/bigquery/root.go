package bigquery

import (
	"github.com/spf13/cobra"
)

var Bigquery = &cobra.Command{
	Use:   "bigquery",
	Short: "A CLI to manage Big Query commands",
}
