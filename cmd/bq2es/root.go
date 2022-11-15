package bq2es

import (
	"github.com/spf13/cobra"
)

var Bq2Es = &cobra.Command{
	Use:   "bq2es",
	Short: "A CLI to manage BigQuery to Elastic Search commands",
}
