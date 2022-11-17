package bq2psql

import (
	"github.com/spf13/cobra"
)

var Bq2Psql = &cobra.Command{
	Use:   "bq2psql",
	Short: "A CLI to manage BigQuery to Postgres commands",
}
