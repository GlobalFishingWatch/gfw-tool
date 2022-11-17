package postgres

import (
	"github.com/spf13/cobra"
)

var Postgres = &cobra.Command{
	Use:   "postgres",
	Short: "A CLI to manage postgres commands",
}
