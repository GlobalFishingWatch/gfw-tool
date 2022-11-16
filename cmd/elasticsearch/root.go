package elasticsearch

import (
	"github.com/spf13/cobra"
)

var Elasticsearch = &cobra.Command{
	Use:   "elasticsearch",
	Short: "A CLI to manage elasticsearch",
}
