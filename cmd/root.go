package cmd

import (
	"github.com/GlobalFishingWatch/gfw-tool/cmd/bigquery"
	"github.com/GlobalFishingWatch/gfw-tool/cmd/bq2es"
	"github.com/GlobalFishingWatch/gfw-tool/cmd/bq2gcs"
	"github.com/GlobalFishingWatch/gfw-tool/cmd/bq2psql"
	"github.com/GlobalFishingWatch/gfw-tool/cmd/elasticsearch"
	"github.com/GlobalFishingWatch/gfw-tool/cmd/gcs"
	"github.com/GlobalFishingWatch/gfw-tool/cmd/gcs2bq"
	"github.com/GlobalFishingWatch/gfw-tool/cmd/postgres"
	"github.com/spf13/cobra"
	"log"
	"os"
)

var rootCmd = &cobra.Command{
	Use:   "gfw-tools",
	Short: "A CLI to encapsulate common processes",
}

func init() {
	rootCmd.AddCommand(bigquery.Bigquery)
	rootCmd.AddCommand(bq2es.Bq2Es)
	rootCmd.AddCommand(bq2gcs.Bq2Gcs)
	rootCmd.AddCommand(bq2psql.Bq2Psql)
	rootCmd.AddCommand(elasticsearch.Elasticsearch)
	rootCmd.AddCommand(gcs.Gcs)
	rootCmd.AddCommand(gcs2bq.Gcs2Bq)
	rootCmd.AddCommand(postgres.Postgres)
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		log.Println(err)
		os.Exit(1)
	}
}
