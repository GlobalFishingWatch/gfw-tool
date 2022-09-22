package cmd

import (
	"github.com/GlobalFishingWatch/gfw-tool/cmd/bq2gcs"
	"github.com/GlobalFishingWatch/gfw-tool/cmd/gcs"
	"github.com/spf13/cobra"
	"log"
	"os"
)

var rootCmd = &cobra.Command{
	Use:   "gfw-tools",
	Short: "A CLI to encapsulate common processes",
}

func init() {
	rootCmd.AddCommand(bq2gcs.Bq2Gcs)
	rootCmd.AddCommand(gcs.Gcs)
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		log.Println(err)
		os.Exit(1)
	}
}
