package bq2es

import (
	"github.com/GlobalFishingWatch/gfw-tool/internal/action/bq2es"
	"github.com/GlobalFishingWatch/gfw-tool/types"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
)

var exportBqToEsViper *viper.Viper

func init() {

	exportBqToEsViper = viper.New()

	Bq2Es.AddCommand(exportBq2EsCmd)

	exportBq2EsCmd.Flags().StringP("project-id", "p", "", "Project id related to BigQuery database (required)")
	exportBq2EsCmd.MarkFlagRequired("project-id")
	exportBq2EsCmd.Flags().StringP("query", "q", "", "Query to find data in BigQuery (required)")
	exportBq2EsCmd.MarkFlagRequired("query")
	exportBq2EsCmd.Flags().StringP("elastic-search-url", "u", "", "URL exposed by Elasticsearch cluster (required)")
	exportBq2EsCmd.MarkFlagRequired("elastic-search-url")
	exportBq2EsCmd.Flags().StringP("index-name", "i", "", "The name of the destination index (required)")
	exportBq2EsCmd.MarkFlagRequired("index-name")
	exportBq2EsCmd.Flags().StringP("import-mode", "m", "recreate", "Import mode [recreate|append]")
	exportBq2EsCmd.Flags().StringP("normalize", "n", "", "The property name to normalize")
	exportBq2EsCmd.Flags().StringP("normalize-property-name", "", "", "The property name to store the normalized value")
	exportBq2EsCmd.Flags().StringP("normalize-endpoint", "", "", "The final endpoint to normalize")
	exportBq2EsCmd.Flags().StringP("on-error", "e", "reindex", "Action to do if command fails [reindex|delete|keep]")

	exportBqToEsViper.BindPFlags(exportBq2EsCmd.Flags())

}

var exportBq2EsCmd = &cobra.Command{
	Use:   "export",
	Short: "Export data from BigQuery to Elasticsearch",
	Long: `Export data from BigQuery to Elasticsearch
Format:
	bq2es-tool export --project-id= --query= --elastic-search-url= --index-name= --normalize=
Example:
	bq2es-tool export 
		--project-id=world-fishing-827 
		--query="SELECT * FROM vessels" 
		--normalize=shipname
		--normalize-property-name=normalizedShipname
		--normalize-endpoint=https://us-central1-world-fishing-827.cloudfunctions.net/normalize_shipname_http 
		--elastic-search-url="https://user:password@elastic.gfw.org"`,
	Run: func(cmd *cobra.Command, args []string) {
		params := types.BQ2ESImportConfig{
			Query:                  exportBqToEsViper.GetString("query"),
			ElasticSearchUrl:       exportBqToEsViper.GetString("elastic-search-url"),
			ProjectId:              exportBqToEsViper.GetString("project-id"),
			IndexName:              exportBqToEsViper.GetString("index-name"),
			ImportMode:             exportBqToEsViper.GetString("import-mode"),
			Normalize:              exportBqToEsViper.GetString("normalize"),
			NormalizedPropertyName: exportBqToEsViper.GetString("normalize-property-name"),
			NormalizeEndpoint:      exportBqToEsViper.GetString("normalize-endpoint"),
			OnError:                exportBqToEsViper.GetString("on-error"),
		}

		log.Println("→ Executing export command")
		bq2es.ExportBigQueryToElasticSearch(params)
	},
}
