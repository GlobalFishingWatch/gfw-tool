package elasticsearch

import (
	"github.com/GlobalFishingWatch/gfw-tool/internal/action/elasticsearch"
	"github.com/GlobalFishingWatch/gfw-tool/types"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
)

var deleteIndexViper *viper.Viper

func init() {

	deleteIndexViper = viper.New()

	Elasticsearch.AddCommand(deleteIndexCmd)

	deleteIndexCmd.Flags().StringP("index-name", "", "", "The name of the destination index (required)")
	deleteIndexCmd.MarkFlagRequired("index-name")
	deleteIndexCmd.Flags().StringP("elastic-search-url", "", "", "URL exposed by Elasticsearch cluster (required)")
	deleteIndexCmd.MarkFlagRequired("elastic-search-url")

	deleteIndexViper.BindPFlags(deleteIndexCmd.Flags())
}

var deleteIndexCmd = &cobra.Command{
	Use:   "delete-index",
	Short: "Delete index",
	Long: `Delete index
Format:
	gfw-tool elasticsearch delete-index --index-name=[name] --elastic-search-url=[url]
Example:
	gfw-tool elasticsearch delete-index --index-name=test-vessels --elastic-search-url="https://user:password@elastic.gfw.org"`,
	Run: func(cmd *cobra.Command, args []string) {
		params := types.ElasticsearchDeleteIndexConfig{
			IndexName:        viper.GetString("index-name"),
			ElasticSearchUrl: viper.GetString("elastic-search-url"),
		}
		log.Println("→ Executing Delete Index command")
		elasticsearch.DeleteIndex(params)
		log.Println("→ Delete Index command finished")
	},
}
