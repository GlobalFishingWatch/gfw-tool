package elasticsearch

import (
	action "github.com/GlobalFishingWatch/gfw-tool/internal/action/elasticsearch"
	"github.com/GlobalFishingWatch/gfw-tool/types"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
)

var deleteIndicesByPrefixViper *viper.Viper

func init() {
	deleteIndicesByPrefixViper = viper.New()

	Elasticsearch.AddCommand(deleteIndicesByPrefixCmd)

	deleteIndicesByPrefixCmd.Flags().StringP("index-prefix", "", "", "The prefix of the indices to delete (required)")
	deleteIndicesByPrefixCmd.MarkFlagRequired("index-prefix")
	deleteIndicesByPrefixCmd.Flags().StringP("no-delete-index", "", "", "Index name that you do not want to delete (optional)")
	deleteIndicesByPrefixCmd.Flags().StringP("elastic-search-url", "", "", "URL exposed by Elasticsearch cluster (required)")
	deleteIndicesByPrefixCmd.MarkFlagRequired("elastic-search-url")

	deleteIndicesByPrefixViper.BindPFlags(deleteIndicesByPrefixCmd.Flags())

}

var deleteIndicesByPrefixCmd = &cobra.Command{
	Use:   "delete-indices-by-prefix",
	Short: "Delete indices by prefix",
	Long: `Delete indices by prefix
Format:
	gfw-tool elasticsearch delete-indices-by-prefix --index-prefix=[name] --no-delete-index=test-vessels-2021-01 --elastic-search-url=[url]
Example:
	gfw-tool elasticsearch delete-indices-by-prefix --index-prefix=test-vessels --no-delete-index=test-vessels-2021-01 --elastic-search-url="https://user:password@elastic.gfw.org"`,
	Run: func(cmd *cobra.Command, args []string) {
		params := types.ElasticsearchDeleteIndicesByPrefixConfig{
			Prefix:           deleteIndicesByPrefixViper.GetString("index-prefix"),
			NoDeleteIndex:    deleteIndicesByPrefixViper.GetString("no-delete-index"),
			ElasticSearchUrl: deleteIndicesByPrefixViper.GetString("elastic-search-url"),
		}
		log.Println("→ Executing Delete Indices by prefix command")
		action.DeleteIndicesByPrefix(params)
		log.Println("→ Delete Indices by prefix command finished")
	},
}
