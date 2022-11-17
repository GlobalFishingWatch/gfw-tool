package elasticsearch

import (
	"github.com/GlobalFishingWatch/gfw-tool/internal/action/elasticsearch"
	"github.com/GlobalFishingWatch/gfw-tool/types"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
)

var createIndexViper *viper.Viper

func init() {

	createIndexViper = viper.New()

	Elasticsearch.AddCommand(createIndexCmd)

	createIndexCmd.Flags().StringP("mapping", "", "", "The mapping of the destination index (required)")
	createIndexCmd.MarkFlagRequired("mapping")
	createIndexCmd.Flags().StringP("settings", "", "", "The settings of the destination index (optional)")
	createIndexCmd.Flags().StringP("index-name", "", "", "The settings of the destination index (required)")
	createIndexCmd.MarkFlagRequired("index-name")
	createIndexCmd.Flags().StringP("elastic-search-url", "", "", "URL exposed by Elasticsearch cluster (required)")
	createIndexCmd.MarkFlagRequired("elastic-search-url")

	createIndexViper.BindPFlags(addAliasCmd.Flags())
}

var createIndexCmd = &cobra.Command{
	Use:   "create-index",
	Short: "Create new index applying a custom mapping",
	Long: `Create new index applying a custom mapping
Format:
	gfw-tool elasticsearch create-index --mapping=[mapping] --index-name=[name] --elastic-search-url=[url]
Example:
	gfw-tool elasticsearch create-index --mapping={} --index-name=test-vessels --elastic-search-url="https://user:password@elastic.gfw.org"`,
	Run: func(cmd *cobra.Command, args []string) {
		params := types.ElasticsearchCreateIndexConfig{
			IndexName:        createIndexViper.GetString("index-name"),
			Mapping:          createIndexViper.GetString("mapping"),
			Settings:         createIndexViper.GetString("settings"),
			ElasticSearchUrl: createIndexViper.GetString("elastic-search-url"),
		}
		log.Println("→ Executing Create Index command")
		elasticsearch.CreateIndexWithCustomMapping(params)
		log.Println("→ Create Index command finished")
	},
}
