package elasticsearch

import (
	"github.com/GlobalFishingWatch/gfw-tool/internal/action/elasticsearch"
	"github.com/GlobalFishingWatch/gfw-tool/types"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
)

var addAliasViper *viper.Viper

func init() {

	addAliasViper = viper.New()

	Elasticsearch.AddCommand(addAliasCmd)

	addAliasCmd.Flags().StringP("index-name", "i", "", "The name of the index to create alias")
	addAliasCmd.MarkFlagRequired("index-name")

	addAliasCmd.Flags().StringP("alias", "a", "", "Alias name")
	addAliasCmd.MarkFlagRequired("alias")

	addAliasCmd.Flags().StringP("elastic-search-url", "u", "", "URL exposed by Elasticsearch cluster (required)")
	addAliasCmd.MarkFlagRequired("elastic-search-url")

	addAliasViper.BindPFlags(addAliasCmd.Flags())
}

var addAliasCmd = &cobra.Command{
	Use:   "add-alias",
	Short: "Add an alias to an index",
	Long: `Adds an alias to an index
Format:
	gfw-tool elasticsearch add-alias --index-name=[name] --alias=[name] --elastic-search-url=[url]
Example:
	gfw-tool elasticsearch add-alias --index-name=gfw-tasks-2020 --alias=gfw-tasks --elastic-search-url=https://user:password@elastic.gfw.org`,
	Run: func(cmd *cobra.Command, args []string) {
		params := types.ElasticsearchAddAliasConfig{
			IndexName:        viper.GetString("index-name"),
			Alias:            viper.GetString("alias"),
			ElasticSearchUrl: viper.GetString("elastic-search-url"),
		}

		log.Println("→ Executing Add Alias command")
		elasticsearch.AddAlias(params)
		log.Println("→ Execution completed")
	},
}
