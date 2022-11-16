package bigquery

import (
	"github.com/GlobalFishingWatch/gfw-tool/internal/action/bigquery"
	"github.com/GlobalFishingWatch/gfw-tool/types"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
)

var createTemporalTableViper *viper.Viper

func init() {

	createTemporalTableViper = viper.New()

	Bigquery.AddCommand(createTemporalTableCmd)

	createTemporalTableCmd.Flags().StringP("project-id", "", "", "The destination project id")
	createTemporalTableCmd.MarkFlagRequired("project-id")

	createTemporalTableCmd.Flags().StringP("temp-dataset-id", "", "", "The destination dataset")
	createTemporalTableCmd.MarkFlagRequired("temp-dataset-id")

	createTemporalTableCmd.Flags().StringP("temp-table-name", "", "", "The name of the destination table")
	createTemporalTableCmd.MarkFlagRequired("temp-table-name")

	createTemporalTableCmd.Flags().StringP("temp-table-ttl", "", "", "TTL of the destination table (hours) (optional, default: 12h)")

	createTemporalTableCmd.Flags().StringP("query", "", "", "The query to execute")
	createTemporalTableCmd.MarkFlagRequired("query")

	createTemporalTableViper.BindPFlags(createTemporalTableCmd.Flags())

}

var createTemporalTableCmd = &cobra.Command{
	Use:   "create-temporal-table",
	Short: "Create temporal table",
	Long: `Create temporal table
Format:
	bigquery create-temporal-table-sql --project-id= --sql= 
Example:
	bigquery create-temporal-table-sql \
	  --project-id=world-fishing \
	  --sql="SELECT * FROM vessels;"
`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("→ Executing create temporal table command")

		params := types.BQCreateTemporalTableConfig{
			Query:         viper.GetString("query"),
			ProjectId:     viper.GetString("project-id"),
			TempTableName: viper.GetString("temp-table-name"),
			TempDatasetId: viper.GetString("temp-dataset-id"),
			TTL:           viper.GetInt("temp-table-ttl"),
		}
		log.Println(params)

		bigquery.ExecuteCreateTemporalTable(params)
		log.Println("→ Executing create temporal table finished")
	},
}
