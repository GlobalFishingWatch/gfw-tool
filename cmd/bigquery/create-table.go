package bigquery

import (
	"log"

	"github.com/GlobalFishingWatch/gfw-tool/internal/action/bigquery"
	"github.com/GlobalFishingWatch/gfw-tool/types"
	"github.com/GlobalFishingWatch/gfw-tool/utils"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var createTableViper *viper.Viper

func init() {

	createTableViper = viper.New()

	Bigquery.AddCommand(createTableCmd)

	createTableCmd.Flags().StringP("project-id", "", "", "The destination project id")
	createTableCmd.MarkFlagRequired("project-id")

	createTableCmd.Flags().StringP("dataset-id", "", "", "The destination dataset")
	createTableCmd.MarkFlagRequired("dataset-id")

	createTableCmd.Flags().StringP("table-name", "", "", "The name of the destination table")
	createTableCmd.MarkFlagRequired("table-name")

	createTableCmd.Flags().StringP("query", "", "", "The query to execute")
	createTableCmd.MarkFlagRequired("query")

	createTableCmd.Flags().StringSlice("labels", []string{}, "Labels to apply to BQ separated by comma. Example: project=api,environment=production")

	createTableViper.BindPFlags(createTableCmd.Flags())

}

var createTableCmd = &cobra.Command{
	Use:   "create-table",
	Short: "Create table",
	Long: `Create table
Format:
	gfw-tool bigquery create-table --project-id= --sql= 
Example:
	gfw-tool bigquery create-table \
	  --project-id=world-fishing \
	  --sql="SELECT * FROM vessels;"
`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("→ Executing create table command")

		params := types.BQCreateTableConfig{
			Query:     createTableViper.GetString("query"),
			ProjectId: createTableViper.GetString("project-id"),
			TableName: createTableViper.GetString("table-name"),
			DatasetId: createTableViper.GetString("dataset-id"),
			Labels:    utils.ConvertSliceToMap(createTableViper.GetStringSlice("labels")),
		}
		log.Println(params)

		bigquery.ExecuteCreateTable(params)
		log.Println("→ Executing create table finished")
	},
}
