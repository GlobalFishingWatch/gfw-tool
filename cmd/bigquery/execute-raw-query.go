package bigquery

import (
	"log"

	action "github.com/GlobalFishingWatch/gfw-tool/internal/action/bigquery"
	"github.com/GlobalFishingWatch/gfw-tool/types"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var executeRawQueryViper *viper.Viper

func init() {

	executeRawQueryViper = viper.New()

	Bigquery.AddCommand(executeRawQueryCmd)

	executeRawQueryCmd.Flags().StringP("project-id", "", "", "The destination project id")
	executeRawQueryCmd.MarkFlagRequired("project-id")

	executeRawQueryCmd.Flags().StringP("query", "", "", "The query to execute")
	executeRawQueryCmd.MarkFlagRequired("query")

	executeRawQueryCmd.Flags().StringP("destination-dataset", "", "", "The destination dataset")

	executeRawQueryCmd.Flags().StringP("destination-table", "", "", "The destination table")

	executeRawQueryCmd.Flags().StringP("write-disposition", "", "WRITE_APPEND", "Specifies how existing data in the destination table is treated. Possible value (WRITE_EMPTY, WRITE_TRUNCATE, WRITE_APPEND)")

	executeRawQueryCmd.Flags().String("schema", "", "Specifies schema of the result table")

	executeRawQueryCmd.Flags().String("partition-field", "", "Partition field")

	executeRawQueryCmd.Flags().String("partition-type", "", "Partition type (DAY, WEEK, MONTH, YEAR)")

	executeRawQueryViper.BindPFlags(executeRawQueryCmd.Flags())
}

var executeRawQueryCmd = &cobra.Command{
	Use:   "execute-raw-query",
	Short: "Execute raw sql",
	Long: `Execute raw sql
Format:
	bigquery execute-raw-query --project-id= --sql= 
Example:
	bigquery execute-raw-query \
	  --project-id=world-fishing \
	  --sql="SELECT * FROM vessels;"
`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("→ Executing raw query command")

		params := types.BQRawQueryConfig{
			Query:              executeRawQueryViper.GetString("query"),
			ProjectId:          executeRawQueryViper.GetString("project-id"),
			DestinationTable:   executeRawQueryViper.GetString("destination-table"),
			DestinationDataset: executeRawQueryViper.GetString("destination-dataset"),
			WriteDisposition:   executeRawQueryViper.GetString("write-disposition"),
			PartitionTimeField: executeRawQueryViper.GetString("partition-field"),
			TimePartitioning:   executeRawQueryViper.GetString("partition-type"),
			Schema:             executeRawQueryViper.GetString("schema"),
		}
		log.Println(params)
		if params.PartitionTimeField != "" && params.Schema == "" {
			log.Fatal("Schema is required for partition feature")
		}
		action.ExecuteRawQuery(params)
		log.Println("→ Executing raw query finished")
	},
}
