package postgres

import (
	"github.com/GlobalFishingWatch/gfw-tool/internal/action/postgres"
	"github.com/GlobalFishingWatch/gfw-tool/types"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
)

var createIndexViper *viper.Viper

func init() {

	createIndexViper = viper.New()

	Postgres.AddCommand(createIndexCmd)

	createIndexCmd.Flags().StringP("postgres-address", "", "", "The address of the database")
	createIndexCmd.MarkFlagRequired("postgres-address")
	createIndexCmd.Flags().StringP("postgres-user", "", "", "The destination credentials user")
	createIndexCmd.MarkFlagRequired("postgres-user")
	createIndexCmd.Flags().StringP("postgres-password", "", "", "The destination credentials password")
	createIndexCmd.MarkFlagRequired("postgres-password")
	createIndexCmd.Flags().StringP("postgres-database", "", "", "The destination database name")
	createIndexCmd.MarkFlagRequired("postgres-database")

	createIndexCmd.Flags().StringP("table-name", "", "", "The name of the table to add a index")
	createIndexCmd.MarkFlagRequired("table-name")
	createIndexCmd.Flags().StringP("index-name", "", "", "The name of the new index")
	createIndexCmd.MarkFlagRequired("index-name")
	createIndexCmd.Flags().StringP("column", "", "", "The name of the column")
	createIndexCmd.MarkFlagRequired("column")

	createIndexViper.BindPFlags(createIndexCmd.Flags())
}

var createIndexCmd = &cobra.Command{
	Use:   "create-index",
	Short: "Add a index",
	Long: `Add a index
Format:
	gfw-tool postgres create-index --postgres-address= --postgres-user= --postgres-password= --postgres-database= --table-name= 
Example:
	gfw-tool postgres create-index \
	  --view-name="vessels"	\
	  --postgres-address="localhost:5432" \
	  --postgres-user="postgres" \
	  --postgres-password="XaD2sd$34Sdas1$ae" \
	  --postgres-database="postgres" 
`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("→ Executing delete view command")

		params := types.PostgresCreateIndexConfig{
			TableName: viper.GetString("table-name"),
			IndexName: viper.GetString("index-name"),
			Column:    viper.GetString("column"),
		}
		log.Println(params)
		postgresConfig := types.PostgresConfig{
			Addr:     viper.GetString("postgres-address"),
			User:     viper.GetString("postgres-user"),
			Password: viper.GetString("postgres-password"),
			Database: viper.GetString("postgres-database"),
		}

		postgres.CreateIndex(params, postgresConfig)
		log.Println("→ Executing delete view command finished")
	},
}
