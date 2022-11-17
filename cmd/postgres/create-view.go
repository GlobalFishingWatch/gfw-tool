package postgres

import (
	"github.com/GlobalFishingWatch/gfw-tool/internal/action/postgres"
	"github.com/GlobalFishingWatch/gfw-tool/types"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
)

var createViewViper *viper.Viper

func init() {

	createIndexViper = viper.New()

	Postgres.AddCommand(createViewCmd)

	createViewCmd.Flags().StringP("postgres-address", "", "", "The address of the database")
	createViewCmd.MarkFlagRequired("postgres-address")
	createViewCmd.Flags().StringP("postgres-user", "", "", "The destination credentials user")
	createViewCmd.MarkFlagRequired("postgres-user")
	createViewCmd.Flags().StringP("postgres-password", "", "", "The destination credentials password")
	createViewCmd.MarkFlagRequired("postgres-password")
	createViewCmd.Flags().StringP("postgres-database", "", "", "The destination database name")
	createViewCmd.MarkFlagRequired("postgres-database")

	createViewCmd.Flags().StringP("view-name", "", "", "The name of the view to associate the table")
	createViewCmd.MarkFlagRequired("view-name")
	createViewCmd.Flags().StringP("table-name", "", "", "The name of the table to associate the view")
	createViewCmd.MarkFlagRequired("table-name")

	createViewViper.BindPFlags(createViewCmd.Flags())

}

var createViewCmd = &cobra.Command{
	Use:   "create-view",
	Short: "Create a new view",
	Long: `Create new view
Format:
	gfw-tool postgres create-view --postgres-address= --postgres-user= --postgres-password= --postgres-database= --view-name= --table-name=
Example:
	gfw-tool postgres create-view \
	  --view-name="vessels"	\
	  --table-name="vessels_2021_02_01" \
	  --postgres-address="localhost:5432" \
	  --postgres-user="postgres" \
	  --postgres-password="XaD2sd$34Sdas1$ae" \
	  --postgres-database="postgres" 
`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("→ Executing Create view command")

		params := types.PostgresCreateViewConfig{
			TableName: viper.GetString("table-name"),
			ViewName:  viper.GetString("view-name"),
		}

		postgresConfig := types.PostgresConfig{
			Addr:     viper.GetString("postgres-address"),
			User:     viper.GetString("postgres-user"),
			Password: viper.GetString("postgres-password"),
			Database: viper.GetString("postgres-database"),
		}

		postgres.CreateView(params, postgresConfig)
		log.Println("→ Executing Create view command finished")
	},
}
