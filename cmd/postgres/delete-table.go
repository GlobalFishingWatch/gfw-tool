package postgres

import (
	"github.com/GlobalFishingWatch/gfw-tool/internal/action/postgres"
	"github.com/GlobalFishingWatch/gfw-tool/types"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
)

var deleteTableViper *viper.Viper

func init() {

	deleteTableViper = viper.New()

	Postgres.AddCommand(deleteTableCmd)

	deleteTableCmd.Flags().StringP("postgres-address", "", "", "The address of the database")
	deleteTableCmd.MarkFlagRequired("postgres-address")
	deleteTableCmd.Flags().StringP("postgres-user", "", "", "The destination credentials user")
	deleteTableCmd.MarkFlagRequired("postgres-user")
	deleteTableCmd.Flags().StringP("postgres-password", "", "", "The destination credentials password")
	deleteTableCmd.MarkFlagRequired("postgres-password")
	deleteTableCmd.Flags().StringP("postgres-database", "", "", "The destination database name")
	deleteTableCmd.MarkFlagRequired("postgres-database")

	deleteTableCmd.Flags().StringP("table-name", "", "", "The name of the table to delete")
	deleteTableCmd.MarkFlagRequired("table-name")

	deleteTableViper.BindPFlags(deleteTableCmd.Flags())

}

var deleteTableCmd = &cobra.Command{
	Use:   "delete-table",
	Short: "delete a table",
	Long: `Delete new table
Format:
	gfw-tool postgres delete-table --postgres-address= --postgres-user= --postgres-password= --postgres-database= --table-name= 
Example:
	gfw-tool postgres delete-table \
	  --table-name="vessels"	\
	  --postgres-address="localhost:5432" \
	  --postgres-user="postgres" \
	  --postgres-password="XaD2sd$34Sdas1$ae" \
	  --postgres-database="postgres" 
`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("→ Executing delete table command")

		params := types.PostgresDeleteTableConfig{
			TableName: deleteTableViper.GetString("table-name"),
		}

		postgresConfig := types.PostgresConfig{
			Addr:     deleteTableViper.GetString("postgres-address"),
			User:     deleteTableViper.GetString("postgres-user"),
			Password: deleteTableViper.GetString("postgres-password"),
			Database: deleteTableViper.GetString("postgres-database"),
		}

		postgres.DeleteTable(params, postgresConfig)
		log.Println("→ Executing delete table command finished")
	},
}
