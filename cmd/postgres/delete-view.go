package postgres

import (
	"github.com/GlobalFishingWatch/gfw-tool/internal/action/postgres"
	"github.com/GlobalFishingWatch/gfw-tool/types"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
)

var deleteViewViper *viper.Viper

func init() {

	deleteViewViper = viper.New()

	Postgres.AddCommand(deleteViewCmd)

	deleteViewCmd.Flags().StringP("postgres-address", "", "", "The address of the database")
	deleteViewCmd.MarkFlagRequired("postgres-address")
	deleteViewCmd.Flags().StringP("postgres-user", "", "", "The destination credentials user")
	deleteViewCmd.MarkFlagRequired("postgres-user")
	deleteViewCmd.Flags().StringP("postgres-password", "", "", "The destination credentials password")
	deleteViewCmd.MarkFlagRequired("postgres-password")
	deleteViewCmd.Flags().StringP("postgres-database", "", "", "The destination database name")
	deleteViewCmd.MarkFlagRequired("postgres-database")

	deleteViewCmd.Flags().StringP("view-name", "", "", "The name of the view to delete")
	deleteViewCmd.MarkFlagRequired("view-name")

	deleteViewViper.BindPFlags(deleteViewCmd.Flags())

}

var deleteViewCmd = &cobra.Command{
	Use:   "delete-view",
	Short: "delete a view",
	Long: `Delete new view
Format:
	gfw-tool postgres delete-view --postgres-address= --postgres-user= --postgres-password= --postgres-database= --view-name= 
Example:
	gfw-tool postgres delete-view \
	  --view-name="vessels"	\
	  --postgres-address="localhost:5432" \
	  --postgres-user="postgres" \
	  --postgres-password="XaD2sd$34Sdas1$ae" \
	  --postgres-database="postgres" 
`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("→ Executing delete view command")

		params := types.PostgresDeleteViewConfig{
			ViewName: viper.GetString("view-name"),
		}

		postgresConfig := types.PostgresConfig{
			Addr:     viper.GetString("postgres-address"),
			User:     viper.GetString("postgres-user"),
			Password: viper.GetString("postgres-password"),
			Database: viper.GetString("postgres-database"),
		}

		postgres.DeleteView(params, postgresConfig)
		log.Println("→ Executing delete view command finished")
	},
}
