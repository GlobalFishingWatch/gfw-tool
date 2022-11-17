package postgres

import (
	"github.com/GlobalFishingWatch/gfw-tool/internal/action/postgres"
	"github.com/GlobalFishingWatch/gfw-tool/types"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
)

var executeRawSqlViper *viper.Viper

func init() {

	executeRawSqlViper = viper.New()

	Postgres.AddCommand(executeRawSqlCmd)

	executeRawSqlCmd.Flags().StringP("postgres-address", "", "", "The address of the database")
	executeRawSqlCmd.MarkFlagRequired("postgres-address")
	executeRawSqlCmd.Flags().StringP("postgres-user", "", "", "The destination credentials user")
	executeRawSqlCmd.MarkFlagRequired("postgres-user")
	executeRawSqlCmd.Flags().StringP("postgres-password", "", "", "The destination credentials password")
	executeRawSqlCmd.MarkFlagRequired("postgres-password")
	executeRawSqlCmd.Flags().StringP("postgres-database", "", "", "The destination database name")
	executeRawSqlCmd.MarkFlagRequired("postgres-database")

	executeRawSqlCmd.Flags().StringP("sql", "", "", "The sql statements to execute")
	executeRawSqlCmd.MarkFlagRequired("sql")

	executeRawSqlViper.BindPFlags(executeRawSqlCmd.Flags())
}

var executeRawSqlCmd = &cobra.Command{
	Use:   "execute-raw-sql",
	Short: "Execute raw sql",
	Long: `Execute raw sql
Format:
	gfw-tool postgres execute-raw-sql --postgres-address= --postgres-user= --postgres-password= --postgres-database= --table-name= 
Example:
	gfw-tool postgres execute-raw-sql \
	  --sql="CREATE INDEX events_table_event_id ON public.events_table (event_id);"	\
	  --postgres-address="localhost:5432" \
	  --postgres-user="postgres" \
	  --postgres-password="XaD2sd$34Sdas1$ae" \
	  --postgres-database="postgres" 
`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("→ Executing raw sql command")

		params := types.PostgresExecuteRawSqlConfig{
			Sql: viper.GetString("sql"),
		}
		log.Println(params)

		postgresConfig := types.PostgresConfig{
			Addr:     viper.GetString("postgres-address"),
			User:     viper.GetString("postgres-user"),
			Password: viper.GetString("postgres-password"),
			Database: viper.GetString("postgres-database"),
		}

		postgres.ExecuteRawSql(params, postgresConfig)
		log.Println("→ Executing raw sql finished")
	},
}
