package pkg

import (
	"github.com/GlobalFishingWatch/gfw-tool/internal/action/bq2psql"
	"github.com/GlobalFishingWatch/gfw-tool/types"
)

func BQ2PSQLExportBigQueryToPostgres(params types.BQ2PSQLExportConfig, postgresConfig types.PostgresConfig) {
	bq2psql.ExportBigQueryToPostgres(params, postgresConfig)
}

func BQ2PSQLExportCsvBigQueryToPostgres(params types.BQ2PSQLExportCSVConfig, postgresConfig types.CloudSqlConfig) {
	bq2psql.ExportCsvBigQueryToPostgres(params, postgresConfig)
}
