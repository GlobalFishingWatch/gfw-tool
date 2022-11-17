package pkg

import (
	"github.com/GlobalFishingWatch/gfw-tool/internal/action/postgres"
	"github.com/GlobalFishingWatch/gfw-tool/types"
)

func PostgresExecuteRawSql(params types.PostgresExecuteRawSqlConfig, postgresConfig types.PostgresConfig) {
	postgres.ExecuteRawSql(params, postgresConfig)
}

func PostgresDeleteView(params types.PostgresDeleteViewConfig, postgresConfig types.PostgresConfig) {
	postgres.DeleteView(params, postgresConfig)
}

func PostgresDeleteTable(params types.PostgresDeleteTableConfig, postgresConfig types.PostgresConfig) {
	postgres.DeleteTable(params, postgresConfig)
}

func PostgresCreateView(params types.PostgresCreateViewConfig, postgresConfig types.PostgresConfig) {
	postgres.CreateView(params, postgresConfig)
}

func PostgresCreateIndex(params types.PostgresCreateIndexConfig, postgresConfig types.PostgresConfig) {
	postgres.CreateIndex(params, postgresConfig)
}
