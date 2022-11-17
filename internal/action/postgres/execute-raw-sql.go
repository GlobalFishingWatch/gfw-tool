package postgres

import (
	"context"
	"github.com/GlobalFishingWatch/gfw-tool/internal/common"
	"github.com/GlobalFishingWatch/gfw-tool/types"
)

func ExecuteRawSql(params types.PostgresExecuteRawSqlConfig, postgresConfig types.PostgresConfig) {
	ctx := context.Background()
	retries := 0
	common.PostgresExecuteSQLCommand(ctx, postgresConfig, params.Sql, retries)
}
