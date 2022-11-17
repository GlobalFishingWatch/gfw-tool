package postgres

import (
	"context"
	"github.com/GlobalFishingWatch/gfw-tool/internal/common"
	"github.com/GlobalFishingWatch/gfw-tool/types"
)

func DeleteTable(params types.PostgresDeleteTableConfig, postgresConfig types.PostgresConfig) {
	ctx := context.Background()
	common.PostgresDeleteTable(ctx, postgresConfig, params.TableName)
}
