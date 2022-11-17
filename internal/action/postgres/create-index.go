package postgres

import (
	"context"
	"github.com/GlobalFishingWatch/gfw-tool/internal/common"
	"github.com/GlobalFishingWatch/gfw-tool/types"
)

func CreateIndex(params types.PostgresCreateIndexConfig, postgresConfig types.PostgresConfig) {
	ctx := context.Background()
	common.PostgresCreateIndex(ctx, postgresConfig, params.TableName, params.IndexName, params.Column)
}
