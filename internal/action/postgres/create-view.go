package postgres

import (
	"context"
	"github.com/GlobalFishingWatch/gfw-tool/internal/common"
	"github.com/GlobalFishingWatch/gfw-tool/types"
)

func CreateView(params types.PostgresCreateViewConfig, postgresConfig types.PostgresConfig) {
	ctx := context.Background()
	common.PostgresCreateView(ctx, postgresConfig, params.ViewName, params.TableName)
}
