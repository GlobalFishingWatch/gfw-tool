package postgres

import (
	"context"
	"github.com/GlobalFishingWatch/gfw-tool/internal/common"
	"github.com/GlobalFishingWatch/gfw-tool/types"
)

func DeleteView(params types.PostgresDeleteViewConfig, postgresConfig types.PostgresConfig) {
	ctx := context.Background()
	common.PostgresDeleteView(ctx, postgresConfig, params.ViewName)
}
