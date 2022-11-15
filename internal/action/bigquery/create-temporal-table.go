package bigquery

import (
	"context"
	"github.com/GlobalFishingWatch/gfw-tool/internal/common"
	"github.com/GlobalFishingWatch/gfw-tool/types"
)

func ExecuteCreateTemporalTable(params types.BQCreateTemporalTableConfig) {
	ctx := context.Background()
	common.CreateTemporalTableFromQuery(
		ctx,
		params.ProjectId,
		params.TempDatasetId,
		params.TempTableName,
		params.Query,
		params.TTL,
		"",
	)
}
