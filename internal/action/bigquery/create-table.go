package bigquery

import (
	"context"
	"github.com/GlobalFishingWatch/gfw-tool/internal/common"
	"github.com/GlobalFishingWatch/gfw-tool/types"
)

func ExecuteCreateTable(params types.BQCreateTableConfig) {
	ctx := context.Background()
	table := common.GetTable(
		ctx,
		params.ProjectId,
		params.DatasetId,
		params.TableName,
	)

	var clusterFields []string
	common.CreateTable(
		ctx,
		table,
		"",
		"",
		clusterFields,
	)
}

