package bigquery

import (
	"context"

	"github.com/GlobalFishingWatch/gfw-tool/internal/common"
	"github.com/GlobalFishingWatch/gfw-tool/types"
)

func ExecuteCreateTable(params types.BQCreateTableConfig) {
	ctx := context.Background()
	table := common.BigQueryGetTable(
		ctx,
		params.ProjectId,
		params.DatasetId,
		params.TableName,
	)

	var clusterFields []string
	common.BigQueryCreateTable(
		ctx,
		table,
		"",
		"",
		clusterFields,
		params.Labels,
	)
}
