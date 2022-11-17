package pkg

import (
	"github.com/GlobalFishingWatch/gfw-tool/internal/action/bigquery"
	"github.com/GlobalFishingWatch/gfw-tool/types"
)

func BigQueryCreateTable(params types.BQCreateTableConfig) {
	bigquery.ExecuteCreateTable(params)
}

func BigQueryCreateTemporalTable(params types.BQCreateTemporalTableConfig) {
	bigquery.ExecuteCreateTemporalTable(params)
}

func BigQueryExecuteRawQuery(params types.BQRawQueryConfig) []map[string]interface{} {
	return bigquery.ExecuteRawQuery(params)
}
