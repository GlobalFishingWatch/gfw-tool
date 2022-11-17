package pkg

import (
	"github.com/GlobalFishingWatch/gfw-tool/internal/action/bq2es"
	"github.com/GlobalFishingWatch/gfw-tool/types"
)

func BQ2ESExportBigQueryToElasticSearch(params types.BQ2ESImportConfig) {
	bq2es.ExportBigQueryToElasticSearch(params)
}
