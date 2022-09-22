package pkg

import (
	"github.com/GlobalFishingWatch/gfw-tool/internal/action/bq2gcs"
	"github.com/GlobalFishingWatch/gfw-tool/types"
)

func ExportDataFromBQQueryToGCS(params types.BQExportDataToGCSConfig) {
	bq2gcs.ExportDataFromBigQueryQueryToGCS(params)
}
