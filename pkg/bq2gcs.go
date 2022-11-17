package pkg

import (
	"github.com/GlobalFishingWatch/gfw-tool/internal/action/bq2gcs"
	"github.com/GlobalFishingWatch/gfw-tool/types"
)

func BQ2GCSExportDataFromBQQueryToGCS(params types.BQ2GCSExportDataToGCSConfig) {
	bq2gcs.ExportDataFromBigQueryQueryToGCS(params)
}
