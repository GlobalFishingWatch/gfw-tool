package pkg

import (
	"github.com/GlobalFishingWatch/gfw-tool/internal/action/gcs2bq"
	"github.com/GlobalFishingWatch/gfw-tool/types"
)

func GCS2BQExportDataFromGCStoBigQuery(params types.GCSExportDataToBigQueryConfig) {
	gcs2bq.Export(params)
}
