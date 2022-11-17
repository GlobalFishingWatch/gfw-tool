package elasticsearch

import (
	"github.com/GlobalFishingWatch/gfw-tool/internal/common"
	"github.com/GlobalFishingWatch/gfw-tool/types"
	"github.com/GlobalFishingWatch/gfw-tool/utils"
)

func DeleteIndex(params types.ElasticsearchDeleteIndexConfig) {
	utils.ValidateUrl(params.ElasticSearchUrl)
	common.DeleteIndex(
		params.ElasticSearchUrl,
		params.IndexName,
		false,
	)
}
