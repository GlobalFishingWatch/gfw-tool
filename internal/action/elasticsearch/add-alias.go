package elasticsearch

import (
	"github.com/GlobalFishingWatch/gfw-tool/internal/common"
	"github.com/GlobalFishingWatch/gfw-tool/types"
	"github.com/GlobalFishingWatch/gfw-tool/utils"
)

func AddAlias(params types.ElasticsearchAddAliasConfig) {
	utils.ValidateUrl(params.ElasticSearchUrl)
	common.AddAlias(params.ElasticSearchUrl, params.IndexName, params.Alias)
}
