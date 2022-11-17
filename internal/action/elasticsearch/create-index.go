package elasticsearch

import (
	"github.com/GlobalFishingWatch/gfw-tool/internal/common"
	"github.com/GlobalFishingWatch/gfw-tool/types"
	"github.com/GlobalFishingWatch/gfw-tool/utils"
	"log"
)

func CreateIndexWithCustomMapping(params types.ElasticsearchCreateIndexConfig) {

	utils.ValidateUrl(params.ElasticSearchUrl)

	common.ElasticSearchCreateIndex(
		params.ElasticSearchUrl,
		params.IndexName,
	)

	if params.Settings != "" {
		settingsRes := common.ElasticSearchPutSettingsToIndex(
			params.ElasticSearchUrl,
			params.IndexName,
			params.Settings,
		)
		log.Printf("→ Set Settings response: %v", settingsRes)
	}

	mappingRes := common.ElasticSearchPutMappingToIndex(
		params.ElasticSearchUrl,
		params.IndexName,
		params.Mapping,
	)
	log.Printf("→ Set Mapping response: %v", mappingRes)

}
