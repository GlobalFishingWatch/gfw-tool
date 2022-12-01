package elasticsearch

import (
	"github.com/GlobalFishingWatch/gfw-tool/internal/common"
	"github.com/GlobalFishingWatch/gfw-tool/types"
	"github.com/GlobalFishingWatch/gfw-tool/utils"
	"log"
)

func DeleteIndicesByPrefix(params types.ElasticsearchDeleteIndicesByPrefixConfig) {
	utils.ValidateUrl(params.ElasticSearchUrl)
	deleteIndicesByPrefix(params.Prefix, params.Prefix, params.NoDeleteIndex)
}

func deleteIndicesByPrefix(elasticsearchUrl string, prefix string, noDeleteIndex string) {
	log.Printf("→ ES →→ Listing indices by prefix %s", prefix)
	indices := common.ElasticSearchGetIndicesFilteringByPrefix(elasticsearchUrl, prefix)
	for _, index := range indices {
		if index.Index != noDeleteIndex {
			common.ElasticSearchDeleteIndex(elasticsearchUrl, index.Index, false)
		} else {
			log.Printf("→ ES →→ %s index not delete because it is in no-delete-index param", index.Index)
		}
	}
}
