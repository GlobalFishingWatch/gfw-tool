package elasticsearch

import (
	"github.com/GlobalFishingWatch/gfw-tool/internal/common"
	"github.com/GlobalFishingWatch/gfw-tool/types"
	"github.com/GlobalFishingWatch/gfw-tool/utils"
	"log"
)

func DeleteIndicesByPrefix(params types.ElasticsearchDeleteIndicesByPrefixConfig) {
	utils.ValidateUrl(params.ElasticSearchUrl)
	deleteIndicesByPrefix(params.Prefix, params.NoDeleteIndex, params.NoDeleteIndex)
}

func deleteIndicesByPrefix(elasticsearchUrl string, prefix string, noDeleteIndex string) {
	log.Printf("→ ES →→ Listing indices by prefix %s", prefix)
	indices := common.GetIndicesFilteringByPrefix(elasticsearchUrl, prefix)
	for _, index := range indices {
		if index.Index != noDeleteIndex {
			common.DeleteIndex(elasticsearchUrl, index.Index, false)
		} else {
			log.Printf("→ ES →→ %s index not delete because it is in no-delete-index param", index.Index)
		}
	}
}
