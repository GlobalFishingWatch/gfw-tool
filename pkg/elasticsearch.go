package pkg

import (
	"github.com/GlobalFishingWatch/gfw-tool/internal/action/elasticsearch"
	"github.com/GlobalFishingWatch/gfw-tool/types"
)

func ElasticsearchAddAlias(params types.ElasticsearchAddAliasConfig) {
	elasticsearch.AddAlias(params)
}

func ElasticsearchCreateIndex(params types.ElasticsearchCreateIndexConfig) {
	elasticsearch.CreateIndexWithCustomMapping(params)
}

func ElasticsearchDeleteIndex(params types.ElasticsearchDeleteIndexConfig) {
	elasticsearch.DeleteIndex(params)
}

func ElasticsearchDeleteIndexIfExists(params types.ElasticsearchDeleteIndexConfig) {
	elasticsearch.DeleteIndexIfExists(params)
}

func ElasticsearchDeleteIndicesByPrefix(params types.ElasticsearchDeleteIndicesByPrefixConfig) {
	elasticsearch.DeleteIndicesByPrefix(params)
}
