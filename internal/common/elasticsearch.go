package common

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/GlobalFishingWatch/gfw-tool/types"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"log"
	"strings"
	"time"
)

func ElasticSearchCreateClient(url string) *elasticsearch.Client {
	log.Println("→ BQ →→ Creating Big Query Client")

	cfg := elasticsearch.Config{
		Addresses: []string{url},
	}
	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("→ ES →→ Error creating the client: %s", err)
	}
	return client
}

func ElasticSearchCheckIfIndexExists(elasticsearchUrl string, indexName string) bool {
	client := ElasticSearchCreateClient(elasticsearchUrl)

	res, err := client.Indices.Exists([]string{indexName})
	if err != nil {
		log.Fatalf("→ ES →→ Cannot check if index exists: %s", err)
	}
	log.Println("→ ES →→ Checking if index exists on ElasticSearch: ", res.StatusCode == 200)
	return res.StatusCode == 200
}

func ElasticSearchCreateIndex(elasticsearchUrl string, indexName string) {
	client := ElasticSearchCreateClient(elasticsearchUrl)
	log.Printf("→ ES →→ Creating index with name %v\n", indexName)
	res, err := client.Indices.Create(indexName)
	if err != nil {
		log.Fatalf("→ ES →→ Cannot create index: %s", err)
	}
	if res.IsError() {
		log.Fatalf("→ ES →→ Cannot create index: %s", res)
	}
}

func ElasticSearchDeleteIndex(elasticsearchUrl string, indexName string, ifExists bool) {
	client := ElasticSearchCreateClient(elasticsearchUrl)
	log.Printf("→ ES →→ Deleting index with name %v\n", indexName)

	res, err := client.Indices.Delete([]string{indexName})

	if err != nil {
		log.Fatalf("→ ES →→ Cannot delete index: %s", err)
	}
	if res.IsError() {
		if ifExists == true {
			log.Printf("→ ES →→ Cannot delete index: %s", res)
		} else {
			log.Fatalf("→ ES →→ Cannot delete index: %s", res)
		}
	}
}

func ElasticSearchPutSettingsToIndex(elasticsearchUrl string, indexName string, settings string) *esapi.Response {
	client := ElasticSearchCreateClient(elasticsearchUrl)

	log.Printf("→ ES →→ Putting settings to index with name %v\n and mapping %s\n", indexName, settings)

	settingsReader := strings.NewReader(settings)

	res, err := client.Indices.Close([]string{indexName})
	if res.IsError() || err != nil {
		log.Fatalf("→ ES →→ Cannot close index: %s", err)
	}

	res, err = client.Indices.PutSettings(settingsReader, func(index *esapi.IndicesPutSettingsRequest) {
		index.Index = []string{indexName}
	})
	if res.IsError() || err != nil {
		log.Fatalf("→ ES →→ Cannot put mapping: %s", err)
	}

	res, err = client.Indices.Open([]string{indexName})
	if res.IsError() || err != nil {
		log.Fatalf("→ ES →→ Cannot open index: %s", err)
	}

	return res
}

func ElasticSearchPutMappingToIndex(elasticsearchUrl string, indexName string, mapping string) *esapi.Response {
	client := ElasticSearchCreateClient(elasticsearchUrl)

	log.Printf("→ ES →→ Putting mapping to index with name %v\n and mapping %s\n", indexName, mapping)

	mappingReader := strings.NewReader(mapping)

	res, err := client.Indices.PutMapping([]string{indexName}, mappingReader, func(index *esapi.IndicesPutMappingRequest) {
		index.Index = []string{indexName}
	})
	if err != nil {
		log.Fatalf("→ ES →→ Cannot put mapping: %s", err)
	}
	if res.IsError() {
		log.Fatalf("→ ES →→ Cannot delete index: %s", res)
	}
	return res
}

func ElasticSearchAddAlias(elasticsearchUrl string, indexName string, alias string) {
	client := ElasticSearchCreateClient(elasticsearchUrl)
	indices := []string{indexName}
	res, err := client.Indices.PutAlias(indices, alias)
	if err != nil {
		log.Fatalf("→ ES →→ Error creating new alias: %s", err)
	}
	log.Printf("→ ES →→ Create Alias response: %v", res)
}

func ElasticSearchExecuteBulk(
	elasticsearchUrl string,
	indexName string,
	buf *bytes.Buffer,
	currentBatch int,
	onErrorAction string,
) *esapi.Response {
	client := ElasticSearchCreateClient(elasticsearchUrl)
	res, err := client.Bulk(bytes.NewReader(buf.Bytes()), client.Bulk.WithIndex(indexName))
	if err != nil {
		log.Printf("Error importing Bulk")
		ElasticSearchExecuteOnErrorAction(elasticsearchUrl, indexName, onErrorAction, "")
		log.Fatalf("Failure indexing Batch %d: %s", currentBatch, err)
	}

	return res
}

func ElasticSearchParseEsAPIResponse(res *esapi.Response) map[string]interface{} {
	responseBody := make(map[string]interface{})
	json.NewDecoder(res.Body).Decode(&responseBody)
	return responseBody
}

func ElasticSearchReindex(elasticsearchUrl string, sourceIndexName string, destinationIndexName string) {
	client := ElasticSearchCreateClient(elasticsearchUrl)
	existsDestinationIndex := ElasticSearchCheckIfIndexExists(elasticsearchUrl, destinationIndexName)
	if existsDestinationIndex == true {
		ElasticSearchDeleteIndex(elasticsearchUrl, destinationIndexName, false)
	}

	log.Printf("→ ES →→ Reindexing from %s to %s\n", sourceIndexName, destinationIndexName)
	reindexBody := map[string]map[string]string{
		"source": {"index": sourceIndexName},
		"dest":   {"index": destinationIndexName},
	}
	body, err := json.Marshal(reindexBody)
	if err != nil {
		log.Fatalf("→ ES →→ Error creating body to reindex %s", err)
	}

	res, err := client.Reindex(bytes.NewReader(body), func(request *esapi.ReindexRequest) {
		waitForCompletion := false
		request.WaitForCompletion = &waitForCompletion
	})
	if err != nil {
		log.Fatalf("→ ES →→ Error requesting reindex %s", err)
	}
	if res.IsError() {
		log.Fatalf("→ ES →→ Cannot reindex: %s", res)
	}

	responseBody := ElasticSearchParseEsAPIResponse(res)
	taskId := responseBody["task"].(string)
	log.Printf("→ ES →→ Reindex process started async. Task id: %s \n", taskId)

	for {
		res, err := client.Tasks.Get(taskId)
		if err != nil {
			log.Fatalf("→ ES →→ Error requesting reindex %s", err)
		}
		if res.IsError() {
			log.Fatalf("→ ES →→ Cannot reindex: %s", res)
		}
		responseBody = ElasticSearchParseEsAPIResponse(res)
		taskStatus := responseBody["completed"].(bool)
		if taskStatus == true {
			break
		}
		time.Sleep(5000 * time.Millisecond)
	}
	log.Println("→ ES →→ Reindex process completed")
	ElasticSearchDeleteIndex(elasticsearchUrl, sourceIndexName, false)
}

func ElasticSearchExecuteOnErrorAction(
	elasticsearchUrl string,
	indexName string,
	action string,
	temporalIndexName string,
) {
	if action == "delete" {
		ElasticSearchDeleteIndex(elasticsearchUrl, indexName, false)
	}
	if action == "reindex" {
		if temporalIndexName == "" {
			log.Fatalf("Temporal index name is required")
		}
		ElasticSearchDeleteIndex(elasticsearchUrl, indexName, false)
		ElasticSearchReindex(elasticsearchUrl, temporalIndexName, indexName)
		ElasticSearchDeleteIndex(elasticsearchUrl, temporalIndexName, false)
	}
}

func ElasticSearchRecreateIndex(elasticsearchUrl string, indexName string) {
	log.Printf("→ ES →→ Recreating index with name %v\n", indexName)
	ElasticSearchDeleteIndex(elasticsearchUrl, indexName, false)
	ElasticSearchCreateIndex(elasticsearchUrl, indexName)
}

func ElasticSearchGetIndicesFilteringByPrefix(elasticsearchUrl string, prefix string) []types.Index {
	client := ElasticSearchCreateClient(elasticsearchUrl)

	res, err := client.Cat.Indices(
		client.Cat.Indices.WithIndex(fmt.Sprintf("%s*", prefix)),
		client.Cat.Indices.WithFormat("json"),
	)

	if err != nil {
		log.Fatalf("→ ES →→ Cannot list indices with prefix: %s", err)
	}
	if res.IsError() {
		log.Fatalf("→ ES →→ Cannot list indices with prefix: %s", res)
	}
	defer res.Body.Close()
	var indices []types.Index
	err = json.NewDecoder(res.Body).Decode(&indices)
	if err != nil {
		log.Fatalf("→ ES →→ Cannot list indices with prefix: %s", err)
	}

	return indices
}
