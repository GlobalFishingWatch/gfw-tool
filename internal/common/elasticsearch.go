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

func CreateElasticSearchClient(url string) *elasticsearch.Client {
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

func CheckIfIndexExists(elasticsearchUrl string, indexName string) bool {
	client := CreateElasticSearchClient(elasticsearchUrl)

	res, err := client.Indices.Exists([]string{indexName})
	if err != nil {
		log.Fatalf("→ ES →→ Cannot check if index exists: %s", err)
	}
	log.Println("→ ES →→ Checking if index exists on ElasticSearch: ", res.StatusCode == 200)
	return res.StatusCode == 200
}

func CreateIndex(elasticsearchUrl string, indexName string) {
	client := CreateElasticSearchClient(elasticsearchUrl)
	log.Printf("→ ES →→ Creating index with name %v\n", indexName)
	res, err := client.Indices.Create(indexName)
	if err != nil {
		log.Fatalf("→ ES →→ Cannot create index: %s", err)
	}
	if res.IsError() {
		log.Fatalf("→ ES →→ Cannot create index: %s", res)
	}
}

func DeleteIndex(elasticsearchUrl string, indexName string, ifExists bool) {
	client := CreateElasticSearchClient(elasticsearchUrl)
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

func PutSettingsToIndex(elasticsearchUrl string, indexName string, settings string) *esapi.Response {
	client := CreateElasticSearchClient(elasticsearchUrl)

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

func PutMappingToIndex(elasticsearchUrl string, indexName string, mapping string) *esapi.Response {
	client := CreateElasticSearchClient(elasticsearchUrl)

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

func AddAlias(elasticsearchUrl string, indexName string, alias string) {
	client := CreateElasticSearchClient(elasticsearchUrl)
	indices := []string{indexName}
	res, err := client.Indices.PutAlias(indices, alias)
	if err != nil {
		log.Fatalf("→ ES →→ Error creating new alias: %s", err)
	}
	log.Printf("→ ES →→ Create Alias response: %v", res)
}

func ExecuteBulk(
	elasticsearchUrl string,
	indexName string,
	buf *bytes.Buffer,
	currentBatch int,
	onErrorAction string,
) *esapi.Response {
	client := CreateElasticSearchClient(elasticsearchUrl)
	res, err := client.Bulk(bytes.NewReader(buf.Bytes()), client.Bulk.WithIndex(indexName))
	if err != nil {
		log.Printf("Error importing Bulk")
		ExecuteOnErrorAction(elasticsearchUrl, indexName, onErrorAction, "")
		log.Fatalf("Failure indexing Batch %d: %s", currentBatch, err)
	}

	return res
}

func ParseEsAPIResponse(res *esapi.Response) map[string]interface{} {
	responseBody := make(map[string]interface{})
	json.NewDecoder(res.Body).Decode(&responseBody)
	return responseBody
}

func Reindex(elasticsearchUrl string, sourceIndexName string, destinationIndexName string) {
	client := CreateElasticSearchClient(elasticsearchUrl)
	existsDestinationIndex := CheckIfIndexExists(elasticsearchUrl, destinationIndexName)
	if existsDestinationIndex == true {
		DeleteIndex(elasticsearchUrl, destinationIndexName, false)
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

	responseBody := ParseEsAPIResponse(res)
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
		responseBody = ParseEsAPIResponse(res)
		taskStatus := responseBody["completed"].(bool)
		if taskStatus == true {
			break
		}
		time.Sleep(5000 * time.Millisecond)
	}
	log.Println("→ ES →→ Reindex process completed")
	DeleteIndex(elasticsearchUrl, sourceIndexName, false)
}

func ExecuteOnErrorAction(
	elasticsearchUrl string,
	indexName string,
	action string,
	temporalIndexName string,
) {
	if action == "delete" {
		DeleteIndex(elasticsearchUrl, indexName, false)
	}
	if action == "reindex" {
		if temporalIndexName == "" {
			log.Fatalf("Temporal index name is required")
		}
		DeleteIndex(elasticsearchUrl, indexName, false)
		Reindex(elasticsearchUrl, temporalIndexName, indexName)
		DeleteIndex(elasticsearchUrl, temporalIndexName, false)
	}
}

func RecreateIndex(elasticsearchUrl string, indexName string) {
	log.Printf("→ ES →→ Recreating index with name %v\n", indexName)
	DeleteIndex(elasticsearchUrl, indexName, false)
	CreateIndex(elasticsearchUrl, indexName)
}

func GetIndicesFilteringByPrefix(elasticsearchUrl string, prefix string) []types.Index {
	client := CreateElasticSearchClient(elasticsearchUrl)

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
