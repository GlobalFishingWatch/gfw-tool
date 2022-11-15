package common

import (
	"encoding/json"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"log"
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

func CheckIfIndexExists(esClient *elasticsearch.Client, indexName string) bool {
	res, err := esClient.Indices.Exists([]string{indexName})
	if err != nil {
		log.Fatalf("→ ES →→ Cannot check if index exists: %s", err)
	}
	log.Println("→ ES →→ Checking if index exists on ElasticSearch: ", res.StatusCode == 200)
	return res.StatusCode == 200
}

func DeleteIndex(elasticSearchClient *elasticsearch.Client, indexName string) {
	log.Printf("→ ES →→ Deleting index with name %v\n", indexName)
	if _, err := elasticSearchClient.Indices.Delete([]string{indexName}); err != nil {
		log.Fatalf("→ ES →→ Cannot delete index: %s", err)
	}
}

func ParseEsAPIResponse(res *esapi.Response) map[string]interface{} {
	responseBody := make(map[string]interface{})
	json.NewDecoder(res.Body).Decode(&responseBody)
	return responseBody
}
