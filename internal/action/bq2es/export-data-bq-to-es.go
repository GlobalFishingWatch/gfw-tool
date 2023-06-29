package bq2es

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/GlobalFishingWatch/gfw-tool/internal/common"
	"github.com/GlobalFishingWatch/gfw-tool/types"
	"github.com/GlobalFishingWatch/gfw-tool/utils"
	"github.com/dustin/go-humanize"
)

var onErrorAction string
var temporalIndexName string
var currentBatch = 0
var totalItemsImported = 0

func ExportBigQueryToElasticSearch(params types.BQ2ESImportConfig) {

	validateFlags(params)

	ctx := context.Background()

	onErrorAction = params.OnError

	indexExists := common.ElasticSearchCheckIfIndexExists(params.ElasticSearchUrl, params.IndexName)
	if indexExists == true && onErrorAction == "reindex" {
		log.Println("→ Reindexing index to avoid losing data")
		temporalIndexName = params.IndexName + "-" + time.Now().UTC().Format("2006-01-02") + "-reindexed"
		common.ElasticSearchReindex(params.ElasticSearchUrl, params.IndexName, temporalIndexName)
	}

	ch := make(chan map[string]bigquery.Value, 500)

	log.Println("→ Getting results from big query")
	getResultsFromBigQuery(ctx, params.ProjectId, params.Query, params.Labels, ch)

	log.Println("→ Importing results to elasticsearch (Bulk)")
	if strings.TrimRight(params.ImportMode, "\n") == "recreate" {
		common.ElasticSearchRecreateIndex(params.ElasticSearchUrl, params.IndexName)
	}
	var wg sync.WaitGroup
	const threads = 15
	const Batch = 2000

	log.Println("→ ES →→ Importing data to ElasticSearch")
	log.Printf("→ ES →→ Opening [%s] threads", threads)
	log.Printf("→ ES →→ Bulk size [%s] documents", Batch)
	log.Println(strings.Repeat("▁", 65))
	start := time.Now().UTC()
	createPreReport(Batch, start)
	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup, ch chan map[string]bigquery.Value) {
			importBulk(params.ElasticSearchUrl, params.IndexName, params.ImportMode, params.Normalize, params.NormalizedPropertyName, params.NormalizeEndpoint, Batch, start, ch)
			wg.Done()
		}(&wg, ch)
	}
	wg.Wait()
}

func validateFlags(params types.BQ2ESImportConfig) {

	utils.ValidateUrl(params.ElasticSearchUrl)

	if strings.TrimRight(params.ImportMode, "\n") != "recreate" && strings.TrimRight(params.ImportMode, "\n") != "append" {
		log.Fatalln("--import-mode should equal to 'recreate' or 'append'")
	}
	if strings.TrimRight(params.OnError, "\n") != "delete" && strings.TrimRight(params.OnError, "\n") != "keep" && strings.TrimRight(params.OnError, "\n") != "reindex" {
		log.Fatalln("--on-error should equal to 'delete', 'keep' or 'reindex'")
	}

	if strings.TrimRight(params.Normalize, "\n") != "" && strings.TrimRight(params.NormalizeEndpoint, "\n") == "" {
		log.Fatalln("if you set the flag normalized, you must to set the normalize endpoint")
	}

	if strings.TrimRight(params.Normalize, "\n") != "" && strings.TrimRight(params.NormalizedPropertyName, "\n") == "" {
		log.Fatalln("if you set the flag normalized, you must to set the normalize property name")
	}

}

// BigQuery Functions
func getResultsFromBigQuery(ctx context.Context, projectId string, query string, labels map[string]string, ch chan map[string]bigquery.Value) {
	iterator := common.BigQueryMakeQuery(ctx, projectId, query, false, labels)
	go common.BigQueryParseResultsToJson(iterator, ch)
}

// Elastic Search Functions
func importBulk(
	elasticsearchUrl string,
	indexName string,
	importMode string,
	normalize string,
	normalizePropertyName string,
	normalizeEndpoint string,
	Batch int,
	start time.Time,
	ch chan map[string]bigquery.Value,
) {

	var (
		buf         bytes.Buffer
		numItems    int
		numErrors   int
		numIndexed  int
		requestBody map[string]string
		jsonStr     []byte
		err         error
		req         *http.Request
		resp        *http.Response
	)

	client := &http.Client{}

	numItems = 0
	for doc := range ch {
		if strings.TrimRight(normalize, "\n") != "" {
			if doc[normalize] == nil {
				// log.Printf("The property %v does not exist on the documents", normalize)
				doc[normalizePropertyName] = ""
			} else {
				requestBody = map[string]string{
					"type":  normalize,
					"value": doc[normalize].(string),
				}
				jsonStr, err = json.Marshal(requestBody)
				if err != nil {
					doc["normalized_"+normalize] = doc[normalize].(string)
				} else {
					req, err = http.NewRequest("POST", normalizeEndpoint, bytes.NewBuffer(jsonStr))
					req.Header.Set("Content-Type", "application/json")
					resp, err = client.Do(req)
					if err != nil {
						log.Fatalf("Error normalizing property %s: %s", normalize, err)
					}

					if resp.StatusCode != 200 {
						// log.Printf("Error normalizing the property %s. Value: %s. Error: %s", normalize, doc[normalize].(string), resp.Status)
						doc["normalized_"+normalize] = doc[normalize].(string)
					} else {
						var responseParsed = types.NormalizeResponse{}
						err = json.NewDecoder(resp.Body).Decode(&responseParsed)
						if err != nil {
							// log.Printf("Error normalizing the property %s. Error: %s", normalize, err)
							doc["normalized_"+normalize] = doc[normalize].(string)
						} else {
							doc["normalized_"+normalize] = responseParsed.Result
						}
					}
					resp.Body.Close()
				}

			}
		}
		preparePayload(&buf, doc)
		numItems++
		if numItems == Batch {
			currentBatch++
			totalItemsImported += numItems
			errors, items, indexed := executeBulk(elasticsearchUrl, indexName, &buf)
			numErrors += errors
			numItems += items
			numIndexed += indexed
			numItems = 0
			buf = bytes.Buffer{}
			// log.Println("Cleaning memory")
			runtime.GC()
		}
	}

	if numItems > 0 {
		currentBatch++
		totalItemsImported += numItems
		errors, items, indexed := executeBulk(elasticsearchUrl, indexName, &buf)
		numErrors += errors
		numItems += items
		numIndexed += indexed
		buf = bytes.Buffer{}
		// log.Println("Cleaning memory")
		runtime.GC()
	}

	createReport(start, numErrors, numIndexed)
}

func executeBulk(elasticsearchUrl string, indexName string, buf *bytes.Buffer) (int, int, int) {
	var (
		raw        map[string]interface{}
		blk        *types.ElasticSearchBulkResponse
		numErrors  int
		numItems   int
		numIndexed int
	)
	log.Printf("Batch [%d]", currentBatch)

	res := common.ElasticSearchExecuteBulk(
		elasticsearchUrl,
		indexName,
		buf,
		currentBatch,
		onErrorAction,
	)
	if res.IsError() {
		numErrors += numItems
		common.ElasticSearchExecuteOnErrorAction(elasticsearchUrl, indexName, onErrorAction, "")
		log.Printf("Response error: [%s]", res.Body)
		if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
			log.Fatalf("Failure to to parse response body: %s", err)
		}
		log.Fatalf("  Error: [%d] %s: %s",
			res.StatusCode,
			raw["error"].(map[string]interface{})["type"],
			raw["error"].(map[string]interface{})["reason"],
		)
	}

	if err := json.NewDecoder(res.Body).Decode(&blk); err != nil {
		common.ElasticSearchExecuteOnErrorAction(elasticsearchUrl, indexName, onErrorAction, "")
		log.Fatalf("Failure to to parse response body: %s", err)
	}

	for _, d := range blk.Items {
		if d.Index.Status > 201 {
			numErrors++
			common.ElasticSearchExecuteOnErrorAction(elasticsearchUrl, indexName, onErrorAction, "")
			log.Fatalf("  Error: [%d]: %s: %s: %s: %s",
				d.Index.Status,
				d.Index.Error.Type,
				d.Index.Error.Reason,
				d.Index.Error.Cause.Type,
				d.Index.Error.Cause.Reason,
			)
		}
		numIndexed++
	}
	res.Body.Close()
	return numErrors, numItems, numIndexed
}

func preparePayload(buf *bytes.Buffer, document map[string]bigquery.Value) {
	var meta []byte
	if _, found := document["id"]; found {
		meta = []byte(fmt.Sprintf(`{ "index" : { "_id": "%s" }}%s`, document["id"].(string), "\n"))
	} else {
		meta = []byte(fmt.Sprintf(`{ "index" : { }%s`, "\n"))
	}

	body, err := json.Marshal(document)
	if err != nil {
		log.Fatalf("→ ES →→ Error parsing to json: %v", err)
	}
	body = append(body, "\n"...)
	buf.Grow(len(meta) + len(body))
	buf.Write(meta)
	buf.Write(body)
}

// Reports functions
func createPreReport(Batch int, start time.Time) {
	log.Printf(
		"→ ES →→ \x1b[1mBulk\x1b[0m: Batch size [%s]",
		humanize.Comma(int64(Batch)))
	log.Printf("→ ES →→  Start time: %v\n", start)
	log.Print("→ ES →→  Sending Batch ")
	log.Println(strings.Repeat("▁", 65))
}

func createReport(start time.Time, numErrors int, numIndexed int) {
	log.Print("\n")
	log.Println(strings.Repeat("▔", 65))

	duration := time.Since(start)

	if numErrors > 0 {
		log.Fatalf(
			"→ ES →→ Indexed [%s] documents with [%s] errors in %s (%s docs/sec)",
			humanize.Comma(int64(numIndexed)),
			humanize.Comma(int64(numErrors)),
			duration.Truncate(time.Millisecond),
			humanize.Comma(int64(1000.0/float64(duration/time.Millisecond)*float64(numIndexed))),
		)
		return
	}
	log.Printf(
		"→ ES →→ Sucessfuly indexed [%s] documents in %s (%s docs/sec)",
		humanize.Comma(int64(numIndexed)),
		duration.Truncate(time.Millisecond),
		humanize.Comma(int64(1000.0/float64(duration/time.Millisecond)*float64(numIndexed))),
	)
}
