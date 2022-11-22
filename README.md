# gfw-tool

## Description

The gfw-tool is a CLI that grouped a set of tools to reuse common processes in an easy way.

### Tech Stack:
* [Golang](https://golang.org/doc/)
* [Cobra Framework](https://github.com/spf13/cobra#working-with-flags)
* [Viper](https://github.com/spf13/viper)
* [Docker](https://docs.docker.com/)

### Git
* Repository:  
  https://github.com/GlobalFishingWatch/bigquery-tool

## Commands and subcommands
* Bigquery
  * `create-table`: Create a new table from a query.
  * `create-temporal-table`: Create a new temporal table from a query. Expires in 12h by default.
  * `execute-raw-query`: Execute a SQL statement defined by the user.
* bq2es
  * `export`: Export data from a query (Big Query) and imports the documents in Elastic Search
* bq2gcs
  * `export`: Export data from a query (Big Query) and imports the documents in Google Cloud Storage
* bq2psql
  * `export`: Export data from a query (Big Query) and imports the documents in Postgres SQL
  * `export-csv`: Export data from a query (Big Query) and imports the documents in Postgres SQL using a CSV file
* elasticsearch
  * `add-alias`: Add alias to a specific index
  * `create-index`: Create a new index.
  * `delete-index`: Delete a index.
  * `delete-index-by-prefix`: Delete all indices that match with the prefix. You can exclude one index to delete all indices except that.
* gcs
  * `copy-bucket-directory`: Copy a directory from one bucket to another or in the same bucket with another directory name.
  * `copy-object`: Copy a specific object to other location.
  * `delete-object`: Delete a object.
  * `merge-multiple-objects`: Merge multiple object into one.
  * `upload-object`: Upload a object to a bucket.
* gcs2bq
  * `export`: Export data from Google Cloud Storage (JSON files) and imports in Big Query.
* postgres
  * `create-index`: Create an index and point it to a table.
  * `create-view`: Create a view.
  * `delete-table`: Delete a table.
  * `delete-view`: Delete a view.
  * `execute-raw-sql`: Execute a SQL statement defined by the user.

## Build a binary file
You can build a binary to avoid using Go land, 
```
go build
```
The destination binary filename is gfw-tool
