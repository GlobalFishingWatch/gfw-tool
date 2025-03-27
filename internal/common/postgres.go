package common

import (
	"context"
	"fmt"
	"github.com/GlobalFishingWatch/gfw-tool/types"
	"github.com/jackc/pgx/v4"
	"log"
	"os"
	"time"
)

func PostgresCreateClient(ctx context.Context, postgresConfig types.PostgresConfig) *pgx.Conn {
	ip := Getip2()

	log.Printf("→ PG →→ Public IP: %v", ip)

	uri := "postgresql://" + postgresConfig.Addr + "/" + postgresConfig.Database + "?user=" + postgresConfig.User + "&password=" + postgresConfig.Password
	conn, err := pgx.Connect(ctx, uri)
	if err != nil {
		log.Printf("Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	return conn
}

func PostgresCreateTable(ctx context.Context, postgresConfig types.PostgresConfig, tableName string, schema string) {
	client := PostgresCreateClient(ctx, postgresConfig)
	defer client.Close(ctx)

	log.Println("→ PG →→ Creating a new table")
	BigQueryCreateTableCommand := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (
				%v
           );`, tableName, schema)
	log.Printf("→ PG →→ Creating table with command %s", BigQueryCreateTableCommand)
	_, err := client.Exec(ctx, BigQueryCreateTableCommand)
	if err != nil {
		log.Fatalf("→ PG →→ Error creating table: %v", err)
	}

	log.Printf("→ PG →→ Successfully created table with name %v", tableName)
}

func PostgresCreateIndex(ctx context.Context, postgresConfig types.PostgresConfig, tableName string, indexName string, column string) {
	client := PostgresCreateClient(ctx, postgresConfig)
	defer client.Close(ctx)

	log.Println("→ PG →→ Creating a new index")

	createIndexCommand := fmt.Sprintf(`
		CREATE INDEX IF NOT EXISTS %s 
    		ON %s(%s)
		`, indexName, tableName, column)
	log.Printf("→ PG →→ Creating index with command %s", createIndexCommand)
	_, err := client.Exec(ctx, createIndexCommand)
	if err != nil {
		log.Fatalf("→ PG →→ Error creating index: %v", err)
	}

	log.Printf("→ PG →→ Successfully created view with name %v", indexName)
}

func PostgresCreateView(ctx context.Context, postgresConfig types.PostgresConfig, viewName string, tableName string) {
	client := PostgresCreateClient(ctx, postgresConfig)
	defer client.Close(ctx)

	log.Println("→ PG →→ Creating a new view")

	createViewCommand := fmt.Sprintf(`
		CREATE VIEW %s AS
    		SELECT *
    		FROM %s
		`, viewName, tableName)
	log.Printf("→ PG →→ Creating view with command %s", createViewCommand)

	_, err := client.Exec(ctx, createViewCommand)
	if err != nil {
		log.Fatalf("→ PG →→ Error creating view: %v", err)
	}

	log.Printf("→ PG →→ Successfully created view with name %v", viewName)
}

func PostgresDeleteTable(ctx context.Context, postgresConfig types.PostgresConfig, tableName string) {

	client := PostgresCreateClient(ctx, postgresConfig)
	defer client.Close(ctx)

	log.Println("→ PG →→ Deleting a table")

	deleteTableCommand := fmt.Sprintf(`DROP TABLE IF EXISTS %s;`, tableName)
	log.Printf("→ PG →→ Deleting table with name %s and command %s", tableName, deleteTableCommand)
	_, err := client.Exec(ctx, deleteTableCommand)
	if err != nil {
		log.Fatalf("→ PG →→ Error deleting table: %v", err)
	}

	log.Printf("→ PG →→ Successfully deleting table with name %v", tableName)
}

func PostgresDeleteView(ctx context.Context, postgresConfig types.PostgresConfig, viewName string) {
	client := PostgresCreateClient(ctx, postgresConfig)
	defer client.Close(ctx)

	log.Println("→ PG →→ Deleting a view")

	deleteViewCommand := fmt.Sprintf(`DROP VIEW IF EXISTS %s;`, viewName)
	log.Printf("→ PG →→ Deleting view with name %s and command %s", viewName, deleteViewCommand)
	_, err := client.Exec(ctx, deleteViewCommand)
	if err != nil {
		log.Fatalf("→ PG →→ Error deleting view: %v", err)
	}

	log.Printf("→ PG →→ Successfully deleting view with name %v", viewName)
}

func PostgresExecuteSQLCommand(ctx context.Context, postgresConfig types.PostgresConfig, sql string, retries int) {
	client := PostgresCreateClient(ctx, postgresConfig)
	defer client.Close(ctx)

	log.Printf("→ PG →→ Executing the next SQL:  %s. Retries: %v", sql, retries)
	_, err := client.Exec(ctx, sql)
	if err != nil {
		retries = retries + 1
		log.Printf("→ PG →→ Error executing the SQL:  %s. Retries: %v", sql, retries)
		if retries < 3 {
			time.Sleep(60 * time.Second)
			PostgresExecuteSQLCommand(ctx, postgresConfig, sql, retries)
		} else {
			log.Fatalf("→ PG →→ Error executing the sql: %v. Process finished.", err)
		}
	}

	log.Print("→ PG →→ Successfully executing the command")
}
