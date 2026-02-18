package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/vkozio/ladybug-go-zero"
)

// Basic example: open DB, run a Cypher query, use typed accessors and Scan.
func main() {
	ctx := context.Background()

	cfg := &ladybug.Config{
		Path: "/tmp/ladybug-go-zero-example",
		OnQueryFinished: func(ctx context.Context, cypher string, s ladybug.QuerySummary, err error) {
			if err != nil {
				log.Printf("query failed: %s: %v", cypher, err)
				return
			}
			log.Printf("query ok: %s (compile=%.2fms exec=%.2fms)", cypher, s.CompileMS, s.ExecMS)
		},
	}

	db, err := ladybug.Open(ctx, "", cfg)
	if err != nil {
		log.Fatalf("Open: %v", err)
	}
	defer db.Close()

	conn, err := db.Conn(ctx)
	if err != nil {
		log.Fatalf("Conn: %v", err)
	}
	defer conn.Close()

	// Simple query with typed accessors.
	res, err := conn.Query(ctx, "RETURN 1 AS x, 'hello' AS y, current_timestamp() AS ts")
	if err != nil {
		log.Fatalf("Query: %v", err)
	}
	defer res.Close()

	for row, ok := res.Next(); ok; row, ok = res.Next() {
		var (
			x  int64
			y  string
			ts time.Time
		)
		if err := row.Scan(&x, &y, &ts); err != nil {
			log.Fatalf("Scan: %v", err)
		}
		fmt.Printf("x=%d y=%s ts=%s\n", x, y, ts.Format(time.RFC3339Nano))
	}
}

