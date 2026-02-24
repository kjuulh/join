// Run several independent tasks in parallel using JoinAll.
// Useful for fire-and-forget work where you only care about errors.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/kjuulh/join"
)

func main() {
	ctx := context.Background()

	err := join.JoinAll(ctx,
		func(ctx context.Context) error {
			time.Sleep(50 * time.Millisecond)
			fmt.Println("warmed cache")
			return nil
		},
		func(ctx context.Context) error {
			time.Sleep(50 * time.Millisecond)
			fmt.Println("synced index")
			return nil
		},
		func(ctx context.Context) error {
			time.Sleep(50 * time.Millisecond)
			fmt.Println("sent notification")
			return nil
		},
	)
	if err != nil {
		log.Fatal(err)
	}
}
