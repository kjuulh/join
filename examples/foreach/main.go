// Notify multiple services in parallel using ForEach.
// Like Map, but when you don't need return values.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/kjuulh/join"
)

func notify(ctx context.Context, service string) error {
	time.Sleep(50 * time.Millisecond)
	fmt.Printf("notified %s\n", service)
	return nil
}

func main() {
	ctx := context.Background()

	services := []string{"slack", "email", "webhook"}
	if err := join.ForEach(ctx, services, notify); err != nil {
		log.Fatal(err)
	}
}
