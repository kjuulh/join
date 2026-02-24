// Fetch three users in parallel using All.
// All tasks share the same return type.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/kjuulh/join"
)

type User struct{ Name string }

func fetchUser(ctx context.Context, name string) (*User, error) {
	time.Sleep(50 * time.Millisecond)
	return &User{Name: name}, nil
}

func main() {
	ctx := context.Background()

	users, err := join.All(ctx,
		func(ctx context.Context) (*User, error) { return fetchUser(ctx, "Alice") },
		func(ctx context.Context) (*User, error) { return fetchUser(ctx, "Bob") },
		func(ctx context.Context) (*User, error) { return fetchUser(ctx, "Carol") },
	)
	if err != nil {
		log.Fatal(err)
	}

	for _, u := range users {
		fmt.Println(u.Name)
	}
}
