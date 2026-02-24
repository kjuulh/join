// Fetch a user profile and their posts in parallel using Join2.
// Each result has its own type — no type assertions needed.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/kjuulh/join"
)

type User struct{ Name string }
type Post struct{ Title string }

func main() {
	ctx := context.Background()

	user, posts, err := join.Join2(ctx,
		func(ctx context.Context) (*User, error) {
			time.Sleep(50 * time.Millisecond) // simulate fetch
			return &User{Name: "Alice"}, nil
		},
		func(ctx context.Context) ([]Post, error) {
			time.Sleep(50 * time.Millisecond)
			return []Post{{Title: "Hello"}, {Title: "World"}}, nil
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("user: %s\n", user.Name)
	for _, p := range posts {
		fmt.Printf("post: %s\n", p.Title)
	}
}
