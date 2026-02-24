// Fetch users from a slice of IDs in parallel using Map.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/kjuulh/join"
)

type User struct {
	ID   int
	Name string
}

func fetchUser(ctx context.Context, id int) (*User, error) {
	time.Sleep(50 * time.Millisecond)
	return &User{ID: id, Name: fmt.Sprintf("user-%d", id)}, nil
}

func main() {
	ctx := context.Background()

	ids := []int{1, 2, 3, 4, 5}
	users, err := join.Map(ctx, ids, fetchUser)
	if err != nil {
		log.Fatal(err)
	}

	for _, u := range users {
		fmt.Printf("%d: %s\n", u.ID, u.Name)
	}
}
