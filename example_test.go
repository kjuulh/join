package join_test

import (
	"context"
	"fmt"
	"time"

	"github.com/kjuulh/join"
)

// Simulated API types and functions for the examples.
type User struct {
	ID   int
	Name string
}

type Post struct {
	Title string
}

type Preferences struct {
	Theme string
}

func fetchUser(ctx context.Context, id int) (*User, error) {
	time.Sleep(10 * time.Millisecond) // simulate network
	return &User{ID: id, Name: "Alice"}, nil
}

func fetchPosts(ctx context.Context, userID int) ([]*Post, error) {
	time.Sleep(10 * time.Millisecond)
	return []*Post{{Title: "Hello World"}, {Title: "Go Generics"}}, nil
}

func fetchPreferences(ctx context.Context, userID int) (*Preferences, error) {
	time.Sleep(10 * time.Millisecond)
	return &Preferences{Theme: "dark"}, nil
}

func syncToService(ctx context.Context, name string) error {
	time.Sleep(10 * time.Millisecond)
	return nil
}

func ExampleJoin2() {
	ctx := context.Background()

	user, posts, err := join.Join2(ctx,
		func(ctx context.Context) (*User, error) {
			return fetchUser(ctx, 1)
		},
		func(ctx context.Context) ([]*Post, error) {
			return fetchPosts(ctx, 1)
		},
	)
	if err != nil {
		fmt.Println("error:", err)
		return
	}

	fmt.Printf("user: %s, posts: %d\n", user.Name, len(posts))
	// Output: user: Alice, posts: 2
}

func ExampleJoin3() {
	ctx := context.Background()

	user, posts, prefs, err := join.Join3(ctx,
		func(ctx context.Context) (*User, error) {
			return fetchUser(ctx, 1)
		},
		func(ctx context.Context) ([]*Post, error) {
			return fetchPosts(ctx, 1)
		},
		func(ctx context.Context) (*Preferences, error) {
			return fetchPreferences(ctx, 1)
		},
	)
	if err != nil {
		fmt.Println("error:", err)
		return
	}

	fmt.Printf("user: %s, posts: %d, theme: %s\n", user.Name, len(posts), prefs.Theme)
	// Output: user: Alice, posts: 2, theme: dark
}

func ExampleJoinAll() {
	ctx := context.Background()

	err := join.JoinAll(ctx,
		func(ctx context.Context) error {
			return syncToService(ctx, "service-a")
		},
		func(ctx context.Context) error {
			return syncToService(ctx, "service-b")
		},
		func(ctx context.Context) error {
			return syncToService(ctx, "service-c")
		},
	)
	if err != nil {
		fmt.Println("error:", err)
		return
	}

	fmt.Println("all synced")
	// Output: all synced
}

func ExampleAll() {
	ctx := context.Background()

	ids := []int{1, 2, 3, 4, 5}
	tasks := make([]join.Task[*User], len(ids))
	for i, id := range ids {
		tasks[i] = func(ctx context.Context) (*User, error) {
			return fetchUser(ctx, id)
		}
	}

	users, err := join.All(ctx, tasks...)
	if err != nil {
		fmt.Println("error:", err)
		return
	}

	fmt.Printf("fetched %d users\n", len(users))
	// Output: fetched 5 users
}

func ExampleMap() {
	ctx := context.Background()

	// Fetch multiple users in parallel from a list of IDs.
	ids := []int{1, 2, 3, 4, 5}
	users, err := join.Map(ctx, ids, fetchUser)
	if err != nil {
		fmt.Println("error:", err)
		return
	}

	fmt.Printf("fetched %d users, first: %s\n", len(users), users[0].Name)
	// Output: fetched 5 users, first: Alice
}

func ExampleForEach() {
	ctx := context.Background()

	// Sync to multiple services in parallel.
	services := []string{"service-a", "service-b", "service-c"}
	err := join.ForEach(ctx, services, syncToService)
	if err != nil {
		fmt.Println("error:", err)
		return
	}

	fmt.Println("all synced")
	// Output: all synced
}
