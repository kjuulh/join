# join

Type-safe parallel execution for Go using generics. A more ergonomic alternative to `errgroup` — results are returned directly instead of through closure side-effects.

```
go get github.com/kjuulh/join
```

## Join — parallel execution with different return types

```go
user, posts, err := join.Join2(ctx,
    func(ctx context.Context) (*User, error) {
        return fetchUser(ctx, id)
    },
    func(ctx context.Context) ([]*Post, error) {
        return fetchPosts(ctx, id)
    },
)
```

Compare with `errgroup`:

```go
var user *User
var posts []*Post
g, ctx := errgroup.WithContext(ctx)
g.Go(func() error {
    var err error
    user, err = fetchUser(ctx, id)
    return err
})
g.Go(func() error {
    var err error
    posts, err = fetchPosts(ctx, id)
    return err
})
err := g.Wait()
```

`Join1` through `Join5` are available for up to five tasks with different return types.

Use `join.NoValue` when a task has no meaningful return value:

```go
user, _, err := join.Join2(ctx,
    func(ctx context.Context) (*User, error) { return fetchUser(ctx, id) },
    join.NoValue(func(ctx context.Context) error { return syncCache(ctx) }),
)
```

## Map — parallel transform over a slice

```go
users, err := join.Map(ctx, userIDs, fetchUser)
```

Applies a function to every element in parallel and returns results in order. This is the parallel equivalent of a for loop.

## Other functions

`ForEach` — parallel for loop without return values:

```go
err := join.ForEach(ctx, files, processFile)
```

`All` — run variadic tasks of the same type:

```go
items, err := join.All(ctx, taskA, taskB, taskC)
```

`JoinAll` — run variadic error-only tasks:

```go
err := join.JoinAll(ctx, syncA, syncB, syncC)
```

## Behavior

All functions cancel remaining tasks on the first error (fail-fast). Panics in tasks are recovered and returned as errors. Goroutines are always awaited before returning — no leaks.
