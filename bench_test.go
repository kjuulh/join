package join_test

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/kjuulh/join"
	"golang.org/x/sync/errgroup"
)

// transform is a trivial but non-inlinable operation used across all benchmarks
// to keep the comparison fair. The work is intentionally cheap so that we
// measure coordination overhead, not I/O latency.
//
//go:noinline
func transform(input string) string {
	return input + "-mapped"
}

func makeInputs(n int) []string {
	items := make([]string, n)
	for i := range items {
		items[i] = strconv.Itoa(i)
	}
	return items
}

// --- join.Map (this library) ---

func benchJoinMap(b *testing.B, items []string) {
	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		_, err := join.Map(ctx, items, func(ctx context.Context, s string) (string, error) {
			return transform(s), nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

// --- errgroup (x/sync) ---

func benchErrgroup(b *testing.B, items []string) {
	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		results := make([]string, len(items))
		g, _ := errgroup.WithContext(ctx)
		for i, item := range items {
			g.Go(func() error {
				results[i] = transform(item)
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			b.Fatal(err)
		}
	}
}

// --- errgroup with limit (bounded parallelism) ---

func benchErrgroupLimited(b *testing.B, items []string) {
	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		results := make([]string, len(items))
		g, _ := errgroup.WithContext(ctx)
		g.SetLimit(64)
		for i, item := range items {
			g.Go(func() error {
				results[i] = transform(item)
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			b.Fatal(err)
		}
	}
}

// --- raw goroutines + sync.WaitGroup (minimal overhead baseline) ---

func benchRawGoroutines(b *testing.B, items []string) {
	b.ResetTimer()
	for b.Loop() {
		results := make([]string, len(items))
		var wg sync.WaitGroup
		wg.Add(len(items))
		for i, item := range items {
			go func() {
				defer wg.Done()
				results[i] = transform(item)
			}()
		}
		wg.Wait()
	}
}

// --- sequential (no concurrency, pure baseline) ---

func benchSequential(b *testing.B, items []string) {
	b.ResetTimer()
	for b.Loop() {
		results := make([]string, len(items))
		for i, item := range items {
			results[i] = transform(item)
		}
	}
}

// --- benchmark matrix: Map ---

func BenchmarkMap(b *testing.B) {
	for _, n := range []int{1, 10, 100, 1000} {
		items := makeInputs(n)
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.Run("join.Map", func(b *testing.B) { benchJoinMap(b, items) })
			b.Run("errgroup", func(b *testing.B) { benchErrgroup(b, items) })
			b.Run("errgroup_limited", func(b *testing.B) { benchErrgroupLimited(b, items) })
			b.Run("raw_goroutines", func(b *testing.B) { benchRawGoroutines(b, items) })
			b.Run("sequential", func(b *testing.B) { benchSequential(b, items) })
		})
	}
}

// --- benchmark: Join1–5 vs errgroup equivalents ---

// work simulates a cheap unit of work returning a typed result.
//
//go:noinline
func work(v int) int { return v * 2 }

func BenchmarkJoin1(b *testing.B) {
	ctx := context.Background()

	b.Run("join", func(b *testing.B) {
		for b.Loop() {
			_, err := join.Join1(ctx, func(ctx context.Context) (int, error) {
				return work(1), nil
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("errgroup", func(b *testing.B) {
		for b.Loop() {
			var a int
			g, _ := errgroup.WithContext(ctx)
			g.Go(func() error { a = work(1); return nil })
			if err := g.Wait(); err != nil {
				b.Fatal(err)
			}
			_ = a
		}
	})
}

func BenchmarkJoin2(b *testing.B) {
	ctx := context.Background()

	b.Run("join", func(b *testing.B) {
		for b.Loop() {
			_, _, err := join.Join2(ctx,
				func(ctx context.Context) (int, error) { return work(1), nil },
				func(ctx context.Context) (int, error) { return work(2), nil },
			)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("errgroup", func(b *testing.B) {
		for b.Loop() {
			var a, b2 int
			g, _ := errgroup.WithContext(ctx)
			g.Go(func() error { a = work(1); return nil })
			g.Go(func() error { b2 = work(2); return nil })
			if err := g.Wait(); err != nil {
				b.Fatal(err)
			}
			_, _ = a, b2
		}
	})
}

func BenchmarkJoin3(b *testing.B) {
	ctx := context.Background()

	b.Run("join", func(b *testing.B) {
		for b.Loop() {
			_, _, _, err := join.Join3(ctx,
				func(ctx context.Context) (int, error) { return work(1), nil },
				func(ctx context.Context) (int, error) { return work(2), nil },
				func(ctx context.Context) (int, error) { return work(3), nil },
			)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("errgroup", func(b *testing.B) {
		for b.Loop() {
			var a, b2, c int
			g, _ := errgroup.WithContext(ctx)
			g.Go(func() error { a = work(1); return nil })
			g.Go(func() error { b2 = work(2); return nil })
			g.Go(func() error { c = work(3); return nil })
			if err := g.Wait(); err != nil {
				b.Fatal(err)
			}
			_, _, _ = a, b2, c
		}
	})
}

func BenchmarkJoin4(b *testing.B) {
	ctx := context.Background()

	b.Run("join", func(b *testing.B) {
		for b.Loop() {
			_, _, _, _, err := join.Join4(ctx,
				func(ctx context.Context) (int, error) { return work(1), nil },
				func(ctx context.Context) (int, error) { return work(2), nil },
				func(ctx context.Context) (int, error) { return work(3), nil },
				func(ctx context.Context) (int, error) { return work(4), nil },
			)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("errgroup", func(b *testing.B) {
		for b.Loop() {
			var a, b2, c, d int
			g, _ := errgroup.WithContext(ctx)
			g.Go(func() error { a = work(1); return nil })
			g.Go(func() error { b2 = work(2); return nil })
			g.Go(func() error { c = work(3); return nil })
			g.Go(func() error { d = work(4); return nil })
			if err := g.Wait(); err != nil {
				b.Fatal(err)
			}
			_, _, _, _ = a, b2, c, d
		}
	})
}

func BenchmarkJoin5(b *testing.B) {
	ctx := context.Background()

	b.Run("join", func(b *testing.B) {
		for b.Loop() {
			_, _, _, _, _, err := join.Join5(ctx,
				func(ctx context.Context) (int, error) { return work(1), nil },
				func(ctx context.Context) (int, error) { return work(2), nil },
				func(ctx context.Context) (int, error) { return work(3), nil },
				func(ctx context.Context) (int, error) { return work(4), nil },
				func(ctx context.Context) (int, error) { return work(5), nil },
			)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("errgroup", func(b *testing.B) {
		for b.Loop() {
			var a, b2, c, d, e int
			g, _ := errgroup.WithContext(ctx)
			g.Go(func() error { a = work(1); return nil })
			g.Go(func() error { b2 = work(2); return nil })
			g.Go(func() error { c = work(3); return nil })
			g.Go(func() error { d = work(4); return nil })
			g.Go(func() error { e = work(5); return nil })
			if err := g.Wait(); err != nil {
				b.Fatal(err)
			}
			_, _, _, _, _ = a, b2, c, d, e
		}
	})
}
