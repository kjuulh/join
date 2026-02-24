package join_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kjuulh/join"
)

var (
	errA = errors.New("error a")
	errB = errors.New("error b")
)

// --- None / NoValue ---

func TestNoValue_Success(t *testing.T) {
	called := false
	result, err := join.Join1(context.Background(), join.NoValue(func(ctx context.Context) error {
		called = true
		return nil
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != (join.None{}) {
		t.Fatalf("expected None{}, got %v", result)
	}
	if !called {
		t.Fatal("function was not called")
	}
}

func TestNoValue_Error(t *testing.T) {
	_, err := join.Join1(context.Background(), join.NoValue(func(ctx context.Context) error {
		return errA
	}))
	if !errors.Is(err, errA) {
		t.Fatalf("got %v, want %v", err, errA)
	}
}

func TestJoin2_MixedWithNoValue(t *testing.T) {
	var synced atomic.Bool
	user, _, err := join.Join2(context.Background(),
		func(ctx context.Context) (string, error) { return "alice", nil },
		join.NoValue(func(ctx context.Context) error {
			synced.Store(true)
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if user != "alice" {
		t.Fatalf("got %q, want %q", user, "alice")
	}
	if !synced.Load() {
		t.Fatal("NoValue task was not called")
	}
}

func TestJoin3_MultipleNoValue(t *testing.T) {
	user, _, _, err := join.Join3(context.Background(),
		func(ctx context.Context) (string, error) { return "alice", nil },
		join.NoValue(func(ctx context.Context) error { return nil }),
		join.NoValue(func(ctx context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if user != "alice" {
		t.Fatalf("got %q, want %q", user, "alice")
	}
}

func TestJoin2_NoValueError(t *testing.T) {
	_, _, err := join.Join2(context.Background(),
		func(ctx context.Context) (string, error) {
			<-ctx.Done()
			return "", ctx.Err()
		},
		join.NoValue(func(ctx context.Context) error { return errA }),
	)
	if !errors.Is(err, errA) {
		t.Fatalf("got %v, want %v", err, errA)
	}
}

// --- Join1 ---

func TestJoin1_Success(t *testing.T) {
	a, err := join.Join1(context.Background(), func(ctx context.Context) (int, error) {
		return 42, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if a != 42 {
		t.Fatalf("got %d, want 42", a)
	}
}

func TestJoin1_Error(t *testing.T) {
	_, err := join.Join1(context.Background(), func(ctx context.Context) (int, error) {
		return 0, errA
	})
	if !errors.Is(err, errA) {
		t.Fatalf("got %v, want %v", err, errA)
	}
}

// --- Join2 ---

func TestJoin2_Success(t *testing.T) {
	a, b, err := join.Join2(context.Background(),
		func(ctx context.Context) (int, error) { return 1, nil },
		func(ctx context.Context) (string, error) { return "hello", nil },
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if a != 1 {
		t.Fatalf("a: got %d, want 1", a)
	}
	if b != "hello" {
		t.Fatalf("b: got %q, want %q", b, "hello")
	}
}

func TestJoin2_FirstErrors(t *testing.T) {
	_, _, err := join.Join2(context.Background(),
		func(ctx context.Context) (int, error) { return 0, errA },
		func(ctx context.Context) (string, error) {
			<-ctx.Done()
			return "", ctx.Err()
		},
	)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestJoin2_SecondErrors(t *testing.T) {
	_, _, err := join.Join2(context.Background(),
		func(ctx context.Context) (int, error) {
			<-ctx.Done()
			return 0, ctx.Err()
		},
		func(ctx context.Context) (string, error) { return "", errB },
	)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestJoin2_BothError(t *testing.T) {
	_, _, err := join.Join2(context.Background(),
		func(ctx context.Context) (int, error) { return 0, errA },
		func(ctx context.Context) (string, error) { return "", errB },
	)
	if err == nil {
		t.Fatal("expected error")
	}
	// Should be one of the two errors (first one wins)
	if !errors.Is(err, errA) && !errors.Is(err, errB) {
		t.Fatalf("got %v, want errA or errB", err)
	}
}

// --- Join3 ---

func TestJoin3_Success(t *testing.T) {
	a, b, c, err := join.Join3(context.Background(),
		func(ctx context.Context) (int, error) { return 1, nil },
		func(ctx context.Context) (string, error) { return "two", nil },
		func(ctx context.Context) (float64, error) { return 3.0, nil },
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if a != 1 || b != "two" || c != 3.0 {
		t.Fatalf("got (%d, %q, %f), want (1, \"two\", 3.0)", a, b, c)
	}
}

func TestJoin3_MiddleErrors(t *testing.T) {
	_, _, _, err := join.Join3(context.Background(),
		func(ctx context.Context) (int, error) {
			<-ctx.Done()
			return 0, ctx.Err()
		},
		func(ctx context.Context) (string, error) { return "", errB },
		func(ctx context.Context) (float64, error) {
			<-ctx.Done()
			return 0, ctx.Err()
		},
	)
	if !errors.Is(err, errB) {
		t.Fatalf("got %v, want %v", err, errB)
	}
}

// --- Join4 ---

func TestJoin4_Success(t *testing.T) {
	a, b, c, d, err := join.Join4(context.Background(),
		func(ctx context.Context) (int, error) { return 1, nil },
		func(ctx context.Context) (string, error) { return "two", nil },
		func(ctx context.Context) (float64, error) { return 3.0, nil },
		func(ctx context.Context) (bool, error) { return true, nil },
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if a != 1 || b != "two" || c != 3.0 || d != true {
		t.Fatalf("unexpected results")
	}
}

func TestJoin4_Error(t *testing.T) {
	_, _, _, _, err := join.Join4(context.Background(),
		func(ctx context.Context) (int, error) { return 1, nil },
		func(ctx context.Context) (string, error) { return "", errA },
		func(ctx context.Context) (float64, error) { return 3.0, nil },
		func(ctx context.Context) (bool, error) { return true, nil },
	)
	if !errors.Is(err, errA) {
		t.Fatalf("got %v, want %v", err, errA)
	}
}

// --- Join5 ---

func TestJoin5_Success(t *testing.T) {
	a, b, c, d, e, err := join.Join5(context.Background(),
		func(ctx context.Context) (int, error) { return 1, nil },
		func(ctx context.Context) (string, error) { return "two", nil },
		func(ctx context.Context) (float64, error) { return 3.0, nil },
		func(ctx context.Context) (bool, error) { return true, nil },
		func(ctx context.Context) ([]int, error) { return []int{5}, nil },
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if a != 1 || b != "two" || c != 3.0 || d != true || len(e) != 1 || e[0] != 5 {
		t.Fatalf("unexpected results")
	}
}

func TestJoin5_Error(t *testing.T) {
	_, _, _, _, _, err := join.Join5(context.Background(),
		func(ctx context.Context) (int, error) { return 1, nil },
		func(ctx context.Context) (string, error) { return "two", nil },
		func(ctx context.Context) (float64, error) { return 0, errA },
		func(ctx context.Context) (bool, error) { return true, nil },
		func(ctx context.Context) ([]int, error) { return []int{5}, nil },
	)
	if !errors.Is(err, errA) {
		t.Fatalf("got %v, want %v", err, errA)
	}
}

// --- JoinAll ---

func TestJoinAll_Success(t *testing.T) {
	var count atomic.Int32
	err := join.JoinAll(context.Background(),
		func(ctx context.Context) error { count.Add(1); return nil },
		func(ctx context.Context) error { count.Add(1); return nil },
		func(ctx context.Context) error { count.Add(1); return nil },
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count.Load() != 3 {
		t.Fatalf("got count %d, want 3", count.Load())
	}
}

func TestJoinAll_Empty(t *testing.T) {
	err := join.JoinAll(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestJoinAll_Error(t *testing.T) {
	err := join.JoinAll(context.Background(),
		func(ctx context.Context) error { return nil },
		func(ctx context.Context) error { return errA },
		func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
	)
	if !errors.Is(err, errA) {
		t.Fatalf("got %v, want %v", err, errA)
	}
}

func TestJoinAll_MultipleTasks(t *testing.T) {
	var ran atomic.Int32
	tasks := make([]func(ctx context.Context) error, 100)
	for i := range tasks {
		tasks[i] = func(ctx context.Context) error {
			ran.Add(1)
			return nil
		}
	}
	err := join.JoinAll(context.Background(), tasks...)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ran.Load() != 100 {
		t.Fatalf("got %d tasks ran, want 100", ran.Load())
	}
}

// --- All ---

func TestAll_Success(t *testing.T) {
	results, err := join.All(context.Background(),
		func(ctx context.Context) (int, error) { return 1, nil },
		func(ctx context.Context) (int, error) { return 2, nil },
		func(ctx context.Context) (int, error) { return 3, nil },
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 3 || results[0] != 1 || results[1] != 2 || results[2] != 3 {
		t.Fatalf("got %v, want [1 2 3]", results)
	}
}

func TestAll_Empty(t *testing.T) {
	results, err := join.All[int](context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("got %v, want empty", results)
	}
}

func TestAll_Error(t *testing.T) {
	results, err := join.All(context.Background(),
		func(ctx context.Context) (int, error) { return 1, nil },
		func(ctx context.Context) (int, error) { return 0, errA },
		func(ctx context.Context) (int, error) {
			<-ctx.Done()
			return 0, ctx.Err()
		},
	)
	if !errors.Is(err, errA) {
		t.Fatalf("got %v, want %v", err, errA)
	}
	if results != nil {
		t.Fatalf("got %v, want nil on error", results)
	}
}

func TestAll_PreservesOrder(t *testing.T) {
	results, err := join.All(context.Background(),
		func(ctx context.Context) (int, error) {
			time.Sleep(30 * time.Millisecond)
			return 1, nil
		},
		func(ctx context.Context) (int, error) {
			return 2, nil
		},
		func(ctx context.Context) (int, error) {
			time.Sleep(10 * time.Millisecond)
			return 3, nil
		},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 3 || results[0] != 1 || results[1] != 2 || results[2] != 3 {
		t.Fatalf("got %v, want [1 2 3]", results)
	}
}

// --- Map ---

func TestMap_Success(t *testing.T) {
	ids := []int{1, 2, 3, 4, 5}
	results, err := join.Map(context.Background(), ids, func(ctx context.Context, id int) (string, error) {
		return fmt.Sprintf("user-%d", id), nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"user-1", "user-2", "user-3", "user-4", "user-5"}
	if len(results) != len(want) {
		t.Fatalf("got %d results, want %d", len(results), len(want))
	}
	for i, r := range results {
		if r != want[i] {
			t.Fatalf("results[%d]: got %q, want %q", i, r, want[i])
		}
	}
}

func TestMap_Empty(t *testing.T) {
	results, err := join.Map(context.Background(), []int{}, func(ctx context.Context, id int) (string, error) {
		return "", nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("got %v, want empty", results)
	}
}

func TestMap_Error(t *testing.T) {
	ids := []int{1, 2, 3}
	results, err := join.Map(context.Background(), ids, func(ctx context.Context, id int) (string, error) {
		if id == 2 {
			return "", errA
		}
		<-ctx.Done()
		return "", ctx.Err()
	})
	if !errors.Is(err, errA) {
		t.Fatalf("got %v, want %v", err, errA)
	}
	if results != nil {
		t.Fatalf("got %v, want nil on error", results)
	}
}

func TestMap_PreservesOrder(t *testing.T) {
	inputs := []int{1, 2, 3}
	results, err := join.Map(context.Background(), inputs, func(ctx context.Context, n int) (int, error) {
		// Reverse completion order via sleep
		time.Sleep(time.Duration(30-n*10) * time.Millisecond)
		return n * 10, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if results[0] != 10 || results[1] != 20 || results[2] != 30 {
		t.Fatalf("got %v, want [10 20 30]", results)
	}
}

func TestMap_RunsInParallel(t *testing.T) {
	inputs := make([]int, 10)
	for i := range inputs {
		inputs[i] = i
	}

	start := time.Now()
	_, err := join.Map(context.Background(), inputs, func(ctx context.Context, n int) (int, error) {
		time.Sleep(50 * time.Millisecond)
		return n, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > 90*time.Millisecond {
		t.Fatalf("tasks appear sequential: took %v", elapsed)
	}
}

func TestMap_PanicRecovery(t *testing.T) {
	_, err := join.Map(context.Background(), []int{1}, func(ctx context.Context, n int) (int, error) {
		panic("boom")
	})
	if err == nil {
		t.Fatal("expected error from panic")
	}
	if !strings.Contains(err.Error(), "panic") {
		t.Fatalf("error should mention panic: %v", err)
	}
}

// --- ForEach ---

func TestForEach_Success(t *testing.T) {
	var sum atomic.Int32
	err := join.ForEach(context.Background(), []int{1, 2, 3, 4}, func(ctx context.Context, n int) error {
		sum.Add(int32(n))
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sum.Load() != 10 {
		t.Fatalf("got sum %d, want 10", sum.Load())
	}
}

func TestForEach_Empty(t *testing.T) {
	err := join.ForEach(context.Background(), []int{}, func(ctx context.Context, n int) error {
		return errors.New("should not be called")
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestForEach_Error(t *testing.T) {
	err := join.ForEach(context.Background(), []int{1, 2, 3}, func(ctx context.Context, n int) error {
		if n == 2 {
			return errA
		}
		<-ctx.Done()
		return ctx.Err()
	})
	if !errors.Is(err, errA) {
		t.Fatalf("got %v, want %v", err, errA)
	}
}

func TestForEach_RunsInParallel(t *testing.T) {
	inputs := make([]int, 10)
	for i := range inputs {
		inputs[i] = i
	}

	start := time.Now()
	err := join.ForEach(context.Background(), inputs, func(ctx context.Context, n int) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > 90*time.Millisecond {
		t.Fatalf("tasks appear sequential: took %v", elapsed)
	}
}

func TestForEach_PanicRecovery(t *testing.T) {
	err := join.ForEach(context.Background(), []int{1}, func(ctx context.Context, n int) error {
		panic("boom")
	})
	if err == nil {
		t.Fatal("expected error from panic")
	}
	if !strings.Contains(err.Error(), "panic") {
		t.Fatalf("error should mention panic: %v", err)
	}
}

func TestForEach_CancelsOnError(t *testing.T) {
	var cancelled atomic.Bool
	err := join.ForEach(context.Background(), []int{1, 2}, func(ctx context.Context, n int) error {
		if n == 1 {
			return errA
		}
		<-ctx.Done()
		cancelled.Store(true)
		return ctx.Err()
	})
	if !errors.Is(err, errA) {
		t.Fatalf("got %v, want %v", err, errA)
	}
	time.Sleep(10 * time.Millisecond)
	if !cancelled.Load() {
		t.Fatal("other task was not cancelled")
	}
}

// --- Context cancellation ---

func TestJoin2_CancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := join.Join2(ctx,
		func(ctx context.Context) (int, error) {
			<-ctx.Done()
			return 0, ctx.Err()
		},
		func(ctx context.Context) (string, error) {
			<-ctx.Done()
			return "", ctx.Err()
		},
	)
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
}

func TestJoin2_CancellationPropagates(t *testing.T) {
	var cancelled atomic.Bool

	_, _, err := join.Join2(context.Background(),
		func(ctx context.Context) (int, error) {
			return 0, errA
		},
		func(ctx context.Context) (string, error) {
			<-ctx.Done()
			cancelled.Store(true)
			return "", ctx.Err()
		},
	)
	if err == nil {
		t.Fatal("expected error")
	}
	// Give a moment for the goroutine to observe cancellation
	time.Sleep(10 * time.Millisecond)
	if !cancelled.Load() {
		t.Fatal("second task was not cancelled")
	}
}

// --- Panic recovery ---

func TestJoin2_PanicRecovery(t *testing.T) {
	_, _, err := join.Join2(context.Background(),
		func(ctx context.Context) (int, error) {
			panic("boom")
		},
		func(ctx context.Context) (string, error) { return "ok", nil },
	)
	if err == nil {
		t.Fatal("expected error from panic")
	}
	if !strings.Contains(err.Error(), "panic") || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("error should mention panic: %v", err)
	}
}

func TestJoinAll_PanicRecovery(t *testing.T) {
	err := join.JoinAll(context.Background(),
		func(ctx context.Context) error { panic("boom") },
		func(ctx context.Context) error { return nil },
	)
	if err == nil {
		t.Fatal("expected error from panic")
	}
	if !strings.Contains(err.Error(), "panic") {
		t.Fatalf("error should mention panic: %v", err)
	}
}

func TestAll_PanicRecovery(t *testing.T) {
	_, err := join.All(context.Background(),
		func(ctx context.Context) (int, error) { panic("boom") },
		func(ctx context.Context) (int, error) { return 1, nil },
	)
	if err == nil {
		t.Fatal("expected error from panic")
	}
	if !strings.Contains(err.Error(), "panic") {
		t.Fatalf("error should mention panic: %v", err)
	}
}

// --- Error preserves identity (errors.Is) ---

func TestJoin2_ErrorIdentityPreserved(t *testing.T) {
	sentinel := errors.New("sentinel")
	_, _, err := join.Join2(context.Background(),
		func(ctx context.Context) (int, error) { return 0, sentinel },
		func(ctx context.Context) (string, error) { return "ok", nil },
	)
	if !errors.Is(err, sentinel) {
		t.Fatalf("errors.Is failed: got %v", err)
	}
}

// --- Parallel execution verification ---

func TestJoin2_RunsInParallel(t *testing.T) {
	start := time.Now()
	_, _, err := join.Join2(context.Background(),
		func(ctx context.Context) (int, error) {
			time.Sleep(50 * time.Millisecond)
			return 1, nil
		},
		func(ctx context.Context) (string, error) {
			time.Sleep(50 * time.Millisecond)
			return "two", nil
		},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	elapsed := time.Since(start)
	// If running in parallel, should take ~50ms, not ~100ms
	if elapsed > 90*time.Millisecond {
		t.Fatalf("tasks appear sequential: took %v", elapsed)
	}
}

func TestAll_RunsInParallel(t *testing.T) {
	n := 10
	tasks := make([]join.Task[int], n)
	for i := range tasks {
		i := i
		tasks[i] = func(ctx context.Context) (int, error) {
			time.Sleep(50 * time.Millisecond)
			return i, nil
		}
	}

	start := time.Now()
	results, err := join.All(context.Background(), tasks...)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	elapsed := time.Since(start)

	if elapsed > 90*time.Millisecond {
		t.Fatalf("tasks appear sequential: took %v", elapsed)
	}
	if len(results) != n {
		t.Fatalf("got %d results, want %d", len(results), n)
	}
}

// --- No goroutine leaks ---

func TestJoin2_NoLeakOnError(t *testing.T) {
	done := make(chan struct{})
	_, _, err := join.Join2(context.Background(),
		func(ctx context.Context) (int, error) { return 0, errA },
		func(ctx context.Context) (string, error) {
			defer close(done)
			<-ctx.Done()
			return "", ctx.Err()
		},
	)
	if err == nil {
		t.Fatal("expected error")
	}
	select {
	case <-done:
		// Second goroutine completed, no leak
	case <-time.After(time.Second):
		t.Fatal("goroutine leaked: second task did not complete")
	}
}

// --- Struct return types ---

func TestJoin2_StructTypes(t *testing.T) {
	type User struct {
		Name string
	}
	type Post struct {
		Title string
	}

	user, posts, err := join.Join2(context.Background(),
		func(ctx context.Context) (*User, error) {
			return &User{Name: "alice"}, nil
		},
		func(ctx context.Context) ([]*Post, error) {
			return []*Post{{Title: "hello"}}, nil
		},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if user.Name != "alice" {
		t.Fatalf("user.Name: got %q, want %q", user.Name, "alice")
	}
	if len(posts) != 1 || posts[0].Title != "hello" {
		t.Fatalf("unexpected posts: %v", posts)
	}
}
