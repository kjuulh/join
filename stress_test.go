package join_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kjuulh/join"
)

// ---------- Stress tests ----------

// TestStress_MapResultCorrectness launches thousands of Map tasks and verifies
// every result lands in the correct slot. Run with -race.
func TestStress_MapResultCorrectness(t *testing.T) {
	const n = 10_000
	inputs := make([]int, n)
	for i := range inputs {
		inputs[i] = i
	}

	results, err := join.Map(context.Background(), inputs, func(ctx context.Context, v int) (int, error) {
		return v * 3, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != n {
		t.Fatalf("got %d results, want %d", len(results), n)
	}
	for i, r := range results {
		if r != i*3 {
			t.Fatalf("results[%d] = %d, want %d", i, r, i*3)
		}
	}
}

// TestStress_AllResultCorrectness verifies All preserves order with many tasks.
func TestStress_AllResultCorrectness(t *testing.T) {
	const n = 5_000
	tasks := make([]join.Task[int], n)
	for i := range tasks {
		i := i
		tasks[i] = func(ctx context.Context) (int, error) {
			return i, nil
		}
	}

	results, err := join.All(context.Background(), tasks...)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i, r := range results {
		if r != i {
			t.Fatalf("results[%d] = %d, want %d", i, r, i)
		}
	}
}

// TestStress_JoinAllCompletion ensures all goroutines finish even at scale.
func TestStress_JoinAllCompletion(t *testing.T) {
	const n = 10_000
	var count atomic.Int64
	tasks := make([]func(ctx context.Context) error, n)
	for i := range tasks {
		tasks[i] = func(ctx context.Context) error {
			count.Add(1)
			return nil
		}
	}

	err := join.JoinAll(context.Background(), tasks...)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count.Load() != n {
		t.Fatalf("only %d/%d tasks ran", count.Load(), n)
	}
}

// TestStress_ForEachCompletion is the ForEach version of the above.
func TestStress_ForEachCompletion(t *testing.T) {
	const n = 10_000
	var count atomic.Int64
	inputs := make([]int, n)
	for i := range inputs {
		inputs[i] = i
	}

	err := join.ForEach(context.Background(), inputs, func(ctx context.Context, _ int) error {
		count.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count.Load() != n {
		t.Fatalf("only %d/%d tasks ran", count.Load(), n)
	}
}

// TestStress_FirstErrorWins runs many tasks that all fail and verifies
// exactly one error is returned (not a mix or corruption).
func TestStress_FirstErrorWins(t *testing.T) {
	const n = 1_000
	sentinels := make([]error, n)
	for i := range sentinels {
		sentinels[i] = fmt.Errorf("err-%d", i)
	}

	tasks := make([]func(ctx context.Context) error, n)
	for i := range tasks {
		i := i
		tasks[i] = func(ctx context.Context) error {
			return sentinels[i]
		}
	}

	err := join.JoinAll(context.Background(), tasks...)
	if err == nil {
		t.Fatal("expected error")
	}

	// The returned error must be exactly one of the sentinels.
	found := false
	for _, s := range sentinels {
		if errors.Is(err, s) {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("returned error %v does not match any sentinel", err)
	}
}

// TestStress_MapFirstErrorWins does the same for Map.
func TestStress_MapFirstErrorWins(t *testing.T) {
	const n = 1_000
	inputs := make([]int, n)
	for i := range inputs {
		inputs[i] = i
	}

	_, err := join.Map(context.Background(), inputs, func(ctx context.Context, v int) (int, error) {
		return 0, fmt.Errorf("err-%d", v)
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.HasPrefix(err.Error(), "err-") {
		t.Fatalf("unexpected error format: %v", err)
	}
}

// TestStress_ConcurrentPanics fires many panicking tasks and ensures
// the library doesn't deadlock or crash.
func TestStress_ConcurrentPanics(t *testing.T) {
	const n = 500
	tasks := make([]func(ctx context.Context) error, n)
	for i := range tasks {
		i := i
		tasks[i] = func(ctx context.Context) error {
			panic(fmt.Sprintf("panic-%d", i))
		}
	}

	err := join.JoinAll(context.Background(), tasks...)
	if err == nil {
		t.Fatal("expected error from panics")
	}
	if !strings.Contains(err.Error(), "panic") {
		t.Fatalf("error should mention panic: %v", err)
	}
}

// TestStress_MapConcurrentPanics checks Map handles many panics.
func TestStress_MapConcurrentPanics(t *testing.T) {
	const n = 500
	inputs := make([]int, n)
	for i := range inputs {
		inputs[i] = i
	}

	_, err := join.Map(context.Background(), inputs, func(ctx context.Context, v int) (int, error) {
		panic(fmt.Sprintf("boom-%d", v))
	})
	if err == nil {
		t.Fatal("expected error from panics")
	}
	if !strings.Contains(err.Error(), "panic") {
		t.Fatalf("error should mention panic: %v", err)
	}
}

// TestStress_MixedPanicsAndErrors ensures panics and errors coexist safely.
func TestStress_MixedPanicsAndErrors(t *testing.T) {
	const n = 1_000
	tasks := make([]func(ctx context.Context) error, n)
	for i := range tasks {
		i := i
		tasks[i] = func(ctx context.Context) error {
			switch i % 3 {
			case 0:
				return nil
			case 1:
				return fmt.Errorf("err-%d", i)
			default:
				panic(fmt.Sprintf("panic-%d", i))
			}
		}
	}

	err := join.JoinAll(context.Background(), tasks...)
	if err == nil {
		t.Fatal("expected error")
	}
}

// TestStress_NoGoroutineLeak verifies goroutine count returns to baseline
// after many calls with errors.
func TestStress_NoGoroutineLeak(t *testing.T) {
	// Warm up the runtime.
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	for i := 0; i < 100; i++ {
		_, err := join.Map(context.Background(), []int{1, 2, 3, 4, 5}, func(ctx context.Context, v int) (int, error) {
			if v == 3 {
				return 0, errA
			}
			<-ctx.Done()
			return 0, ctx.Err()
		})
		if err == nil {
			t.Fatal("expected error")
		}
	}

	// Allow goroutines to wind down.
	time.Sleep(100 * time.Millisecond)
	runtime.GC()
	time.Sleep(50 * time.Millisecond)

	after := runtime.NumGoroutine()
	// Allow some slack for runtime goroutines.
	if after > baseline+10 {
		t.Fatalf("goroutine leak: before=%d, after=%d", baseline, after)
	}
}

// TestStress_RapidContextCancellation cancels contexts quickly and
// ensures no deadlocks.
func TestStress_RapidContextCancellation(t *testing.T) {
	for i := 0; i < 500; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		// Cancel immediately — tasks should observe it.
		cancel()

		_, err := join.Map(context.Background(), []int{1, 2, 3}, func(innerCtx context.Context, v int) (int, error) {
			// Use the outer cancelled ctx by checking context.
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			default:
				return v, nil
			}
		})
		// Either succeeds or errors, but must not hang.
		_ = err
	}
}

// TestStress_Join2RepeatedCalls calls Join2 in a tight loop to detect
// data races or state leaking between calls.
func TestStress_Join2RepeatedCalls(t *testing.T) {
	for i := 0; i < 5_000; i++ {
		i := i
		a, b, err := join.Join2(context.Background(),
			func(ctx context.Context) (int, error) { return i, nil },
			func(ctx context.Context) (string, error) { return fmt.Sprintf("v%d", i), nil },
		)
		if err != nil {
			t.Fatalf("iteration %d: unexpected error: %v", i, err)
		}
		if a != i {
			t.Fatalf("iteration %d: a = %d, want %d", i, a, i)
		}
		want := fmt.Sprintf("v%d", i)
		if b != want {
			t.Fatalf("iteration %d: b = %q, want %q", i, b, want)
		}
	}
}

// TestStress_CancellationCompletesAllTasks verifies that on error,
// all tasks still run to completion (don't leak).
func TestStress_CancellationCompletesAllTasks(t *testing.T) {
	const n = 200
	var completed atomic.Int64

	inputs := make([]int, n)
	for i := range inputs {
		inputs[i] = i
	}

	_ = join.ForEach(context.Background(), inputs, func(ctx context.Context, v int) error {
		defer completed.Add(1)
		if v == 0 {
			return errA
		}
		// Simulate work — check context.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
			return nil
		}
	})

	// Wait briefly for all goroutines to finish.
	time.Sleep(100 * time.Millisecond)
	if completed.Load() != n {
		t.Fatalf("only %d/%d tasks completed", completed.Load(), n)
	}
}

// TestStress_MapPointerResults tests that pointer results aren't corrupted
// under high concurrency (each slot gets its own distinct allocation).
func TestStress_MapPointerResults(t *testing.T) {
	const n = 5_000
	type entry struct {
		ID    int
		Value string
	}
	inputs := make([]int, n)
	for i := range inputs {
		inputs[i] = i
	}

	results, err := join.Map(context.Background(), inputs, func(ctx context.Context, id int) (*entry, error) {
		return &entry{ID: id, Value: fmt.Sprintf("v%d", id)}, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i, r := range results {
		if r.ID != i {
			t.Fatalf("results[%d].ID = %d, want %d", i, r.ID, i)
		}
		want := fmt.Sprintf("v%d", i)
		if r.Value != want {
			t.Fatalf("results[%d].Value = %q, want %q", i, r.Value, want)
		}
	}
}

// TestStress_Join5Rapid calls Join5 in a tight loop with varying error positions.
func TestStress_Join5Rapid(t *testing.T) {
	for i := 0; i < 1_000; i++ {
		errPos := i % 6 // 0-4 = error at that position, 5 = no error
		a, b, c, d, e, err := join.Join5(context.Background(),
			func(ctx context.Context) (int, error) {
				if errPos == 0 {
					return 0, errA
				}
				return 1, nil
			},
			func(ctx context.Context) (int, error) {
				if errPos == 1 {
					return 0, errA
				}
				return 2, nil
			},
			func(ctx context.Context) (int, error) {
				if errPos == 2 {
					return 0, errA
				}
				return 3, nil
			},
			func(ctx context.Context) (int, error) {
				if errPos == 3 {
					return 0, errA
				}
				return 4, nil
			},
			func(ctx context.Context) (int, error) {
				if errPos == 4 {
					return 0, errA
				}
				return 5, nil
			},
		)
		if errPos < 5 {
			if !errors.Is(err, errA) {
				t.Fatalf("iteration %d (errPos=%d): expected errA, got %v", i, errPos, err)
			}
		} else {
			if err != nil {
				t.Fatalf("iteration %d: unexpected error: %v", i, err)
			}
			if a != 1 || b != 2 || c != 3 || d != 4 || e != 5 {
				t.Fatalf("iteration %d: wrong results: %d %d %d %d %d", i, a, b, c, d, e)
			}
		}
	}
}

// TestStress_MapWithRandomDelays adds jitter to surface ordering issues.
func TestStress_MapWithRandomDelays(t *testing.T) {
	const n = 500
	inputs := make([]int, n)
	for i := range inputs {
		inputs[i] = i
	}

	results, err := join.Map(context.Background(), inputs, func(ctx context.Context, v int) (int, error) {
		// Random jitter 0-100µs to vary scheduling order.
		time.Sleep(time.Duration(rand.IntN(100)) * time.Microsecond)
		return v * 2, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i, r := range results {
		if r != i*2 {
			t.Fatalf("results[%d] = %d, want %d", i, r, i*2)
		}
	}
}

// TestStress_AllWithRandomErrors randomly fails ~10% of tasks over many
// iterations and verifies the returned error is always from the set.
func TestStress_AllWithRandomErrors(t *testing.T) {
	for iter := 0; iter < 200; iter++ {
		const n = 100
		errSet := make(map[string]bool)
		tasks := make([]join.Task[int], n)
		hasError := false

		for i := range tasks {
			i := i
			shouldFail := rand.IntN(10) == 0
			if shouldFail {
				hasError = true
				errMsg := fmt.Sprintf("fail-%d", i)
				errSet[errMsg] = true
				tasks[i] = func(ctx context.Context) (int, error) {
					return 0, errors.New(errMsg)
				}
			} else {
				tasks[i] = func(ctx context.Context) (int, error) {
					return i, nil
				}
			}
		}

		results, err := join.All(context.Background(), tasks...)
		if hasError {
			if err == nil {
				t.Fatalf("iter %d: expected error", iter)
			}
			if !errSet[err.Error()] {
				t.Fatalf("iter %d: unexpected error %v", iter, err)
			}
		} else {
			if err != nil {
				t.Fatalf("iter %d: unexpected error: %v", iter, err)
			}
			if len(results) != n {
				t.Fatalf("iter %d: got %d results, want %d", iter, len(results), n)
			}
		}
	}
}

// ---------- High-scale stress tests ----------
//
// These tests launch 100k–1M goroutines. Run without -race for the 1M tests
// (race detector adds ~10x memory overhead per goroutine). 100k with race is fine.

func TestStress_100k_MapCorrectness(t *testing.T) {
	const n = 100_000
	inputs := make([]int, n)
	for i := range inputs {
		inputs[i] = i
	}

	results, err := join.Map(context.Background(), inputs, func(ctx context.Context, v int) (int, error) {
		return v * 3, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != n {
		t.Fatalf("got %d results, want %d", len(results), n)
	}
	for i, r := range results {
		if r != i*3 {
			t.Fatalf("results[%d] = %d, want %d", i, r, i*3)
		}
	}
}

func TestStress_100k_JoinAllCompletion(t *testing.T) {
	const n = 100_000
	var count atomic.Int64
	tasks := make([]func(ctx context.Context) error, n)
	for i := range tasks {
		tasks[i] = func(ctx context.Context) error {
			count.Add(1)
			return nil
		}
	}

	err := join.JoinAll(context.Background(), tasks...)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count.Load() != n {
		t.Fatalf("only %d/%d tasks ran", count.Load(), n)
	}
}

func TestStress_100k_ForEachCompletion(t *testing.T) {
	const n = 100_000
	var count atomic.Int64
	inputs := make([]int, n)
	for i := range inputs {
		inputs[i] = i
	}

	err := join.ForEach(context.Background(), inputs, func(ctx context.Context, _ int) error {
		count.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count.Load() != n {
		t.Fatalf("only %d/%d tasks ran", count.Load(), n)
	}
}

func TestStress_100k_AllCorrectness(t *testing.T) {
	const n = 100_000
	tasks := make([]join.Task[int], n)
	for i := range tasks {
		i := i
		tasks[i] = func(ctx context.Context) (int, error) {
			return i, nil
		}
	}

	results, err := join.All(context.Background(), tasks...)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i, r := range results {
		if r != i {
			t.Fatalf("results[%d] = %d, want %d", i, r, i)
		}
	}
}

// TestStress_100k_AllErrors: 100k goroutines all returning errors — first-error-wins
// under extreme contention on the sync.Once.
func TestStress_100k_AllErrors(t *testing.T) {
	const n = 100_000
	tasks := make([]func(ctx context.Context) error, n)
	for i := range tasks {
		i := i
		tasks[i] = func(ctx context.Context) error {
			return fmt.Errorf("err-%d", i)
		}
	}

	err := join.JoinAll(context.Background(), tasks...)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.HasPrefix(err.Error(), "err-") {
		t.Fatalf("unexpected error format: %v", err)
	}
}

// TestStress_100k_MixedPanicsAndErrors: ~33k panics, ~33k errors, ~33k success.
func TestStress_100k_MixedPanicsAndErrors(t *testing.T) {
	const n = 100_000
	var completed atomic.Int64
	tasks := make([]func(ctx context.Context) error, n)
	for i := range tasks {
		i := i
		tasks[i] = func(ctx context.Context) error {
			defer completed.Add(1)
			switch i % 3 {
			case 0:
				return nil
			case 1:
				return fmt.Errorf("err-%d", i)
			default:
				panic(fmt.Sprintf("panic-%d", i))
			}
		}
	}

	err := join.JoinAll(context.Background(), tasks...)
	if err == nil {
		t.Fatal("expected error")
	}

	// All 100k goroutines must have completed.
	time.Sleep(200 * time.Millisecond)
	if completed.Load() != n {
		t.Fatalf("only %d/%d tasks completed", completed.Load(), n)
	}
}

// TestStress_100k_MapPointerResults: 100k distinct heap allocations returned
// through the results slice — no aliasing or corruption.
func TestStress_100k_MapPointerResults(t *testing.T) {
	const n = 100_000
	type entry struct{ ID int }
	inputs := make([]int, n)
	for i := range inputs {
		inputs[i] = i
	}

	results, err := join.Map(context.Background(), inputs, func(ctx context.Context, id int) (*entry, error) {
		return &entry{ID: id}, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i, r := range results {
		if r.ID != i {
			t.Fatalf("results[%d].ID = %d, want %d", i, r.ID, i)
		}
	}
}

// TestStress_100k_ErrorCancellation: one task errors early, the other 99,999
// must all observe cancellation and complete.
func TestStress_100k_ErrorCancellation(t *testing.T) {
	const n = 100_000
	var completed atomic.Int64

	inputs := make([]int, n)
	for i := range inputs {
		inputs[i] = i
	}

	err := join.ForEach(context.Background(), inputs, func(ctx context.Context, v int) error {
		defer completed.Add(1)
		if v == 0 {
			return errA
		}
		<-ctx.Done()
		return ctx.Err()
	})

	if !errors.Is(err, errA) {
		t.Fatalf("got %v, want %v", err, errA)
	}

	// All goroutines must have finished — ForEach waits internally.
	if completed.Load() != n {
		t.Fatalf("only %d/%d tasks completed", completed.Load(), n)
	}
}

// 1M goroutine tests — skip under -race and -short due to memory/time cost.

func TestStress_1M_JoinAllCompletion(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 1M goroutine test in short mode")
	}

	const n = 1_000_000
	var count atomic.Int64
	tasks := make([]func(ctx context.Context) error, n)
	for i := range tasks {
		tasks[i] = func(ctx context.Context) error {
			count.Add(1)
			return nil
		}
	}

	err := join.JoinAll(context.Background(), tasks...)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count.Load() != n {
		t.Fatalf("only %d/%d tasks ran", count.Load(), n)
	}
}

func TestStress_1M_ForEachCompletion(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 1M goroutine test in short mode")
	}

	const n = 1_000_000
	var count atomic.Int64
	inputs := make([]int, n)
	for i := range inputs {
		inputs[i] = i
	}

	err := join.ForEach(context.Background(), inputs, func(ctx context.Context, _ int) error {
		count.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count.Load() != n {
		t.Fatalf("only %d/%d tasks ran", count.Load(), n)
	}
}

func TestStress_1M_MapCorrectness(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 1M goroutine test in short mode")
	}

	const n = 1_000_000
	inputs := make([]int, n)
	for i := range inputs {
		inputs[i] = i
	}

	results, err := join.Map(context.Background(), inputs, func(ctx context.Context, v int) (int, error) {
		return v * 3, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != n {
		t.Fatalf("got %d results, want %d", len(results), n)
	}
	for i, r := range results {
		if r != i*3 {
			t.Fatalf("results[%d] = %d, want %d", i, r, i*3)
		}
	}
}

func TestStress_1M_AllCorrectness(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 1M goroutine test in short mode")
	}

	const n = 1_000_000
	tasks := make([]join.Task[int], n)
	for i := range tasks {
		i := i
		tasks[i] = func(ctx context.Context) (int, error) {
			return i, nil
		}
	}

	results, err := join.All(context.Background(), tasks...)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i, r := range results {
		if r != i {
			t.Fatalf("results[%d] = %d, want %d", i, r, i)
		}
	}
}

func TestStress_1M_AllErrors(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 1M goroutine test in short mode")
	}

	const n = 1_000_000
	var completed atomic.Int64
	tasks := make([]func(ctx context.Context) error, n)
	for i := range tasks {
		tasks[i] = func(ctx context.Context) error {
			defer completed.Add(1)
			return fmt.Errorf("err")
		}
	}

	err := join.JoinAll(context.Background(), tasks...)
	if err == nil {
		t.Fatal("expected error")
	}
	if completed.Load() != n {
		t.Fatalf("only %d/%d tasks completed", completed.Load(), n)
	}
}

func TestStress_1M_ErrorCancellation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 1M goroutine test in short mode")
	}

	const n = 1_000_000
	var completed atomic.Int64

	inputs := make([]int, n)
	for i := range inputs {
		inputs[i] = i
	}

	err := join.ForEach(context.Background(), inputs, func(ctx context.Context, v int) error {
		defer completed.Add(1)
		if v == 0 {
			return errA
		}
		<-ctx.Done()
		return ctx.Err()
	})
	if !errors.Is(err, errA) {
		t.Fatalf("got %v, want %v", err, errA)
	}
	if completed.Load() != n {
		t.Fatalf("only %d/%d tasks completed", completed.Load(), n)
	}
}

// ---------- Fuzz tests ----------

// FuzzMap fuzzes Map with varying task count, an error injection position,
// and a panic injection position.
func FuzzMap(f *testing.F) {
	f.Add(0, -1, -1)    // empty
	f.Add(1, -1, -1)    // single, no error
	f.Add(1, 0, -1)     // single, errors
	f.Add(5, 2, -1)     // error at middle
	f.Add(5, -1, 3)     // panic at index 3
	f.Add(10, 0, -1)    // error at start
	f.Add(10, 9, -1)    // error at end
	f.Add(100, 50, -1)  // large, error in middle
	f.Add(100, -1, -1)  // large, no error
	f.Add(50, 10, 20)   // both error and panic

	f.Fuzz(func(t *testing.T, n int, errIdx int, panicIdx int) {
		if n < 0 || n > 5_000 {
			return
		}
		inputs := make([]int, n)
		for i := range inputs {
			inputs[i] = i
		}

		expectError := (errIdx >= 0 && errIdx < n) || (panicIdx >= 0 && panicIdx < n)

		results, err := join.Map(context.Background(), inputs, func(ctx context.Context, v int) (int, error) {
			if v == panicIdx {
				panic(fmt.Sprintf("fuzz-panic-%d", v))
			}
			if v == errIdx {
				return 0, fmt.Errorf("fuzz-err-%d", v)
			}
			return v * 7, nil
		})

		if expectError {
			if err == nil {
				t.Fatalf("n=%d errIdx=%d panicIdx=%d: expected error", n, errIdx, panicIdx)
			}
			if results != nil {
				t.Fatalf("expected nil results on error")
			}
		} else {
			if err != nil {
				t.Fatalf("n=%d: unexpected error: %v", n, err)
			}
			if len(results) != n {
				t.Fatalf("got %d results, want %d", len(results), n)
			}
			for i, r := range results {
				if r != i*7 {
					t.Fatalf("results[%d] = %d, want %d", i, r, i*7)
				}
			}
		}
	})
}

// FuzzJoinAll fuzzes JoinAll with varying task count, error positions, and panic positions.
func FuzzJoinAll(f *testing.F) {
	f.Add(0, -1, -1)
	f.Add(1, 0, -1)
	f.Add(1, -1, 0)
	f.Add(5, 2, -1)
	f.Add(10, -1, -1)
	f.Add(100, 50, -1)
	f.Add(50, -1, 25)
	f.Add(20, 5, 15)

	f.Fuzz(func(t *testing.T, n int, errIdx int, panicIdx int) {
		if n < 0 || n > 5_000 {
			return
		}

		var completed atomic.Int64
		expectError := (errIdx >= 0 && errIdx < n) || (panicIdx >= 0 && panicIdx < n)

		tasks := make([]func(ctx context.Context) error, n)
		for i := range tasks {
			i := i
			tasks[i] = func(ctx context.Context) error {
				defer completed.Add(1)
				if i == panicIdx {
					panic(fmt.Sprintf("fuzz-panic-%d", i))
				}
				if i == errIdx {
					return fmt.Errorf("fuzz-err-%d", i)
				}
				// Respect cancellation for clean shutdown.
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					return nil
				}
			}
		}

		err := join.JoinAll(context.Background(), tasks...)

		if expectError {
			if err == nil {
				t.Fatalf("n=%d errIdx=%d panicIdx=%d: expected error", n, errIdx, panicIdx)
			}
		} else {
			if err != nil {
				t.Fatalf("n=%d: unexpected error: %v", n, err)
			}
		}

		// All tasks must have completed (no leaked goroutines).
		time.Sleep(10 * time.Millisecond)
		if completed.Load() != int64(n) {
			t.Fatalf("only %d/%d tasks completed", completed.Load(), n)
		}
	})
}

// FuzzForEach fuzzes ForEach similarly.
func FuzzForEach(f *testing.F) {
	f.Add(0, -1, -1)
	f.Add(1, 0, -1)
	f.Add(5, -1, 2)
	f.Add(10, 5, -1)
	f.Add(100, -1, -1)
	f.Add(50, 25, 30)

	f.Fuzz(func(t *testing.T, n int, errIdx int, panicIdx int) {
		if n < 0 || n > 5_000 {
			return
		}

		inputs := make([]int, n)
		for i := range inputs {
			inputs[i] = i
		}

		var completed atomic.Int64
		expectError := (errIdx >= 0 && errIdx < n) || (panicIdx >= 0 && panicIdx < n)

		err := join.ForEach(context.Background(), inputs, func(ctx context.Context, v int) error {
			defer completed.Add(1)
			if v == panicIdx {
				panic(fmt.Sprintf("fuzz-panic-%d", v))
			}
			if v == errIdx {
				return fmt.Errorf("fuzz-err-%d", v)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				return nil
			}
		})

		if expectError {
			if err == nil {
				t.Fatalf("n=%d errIdx=%d panicIdx=%d: expected error", n, errIdx, panicIdx)
			}
		} else {
			if err != nil {
				t.Fatalf("n=%d: unexpected error: %v", n, err)
			}
		}

		time.Sleep(10 * time.Millisecond)
		if completed.Load() != int64(n) {
			t.Fatalf("only %d/%d tasks completed", completed.Load(), n)
		}
	})
}

// FuzzAll fuzzes All with varying task count and error position.
func FuzzAll(f *testing.F) {
	f.Add(0, -1)
	f.Add(1, 0)
	f.Add(1, -1)
	f.Add(5, 2)
	f.Add(10, -1)
	f.Add(100, 99)

	f.Fuzz(func(t *testing.T, n int, errIdx int) {
		if n < 0 || n > 5_000 {
			return
		}

		tasks := make([]join.Task[int], n)
		for i := range tasks {
			i := i
			tasks[i] = func(ctx context.Context) (int, error) {
				if i == errIdx {
					return 0, fmt.Errorf("fuzz-err-%d", i)
				}
				return i, nil
			}
		}

		expectError := errIdx >= 0 && errIdx < n
		results, err := join.All(context.Background(), tasks...)

		if expectError {
			if err == nil {
				t.Fatalf("n=%d errIdx=%d: expected error", n, errIdx)
			}
			if results != nil {
				t.Fatal("expected nil results on error")
			}
		} else {
			if err != nil {
				t.Fatalf("n=%d: unexpected error: %v", n, err)
			}
			if len(results) != n {
				t.Fatalf("got %d results, want %d", len(results), n)
			}
			for i, r := range results {
				if r != i {
					t.Fatalf("results[%d] = %d, want %d", i, r, i)
				}
			}
		}
	})
}
