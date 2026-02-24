// Package join provides type-safe parallel execution of functions using Go generics.
//
// It is designed as a more ergonomic alternative to [golang.org/x/sync/errgroup]
// for common parallel execution patterns. Instead of requiring callers to
// pre-declare result variables and wire them through closures, join returns
// typed results directly.
//
// For parallel execution of tasks with different return types, use [Join1],
// [Join2], [Join3], [Join4], or [Join5]:
//
//	user, posts, err := join.Join2(ctx,
//	    func(ctx context.Context) (*User, error) { return fetchUser(ctx, id) },
//	    func(ctx context.Context) ([]*Post, error) { return fetchPosts(ctx, id) },
//	)
//
// For parallel execution of tasks with the same return type, use [All]:
//
//	items, err := join.All(ctx, tasks...)
//
// For fire-and-forget parallel execution where you only care about errors, use [JoinAll]:
//
//	err := join.JoinAll(ctx, doA, doB, doC)
//
// All functions cancel remaining tasks on the first error (fail-fast).
package join

import (
	"context"
	"fmt"
	"sync"
)

// Task represents a unit of work that returns a typed result.
type Task[T any] func(ctx context.Context) (T, error)

// None is a zero-size type for tasks that have no meaningful return value.
// Use it with Join1-Join5 when a task only produces an error:
//
//	user, _, err := join.Join2(ctx,
//	    func(ctx context.Context) (*User, error) { return fetchUser(ctx, id) },
//	    join.NoValue(func(ctx context.Context) error { return syncCache(ctx) }),
//	)
type None struct{}

// NoValue wraps an error-only function into a [Task][None], for use with
// Join1-Join5 when a task has no return value.
func NoValue(fn func(context.Context) error) Task[None] {
	return func(ctx context.Context) (None, error) {
		return None{}, fn(ctx)
	}
}

// runner coordinates parallel task execution with fail-fast semantics.
type runner struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	once   sync.Once
	err    error
}

func newRunner(ctx context.Context, n int) *runner {
	ctx, cancel := context.WithCancel(ctx)
	r := &runner{ctx: ctx, cancel: cancel}
	r.wg.Add(n)
	return r
}

func (r *runner) setErr(err error) {
	r.once.Do(func() {
		r.err = err
		r.cancel()
	})
}

func (r *runner) wait() error {
	r.wg.Wait()
	return r.err
}

func run[T any](r *runner, task Task[T], dst *T) {
	go func() {
		defer r.wg.Done()
		defer func() {
			if rec := recover(); rec != nil {
				r.setErr(fmt.Errorf("join: panic: %v", rec))
			}
		}()
		v, err := task(r.ctx)
		if err != nil {
			r.setErr(err)
			return
		}
		*dst = v
	}()
}

// Join1 executes a single task. This is useful as a building block or when
// you want consistent syntax with Join2, Join3, etc.
func Join1[A any](ctx context.Context, fa Task[A]) (A, error) {
	r := newRunner(ctx, 1)
	defer r.cancel()

	var a A
	run(r, fa, &a)

	err := r.wait()
	return a, err
}

// Join2 executes two tasks of potentially different types in parallel.
func Join2[A, B any](ctx context.Context, fa Task[A], fb Task[B]) (A, B, error) {
	r := newRunner(ctx, 2)
	defer r.cancel()

	var a A
	var b B
	run(r, fa, &a)
	run(r, fb, &b)

	err := r.wait()
	return a, b, err
}

// Join3 executes three tasks of potentially different types in parallel.
func Join3[A, B, C any](ctx context.Context, fa Task[A], fb Task[B], fc Task[C]) (A, B, C, error) {
	r := newRunner(ctx, 3)
	defer r.cancel()

	var a A
	var b B
	var c C
	run(r, fa, &a)
	run(r, fb, &b)
	run(r, fc, &c)

	err := r.wait()
	return a, b, c, err
}

// Join4 executes four tasks of potentially different types in parallel.
func Join4[A, B, C, D any](
	ctx context.Context,
	fa Task[A], fb Task[B], fc Task[C], fd Task[D],
) (A, B, C, D, error) {
	r := newRunner(ctx, 4)
	defer r.cancel()

	var a A
	var b B
	var c C
	var d D
	run(r, fa, &a)
	run(r, fb, &b)
	run(r, fc, &c)
	run(r, fd, &d)

	err := r.wait()
	return a, b, c, d, err
}

// Join5 executes five tasks of potentially different types in parallel.
func Join5[A, B, C, D, E any](
	ctx context.Context,
	fa Task[A], fb Task[B], fc Task[C], fd Task[D], fe Task[E],
) (A, B, C, D, E, error) {
	r := newRunner(ctx, 5)
	defer r.cancel()

	var a A
	var b B
	var c C
	var d D
	var e E
	run(r, fa, &a)
	run(r, fb, &b)
	run(r, fc, &c)
	run(r, fd, &d)
	run(r, fe, &e)

	err := r.wait()
	return a, b, c, d, e, err
}

// JoinAll executes all tasks in parallel and returns the first error.
// Unlike the typed Join functions, tasks here have no return value beyond error,
// making this suitable for variadic use with any number of tasks.
func JoinAll(ctx context.Context, tasks ...func(ctx context.Context) error) error {
	if len(tasks) == 0 {
		return nil
	}

	r := newRunner(ctx, len(tasks))
	defer r.cancel()

	for _, task := range tasks {
		go func() {
			defer r.wg.Done()
			defer func() {
				if rec := recover(); rec != nil {
					r.setErr(fmt.Errorf("join: panic: %v", rec))
				}
			}()
			if err := task(r.ctx); err != nil {
				r.setErr(err)
			}
		}()
	}

	return r.wait()
}

// Map applies fn to each element of inputs in parallel and returns the results
// in the same order. If any invocation fails, the context is cancelled and the
// first error is returned.
//
//	users, err := join.Map(ctx, userIDs, func(ctx context.Context, id int) (*User, error) {
//	    return fetchUser(ctx, id)
//	})
func Map[In, Out any](ctx context.Context, inputs []In, fn func(context.Context, In) (Out, error)) ([]Out, error) {
	if len(inputs) == 0 {
		return []Out{}, nil
	}

	r := newRunner(ctx, len(inputs))
	defer r.cancel()

	results := make([]Out, len(inputs))
	for i, in := range inputs {
		run(r, func(ctx context.Context) (Out, error) {
			return fn(ctx, in)
		}, &results[i])
	}

	if err := r.wait(); err != nil {
		return nil, err
	}
	return results, nil
}

// ForEach applies fn to each element of inputs in parallel and returns the
// first error. If any invocation fails, the context is cancelled for the rest.
//
//	err := join.ForEach(ctx, filePaths, func(ctx context.Context, path string) error {
//	    return processFile(ctx, path)
//	})
func ForEach[T any](ctx context.Context, inputs []T, fn func(context.Context, T) error) error {
	if len(inputs) == 0 {
		return nil
	}

	r := newRunner(ctx, len(inputs))
	defer r.cancel()

	for _, in := range inputs {
		go func() {
			defer r.wg.Done()
			defer func() {
				if rec := recover(); rec != nil {
					r.setErr(fmt.Errorf("join: panic: %v", rec))
				}
			}()
			if err := fn(r.ctx, in); err != nil {
				r.setErr(err)
			}
		}()
	}

	return r.wait()
}

// All executes all tasks of the same type in parallel and returns their results
// in the same order as the input tasks. If any task fails, the context is
// cancelled and the first error is returned.
func All[T any](ctx context.Context, tasks ...Task[T]) ([]T, error) {
	if len(tasks) == 0 {
		return []T{}, nil
	}

	r := newRunner(ctx, len(tasks))
	defer r.cancel()

	results := make([]T, len(tasks))
	for i, task := range tasks {
		run(r, task, &results[i])
	}

	if err := r.wait(); err != nil {
		return nil, err
	}
	return results, nil
}
