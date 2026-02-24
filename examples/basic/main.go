package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/kjuulh/join"
	"golang.org/x/sync/errgroup"
)

func main() {
	err := Test()
	if err != nil {
		panic(err)
	}

	err = TestGroup()
	if err != nil {
		panic(err)
	}
}

func Test() error {
	ctx := context.Background()

	items := make([]string, 0)
	for i := range 100 {
		items = append(items, strconv.FormatInt(int64(i), 10))
	}

	start := time.Now()
	output, err := join.Map(
		ctx,
		items,
		func(ctx context.Context, input string) (string, error) {
			return input + "\t" + strconv.FormatInt(time.Since(start).Microseconds(), 10), nil
		},
	)
	if err != nil {
		return err
	}

	fmt.Println("items")
	for _, output := range output {
		fmt.Printf("- %s\n", output)
	}

	return nil
}

func TestGroup() error {
	ctx := context.Background()
	items := make([]string, 0)
	for i := range 100 {
		items = append(items, strconv.FormatInt(int64(i), 10))
	}

	start := time.Now()
	output := make([]string, len(items))

	g, ctx := errgroup.WithContext(ctx)
	for i, item := range items {
		g.Go(func() error {
			output[i] = item + "\t" + strconv.FormatInt(time.Since(start).Microseconds(), 10)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	fmt.Println("items")
	for _, o := range output {
		fmt.Printf("- %s\n", o)
	}
	return nil
}
