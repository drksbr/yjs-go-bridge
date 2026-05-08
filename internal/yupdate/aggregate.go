package yupdate

import (
	"context"
	"fmt"
	"runtime"
	"sync"
)

const (
	defaultUpdateAggregateWorkers       = 5
	sequentialUpdateAggregateMaxUpdates = 4
)

type aggregateResult[T any] struct {
	index int
	value T
	err   error
}

type aggregateTask struct {
	index int
	data  []byte
}

func aggregatePayloadsInParallel[T any](
	ctx context.Context,
	updates [][]byte,
	workers int,
	extract func(context.Context, int, []byte) (T, error),
	reducer func(context.Context, []T) (T, error),
) (T, error) {
	var zero T
	if extract == nil {
		return zero, fmt.Errorf("extractor nao fornecido")
	}
	if reducer == nil {
		return zero, fmt.Errorf("reducer nao fornecido")
	}
	if len(updates) == 0 {
		return reducer(ctx, make([]T, 0))
	}
	if ctx == nil {
		ctx = context.Background()
	}

	workerCount := defaultUpdateAggregateWorkers
	if workers > 0 {
		workerCount = workers
	}
	workerCount = resolveWorkerCount(workerCount, len(updates))
	if workerCount == 0 {
		return zero, nil
	}
	if shouldAggregateSequentially(workerCount, len(updates)) {
		return aggregatePayloadsSequentially(ctx, updates, extract, reducer)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan aggregateTask, len(updates))
	results := make(chan aggregateResult[T], len(updates))
	for index, update := range updates {
		jobs <- aggregateTask{
			index: index,
			data:  update,
		}
	}
	close(jobs)

	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range jobs {
				if ctx.Err() != nil {
					return
				}

				value, err := runUpdateTaskSafely(ctx, task.index, task.data, extract)
				select {
				case results <- aggregateResult[T]{index: task.index, value: value, err: err}:
					if err != nil {
						cancel()
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	entries := make([]T, len(updates))
	errs := make([]error, len(updates))
	for result := range results {
		entries[result.index] = result.value
		errs[result.index] = result.err
	}

	for _, err := range errs {
		if err != nil {
			return zero, err
		}
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}

	return reducer(ctx, entries)
}

func shouldAggregateSequentially(workerCount int, updatesCount int) bool {
	return workerCount == 1 || updatesCount <= sequentialUpdateAggregateMaxUpdates
}

func aggregatePayloadsSequentially[T any](
	ctx context.Context,
	updates [][]byte,
	extract func(context.Context, int, []byte) (T, error),
	reducer func(context.Context, []T) (T, error),
) (T, error) {
	var zero T
	entries := make([]T, 0, len(updates))
	for index, update := range updates {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		value, err := runUpdateTaskSafely(ctx, index, update, extract)
		if err != nil {
			return zero, err
		}
		entries = append(entries, value)
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	return reducer(ctx, entries)
}

func resolveWorkerCount(requested int, updatesCount int) int {
	if updatesCount <= 0 {
		return 0
	}
	if requested <= 0 {
		requested = 1
	}
	if requested > updatesCount {
		requested = updatesCount
	}
	if requested > runtime.GOMAXPROCS(0) {
		requested = runtime.GOMAXPROCS(0)
	}
	if requested < 1 {
		requested = 1
	}
	return requested
}

func runUpdateTaskSafely[T any](
	ctx context.Context,
	index int,
	data []byte,
	extract func(context.Context, int, []byte) (T, error),
) (value T, err error) {
	defer func() {
		if recoverValue := recover(); recoverValue != nil {
			err = fmt.Errorf("update[%d]: panic durante processamento: %v", index, recoverValue)
		}
	}()

	value, err = extract(ctx, index, data)
	if err != nil {
		err = fmt.Errorf("update[%d]: %w", index, err)
	}
	return value, err
}
