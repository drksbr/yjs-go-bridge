package yupdate

import (
	"context"
	"errors"
	"testing"
)

func TestAggregatePayloadsInParallelReturnsContextCanceledWithoutReducer(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reducerCalled := false
	got, err := aggregatePayloadsInParallel(
		ctx,
		[][]byte{{0x01}, {0x02}},
		1,
		func(ctx context.Context, index int, _ []byte) (int, error) {
			if index == 0 {
				cancel()
				return 1, nil
			}
			<-ctx.Done()
			return 0, ctx.Err()
		},
		func(context.Context, []int) (int, error) {
			reducerCalled = true
			return 99, nil
		},
	)
	if got != 0 {
		t.Fatalf("aggregatePayloadsInParallel() value = %d, want 0 on cancel", got)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("aggregatePayloadsInParallel() error = %v, want context.Canceled", err)
	}
	if reducerCalled {
		t.Fatal("aggregatePayloadsInParallel() called reducer after cancellation")
	}
}

func TestAggregatePayloadsInParallelUsesSequentialFastPathForSmallBatch(t *testing.T) {
	t.Parallel()

	var extracted []int
	got, err := aggregatePayloadsInParallel(
		context.Background(),
		[][]byte{{0x01}, {0x02}, {0x03}},
		0,
		func(_ context.Context, index int, payload []byte) (int, error) {
			if len(payload) != 1 {
				t.Fatalf("payload[%d] len = %d, want 1", index, len(payload))
			}
			extracted = append(extracted, index)
			return int(payload[0]), nil
		},
		func(_ context.Context, values []int) (int, error) {
			total := 0
			for _, value := range values {
				total += value
			}
			return total, nil
		},
	)
	if err != nil {
		t.Fatalf("aggregatePayloadsInParallel() unexpected error: %v", err)
	}
	if got != 6 {
		t.Fatalf("aggregatePayloadsInParallel() value = %d, want 6", got)
	}
	if want := []int{0, 1, 2}; !equalIntSlices(extracted, want) {
		t.Fatalf("extract order = %v, want %v", extracted, want)
	}
}

func TestStateVectorFromUpdatesContextRespectsCanceledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	update := buildUpdate(
		clientBlock{
			client: 1,
			clock:  0,
			structs: []structEncoding{
				itemDeleted(rootParent("doc"), 1),
			},
		},
	)

	_, err := StateVectorFromUpdatesContext(ctx, update)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("StateVectorFromUpdatesContext() error = %v, want context.Canceled", err)
	}
}

func equalIntSlices(left, right []int) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func TestContentIDsFromUpdatesContextRespectsCanceledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	update := buildUpdate(
		clientBlock{
			client: 2,
			clock:  0,
			structs: []structEncoding{
				itemString(rootParent("doc"), "ab"),
			},
		},
	)

	_, err := ContentIDsFromUpdatesContext(ctx, update)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ContentIDsFromUpdatesContext() error = %v, want context.Canceled", err)
	}
}

func TestMergeUpdatesV1ContextRespectsCanceledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	update := buildUpdate(
		clientBlock{
			client: 3,
			clock:  0,
			structs: []structEncoding{
				itemString(rootParent("doc"), "ab"),
			},
		},
	)

	_, err := MergeUpdatesV1Context(ctx, update)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("MergeUpdatesV1Context() error = %v, want context.Canceled", err)
	}
}

func TestDiffUpdateV1ContextRespectsCanceledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	update := buildUpdate(
		clientBlock{
			client: 4,
			clock:  0,
			structs: []structEncoding{
				itemString(rootParent("doc"), "ab"),
			},
		},
	)

	_, err := DiffUpdateV1Context(ctx, update, encodeStateVectorEntry(4, 0))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("DiffUpdateV1Context() error = %v, want context.Canceled", err)
	}
}

func TestIntersectUpdateWithContentIDsV1ContextRespectsCanceledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	update := buildUpdate(
		clientBlock{
			client: 5,
			clock:  0,
			structs: []structEncoding{
				itemJSON(rootParent("doc"), `"a"`, `"b"`),
			},
		},
	)
	ids := NewContentIDs()
	_ = ids.Inserts.Add(5, 0, 1)

	_, err := IntersectUpdateWithContentIDsV1Context(ctx, update, ids)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("IntersectUpdateWithContentIDsV1Context() error = %v, want context.Canceled", err)
	}
}
