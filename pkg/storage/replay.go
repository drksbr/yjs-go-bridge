package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/drksbr/yjs-crdt-golang-server/pkg/yjsbridge"
)

var (
	// ErrNilUpdateLogStore signals that a replay helper received a nil update log store.
	ErrNilUpdateLogStore = errors.New("storage: update log store obrigatorio")
	// ErrNilSnapshotLogStore signals that a compaction helper received a nil snapshot+log store.
	ErrNilSnapshotLogStore = errors.New("storage: snapshot log store obrigatorio")
	// ErrUpdateLogKeyMismatch signals that replay observed a log record for another document key.
	ErrUpdateLogKeyMismatch = errors.New("storage: update log key inconsistente")
	// ErrUpdateLogOffsetsOutOfOrder signals that replay observed non-monotonic log offsets.
	ErrUpdateLogOffsetsOutOfOrder = errors.New("storage: update log offsets fora de ordem")
	// ErrUpdateLogEpochRegression signals that replay observed a tail whose
	// authoritative epoch regressed relative to the checkpoint or to prior log
	// records.
	ErrUpdateLogEpochRegression = errors.New("storage: update log epoch regressivo")
)

// SnapshotLogStore narrows the contracts required to persist a compacted
// snapshot and trim the corresponding update log tail.
type SnapshotLogStore interface {
	SnapshotStore
	UpdateLogStore
}

// AuthoritativeSnapshotLogStore narrows the contracts required to compact a
// snapshot and trim its update log while validating the same authority fence for
// both writes.
type AuthoritativeSnapshotLogStore interface {
	AuthoritativeSnapshotCheckpointStore
	AuthoritativeUpdateLogStore
}

// RecoveryResult materializes the result of loading a base snapshot and replaying
// the incremental update log tail for the same document.
type RecoveryResult struct {
	Snapshot          *yjsbridge.PersistedSnapshot
	Updates           []*UpdateLogRecord
	CheckpointThrough UpdateOffset
	CheckpointEpoch   uint64
	LastOffset        UpdateOffset
	LastEpoch         uint64
}

// RecoveryStateResult materializes the current document state after replaying
// a snapshot plus update-log tail, without retaining the replayed tail records.
type RecoveryStateResult struct {
	Snapshot          *yjsbridge.PersistedSnapshot
	CheckpointThrough UpdateOffset
	CheckpointEpoch   uint64
	LastOffset        UpdateOffset
	LastEpoch         uint64
	Applied           int
}

// UpdateLogReplayResult describes the snapshot rebuilt from a base cut plus a
// paginated update log tail.
//
// Through stores the highest applied offset. When no new log records are
// applied, it remains equal to the input `after`.
type UpdateLogReplayResult struct {
	Snapshot  *yjsbridge.PersistedSnapshot
	Through   UpdateOffset
	Applied   int
	LastEpoch uint64
}

// UpdateLogCompactionResult describes the outcome of compacting a snapshot plus
// its update log tail.
//
// Record stays nil when there were no new updates to persist or trim.
type UpdateLogCompactionResult struct {
	Snapshot  *yjsbridge.PersistedSnapshot
	Record    *SnapshotRecord
	Through   UpdateOffset
	Applied   int
	LastEpoch uint64
}

const defaultRecoverSnapshotStateLimit = 512

func boundedReplayLimit(limit int) int {
	if limit <= 0 {
		return defaultRecoverSnapshotStateLimit
	}
	return limit
}

// ReplaySnapshot applies an ordered set of update log records over a base
// snapshot and returns the merged persisted snapshot.
//
// `ctx == nil` is treated as `context.Background()`.
// `base == nil` is treated as an empty document.
//
// The helper keeps the record order as received. It validates individual
// records, but does not infer document ownership or reorder offsets.
func ReplaySnapshot(ctx context.Context, base *yjsbridge.PersistedSnapshot, updates ...*UpdateLogRecord) (*yjsbridge.PersistedSnapshot, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	currentUpdate, err := yjsbridge.EncodePersistedSnapshotV2(base)
	if err != nil {
		return nil, err
	}
	if len(updates) == 0 {
		return yjsbridge.DecodePersistedSnapshotV2Context(ctx, currentUpdate)
	}

	payloads := make([][]byte, 0, len(updates)+1)
	payloads = append(payloads, currentUpdate)

	for idx, record := range updates {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if record == nil {
			return nil, fmt.Errorf("%w: record %d nil", ErrInvalidUpdatePayload, idx)
		}
		if err := record.Validate(); err != nil {
			return nil, fmt.Errorf("update log record %d: %w", idx, err)
		}
		updateV2, err := updateLogRecordV2(record)
		if err != nil {
			return nil, fmt.Errorf("update log record %d: %w", idx, err)
		}
		payloads = append(payloads, updateV2)
	}

	merged, err := yjsbridge.MergeUpdatesV2Context(ctx, payloads...)
	if err != nil {
		return nil, err
	}
	return yjsbridge.DecodePersistedSnapshotV2Context(ctx, merged)
}

// ReplayUpdateLog rebuilds a persisted snapshot from a base snapshot plus the
// log tail stored strictly after `after`.
func ReplayUpdateLog(store UpdateLogStore, key DocumentKey, base *yjsbridge.PersistedSnapshot, after UpdateOffset, limit int) (*UpdateLogReplayResult, error) {
	return ReplayUpdateLogContext(context.Background(), store, key, base, after, limit)
}

// ReplayUpdateLogContext rebuilds a persisted snapshot from a base snapshot plus
// the log tail stored strictly after `after`.
//
// `ctx == nil` is treated as `context.Background()`.
//
// The helper paginates through `ListUpdates`, validates that every record
// belongs to `key`, and requires strictly increasing offsets across pages.
// `limit <= 0` is passed through to every `ListUpdates` call.
func ReplayUpdateLogContext(ctx context.Context, store UpdateLogStore, key DocumentKey, base *yjsbridge.PersistedSnapshot, after UpdateOffset, limit int) (result *UpdateLogReplayResult, err error) {
	return replayUpdateLogContext(ctx, store, key, base, after, limit, 0)
}

func replayUpdateLogContext(ctx context.Context, store UpdateLogStore, key DocumentKey, base *yjsbridge.PersistedSnapshot, after UpdateOffset, limit int, initialEpoch uint64) (result *UpdateLogReplayResult, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	start := time.Now()
	defer func() {
		applied := 0
		through := after
		lastEpoch := uint64(0)
		if result != nil {
			applied = result.Applied
			through = result.Through
			lastEpoch = result.LastEpoch
		}
		observeReplayUpdateLog(ctx, key, time.Since(start), applied, through, lastEpoch, err)
	}()
	if store == nil {
		return nil, ErrNilUpdateLogStore
	}
	if err = key.Validate(); err != nil {
		return nil, err
	}

	currentUpdate, err := yjsbridge.EncodePersistedSnapshotV2(base)
	if err != nil {
		return nil, err
	}

	result = &UpdateLogReplayResult{
		Through:   after,
		LastEpoch: initialEpoch,
	}

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		records, err := store.ListUpdates(ctx, key, result.Through, limit)
		if err != nil {
			return nil, err
		}
		if len(records) == 0 {
			break
		}

		currentUpdate, err = replayUpdateBatchContext(ctx, key, currentUpdate, result, records)
		if err != nil {
			return nil, err
		}
	}

	snapshot, err := yjsbridge.DecodePersistedSnapshotV2Context(ctx, currentUpdate)
	if err != nil {
		return nil, err
	}
	result.Snapshot = snapshot
	return result, nil
}

// RecoverSnapshot loads the known base snapshot for the document, pages through
// the update log tail after `after`, and replays the result in order.
//
// `ctx == nil` is treated as `context.Background()`.
// `snapshots == nil` is treated as no base snapshot.
// `updates == nil` returns only the loaded base snapshot.
func RecoverSnapshot(
	ctx context.Context,
	snapshots SnapshotStore,
	updates UpdateLogStore,
	key DocumentKey,
	after UpdateOffset,
	limit int,
) (result *RecoveryResult, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	start := time.Now()
	defer func() {
		updatesApplied := 0
		checkpointThrough := UpdateOffset(0)
		lastOffset := after
		lastEpoch := uint64(0)
		if result != nil {
			updatesApplied = len(result.Updates)
			checkpointThrough = result.CheckpointThrough
			lastOffset = result.LastOffset
			lastEpoch = result.LastEpoch
		}
		observeRecoverSnapshot(ctx, key, time.Since(start), updatesApplied, checkpointThrough, lastOffset, lastEpoch, err)
	}()
	if err = key.Validate(); err != nil {
		return nil, err
	}

	base, checkpointThrough, checkpointEpoch, err := loadReplayBaseSnapshot(ctx, snapshots, key)
	if err != nil {
		return nil, err
	}
	if checkpointThrough > 0 && after > 0 && after != checkpointThrough {
		return nil, fmt.Errorf("%w: snapshot through %d, after %d", ErrSnapshotCheckpointMismatch, checkpointThrough, after)
	}

	replayAfter := after
	if replayAfter < checkpointThrough {
		replayAfter = checkpointThrough
	}

	result = &RecoveryResult{
		Snapshot:          base,
		CheckpointThrough: checkpointThrough,
		CheckpointEpoch:   checkpointEpoch,
		LastOffset:        replayAfter,
		LastEpoch:         checkpointEpoch,
	}
	if updates == nil {
		return result, nil
	}

	tail, lastOffset, lastEpoch, err := listReplayTailContext(ctx, updates, key, replayAfter, limit, checkpointEpoch)
	if err != nil {
		return nil, err
	}
	if len(tail) == 0 {
		return result, nil
	}

	snapshot, err := ReplaySnapshot(ctx, base, tail...)
	if err != nil {
		return nil, err
	}

	result = &RecoveryResult{
		Snapshot:          snapshot,
		Updates:           cloneUpdateLogRecords(tail),
		CheckpointThrough: checkpointThrough,
		CheckpointEpoch:   checkpointEpoch,
		LastOffset:        lastOffset,
		LastEpoch:         lastEpoch,
	}
	return result, nil
}

// RecoverSnapshotState loads a base snapshot and replays the update-log tail
// without retaining the applied records in memory.
func RecoverSnapshotState(
	ctx context.Context,
	snapshots SnapshotStore,
	updates UpdateLogStore,
	key DocumentKey,
	after UpdateOffset,
	limit int,
) (*RecoveryStateResult, error) {
	return RecoverSnapshotStateContext(ctx, snapshots, updates, key, after, limit)
}

// RecoverSnapshotStateContext is the context-aware variant of
// RecoverSnapshotState.
//
// When `limit <= 0`, the helper uses a bounded default page size so room
// recovery does not load the entire tail in one call.
func RecoverSnapshotStateContext(
	ctx context.Context,
	snapshots SnapshotStore,
	updates UpdateLogStore,
	key DocumentKey,
	after UpdateOffset,
	limit int,
) (result *RecoveryStateResult, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	start := time.Now()
	defer func() {
		applied := 0
		checkpointThrough := UpdateOffset(0)
		lastOffset := after
		lastEpoch := uint64(0)
		if result != nil {
			applied = result.Applied
			checkpointThrough = result.CheckpointThrough
			lastOffset = result.LastOffset
			lastEpoch = result.LastEpoch
		}
		observeRecoverSnapshot(ctx, key, time.Since(start), applied, checkpointThrough, lastOffset, lastEpoch, err)
	}()
	if err = key.Validate(); err != nil {
		return nil, err
	}

	base, checkpointThrough, checkpointEpoch, err := loadReplayBaseSnapshot(ctx, snapshots, key)
	if err != nil {
		return nil, err
	}
	if checkpointThrough > 0 && after > 0 && after != checkpointThrough {
		return nil, fmt.Errorf("%w: snapshot through %d, after %d", ErrSnapshotCheckpointMismatch, checkpointThrough, after)
	}

	replayAfter := after
	if replayAfter < checkpointThrough {
		replayAfter = checkpointThrough
	}

	result = &RecoveryStateResult{
		Snapshot:          base,
		CheckpointThrough: checkpointThrough,
		CheckpointEpoch:   checkpointEpoch,
		LastOffset:        replayAfter,
		LastEpoch:         checkpointEpoch,
	}
	if updates == nil {
		return result, nil
	}
	limit = boundedReplayLimit(limit)

	replay, err := replayUpdateLogContext(ctx, updates, key, base, replayAfter, limit, checkpointEpoch)
	if err != nil {
		return nil, err
	}
	if replay == nil {
		return result, nil
	}
	result.Snapshot = replay.Snapshot
	result.LastOffset = replay.Through
	result.LastEpoch = replay.LastEpoch
	result.Applied = replay.Applied
	return result, nil
}

// CompactUpdateLog replays and persists a compacted snapshot, then trims the
// corresponding log tail through the applied high-water mark.
func CompactUpdateLog(store SnapshotLogStore, key DocumentKey, base *yjsbridge.PersistedSnapshot, after UpdateOffset, limit int) (*UpdateLogCompactionResult, error) {
	return CompactUpdateLogContext(context.Background(), store, key, base, after, limit)
}

// CompactUpdateLogContext replays and persists a compacted snapshot, then trims
// the corresponding log tail through the applied high-water mark.
//
// `ctx == nil` is treated as `context.Background()`.
//
// Save + trim are intentionally sequenced but not transactional unless the
// backing store makes them so. If trimming fails after a successful save, the
// returned result still exposes the rebuilt snapshot and saved record together
// with the error.
func CompactUpdateLogContext(ctx context.Context, store SnapshotLogStore, key DocumentKey, base *yjsbridge.PersistedSnapshot, after UpdateOffset, limit int) (result *UpdateLogCompactionResult, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	start := time.Now()
	defer func() {
		applied := 0
		through := after
		lastEpoch := uint64(0)
		if result != nil {
			applied = result.Applied
			through = result.Through
			lastEpoch = result.LastEpoch
		}
		observeCompactUpdateLog(ctx, key, time.Since(start), applied, through, lastEpoch, err)
	}()
	if store == nil {
		return nil, ErrNilSnapshotLogStore
	}

	replay, err := ReplayUpdateLogContext(ctx, store, key, base, after, boundedReplayLimit(limit))
	if err != nil {
		return nil, err
	}

	result = &UpdateLogCompactionResult{
		Snapshot:  replay.Snapshot,
		Through:   replay.Through,
		Applied:   replay.Applied,
		LastEpoch: replay.LastEpoch,
	}
	if replay.Applied == 0 {
		return result, nil
	}

	record, err := saveSnapshotCheckpoint(ctx, store, key, replay.Snapshot, replay.Through, replay.LastEpoch)
	result.Record = record
	if err != nil {
		return result, fmt.Errorf("save compacted snapshot: %w", err)
	}
	if err := store.TrimUpdates(ctx, key, replay.Through); err != nil {
		return result, fmt.Errorf("trim compacted updates through %d: %w", replay.Through, err)
	}

	return result, nil
}

// CompactUpdateLogAuthoritative replays and persists a compacted snapshot, then
// trims the corresponding log tail through the applied high-water mark while
// validating the provided authority fence for snapshot save and trim.
func CompactUpdateLogAuthoritative(
	store AuthoritativeSnapshotLogStore,
	key DocumentKey,
	base *yjsbridge.PersistedSnapshot,
	after UpdateOffset,
	limit int,
	fence AuthorityFence,
) (*UpdateLogCompactionResult, error) {
	return CompactUpdateLogAuthoritativeContext(context.Background(), store, key, base, after, limit, fence)
}

// CompactUpdateLogAuthoritativeContext is the context-aware variant of
// CompactUpdateLogAuthoritative.
//
// Replay reads are intentionally unfenced, but both mutating steps use the same
// fence. If trim fails after a successful checkpoint save, the returned result
// still exposes the rebuilt snapshot and saved record together with the error.
func CompactUpdateLogAuthoritativeContext(
	ctx context.Context,
	store AuthoritativeSnapshotLogStore,
	key DocumentKey,
	base *yjsbridge.PersistedSnapshot,
	after UpdateOffset,
	limit int,
	fence AuthorityFence,
) (result *UpdateLogCompactionResult, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	start := time.Now()
	defer func() {
		applied := 0
		through := after
		lastEpoch := uint64(0)
		if result != nil {
			applied = result.Applied
			through = result.Through
			lastEpoch = result.LastEpoch
		}
		observeCompactUpdateLog(ctx, key, time.Since(start), applied, through, lastEpoch, err)
	}()
	if store == nil {
		return nil, ErrNilSnapshotLogStore
	}
	if err := fence.Validate(); err != nil {
		return nil, err
	}

	replay, err := ReplayUpdateLogContext(ctx, store, key, base, after, boundedReplayLimit(limit))
	if err != nil {
		return nil, err
	}

	result = &UpdateLogCompactionResult{
		Snapshot:  replay.Snapshot,
		Through:   replay.Through,
		Applied:   replay.Applied,
		LastEpoch: replay.LastEpoch,
	}
	if replay.Applied == 0 {
		return result, nil
	}
	if result.LastEpoch < fence.Owner.Epoch {
		result.LastEpoch = fence.Owner.Epoch
	}

	record, err := store.SaveSnapshotCheckpointAuthoritative(ctx, key, replay.Snapshot, replay.Through, fence)
	result.Record = record
	if err != nil {
		return result, fmt.Errorf("save compacted snapshot: %w", err)
	}
	if record != nil && record.Epoch > result.LastEpoch {
		result.LastEpoch = record.Epoch
	}
	if err := store.TrimUpdatesAuthoritative(ctx, key, replay.Through, fence); err != nil {
		return result, fmt.Errorf("trim compacted updates through %d: %w", replay.Through, err)
	}

	return result, nil
}

func loadReplayBaseSnapshot(ctx context.Context, snapshots SnapshotStore, key DocumentKey) (*yjsbridge.PersistedSnapshot, UpdateOffset, uint64, error) {
	if snapshots == nil {
		return yjsbridge.NewPersistedSnapshot(), 0, 0, nil
	}

	record, err := snapshots.LoadSnapshot(ctx, key)
	if err != nil {
		if errors.Is(err, ErrSnapshotNotFound) {
			return yjsbridge.NewPersistedSnapshot(), 0, 0, nil
		}
		return nil, 0, 0, err
	}
	if record == nil || record.Snapshot == nil {
		if record == nil {
			return yjsbridge.NewPersistedSnapshot(), 0, 0, nil
		}
		return yjsbridge.NewPersistedSnapshot(), record.Through, record.Epoch, nil
	}
	return record.Snapshot.Clone(), record.Through, record.Epoch, nil
}

func listReplayTailContext(ctx context.Context, store UpdateLogStore, key DocumentKey, after UpdateOffset, limit int, checkpointEpoch uint64) ([]*UpdateLogRecord, UpdateOffset, uint64, error) {
	if store == nil {
		return nil, after, checkpointEpoch, ErrNilUpdateLogStore
	}

	through := after
	lastEpoch := checkpointEpoch
	tail := make([]*UpdateLogRecord, 0)

	for {
		if err := ctx.Err(); err != nil {
			return nil, after, checkpointEpoch, err
		}

		records, err := store.ListUpdates(ctx, key, through, limit)
		if err != nil {
			return nil, after, checkpointEpoch, err
		}
		if len(records) == 0 {
			break
		}

		for idx, record := range records {
			if record == nil {
				return nil, after, checkpointEpoch, fmt.Errorf("%w: record %d nil", ErrInvalidUpdatePayload, idx)
			}
			if err := record.Validate(); err != nil {
				return nil, after, checkpointEpoch, fmt.Errorf("update log record %d: %w", idx, err)
			}
			if record.Key != key {
				return nil, after, checkpointEpoch, fmt.Errorf("%w: record %d key %#v", ErrUpdateLogKeyMismatch, idx, record.Key)
			}
			if record.Offset <= through {
				return nil, after, checkpointEpoch, fmt.Errorf("%w: offset %d after %d", ErrUpdateLogOffsetsOutOfOrder, record.Offset, through)
			}
			if err := validateReplayEpochProgression(lastEpoch, record.Epoch); err != nil {
				return nil, after, checkpointEpoch, fmt.Errorf("update log record %d: %w", idx, err)
			}

			tail = append(tail, record)
			through = record.Offset
			if record.Epoch > 0 {
				lastEpoch = record.Epoch
			}
		}
	}

	return tail, through, lastEpoch, nil
}

func replayUpdateBatchContext(ctx context.Context, key DocumentKey, currentUpdate []byte, result *UpdateLogReplayResult, records []*UpdateLogRecord) ([]byte, error) {
	updates := make([][]byte, 0, len(records)+1)
	updates = append(updates, currentUpdate)

	for idx, record := range records {
		if record == nil {
			return nil, fmt.Errorf("%w: record %d nil", ErrInvalidUpdatePayload, idx)
		}
		if err := record.Validate(); err != nil {
			return nil, fmt.Errorf("update log record %d: %w", idx, err)
		}
		if record.Key != key {
			return nil, fmt.Errorf("%w: record %d key %#v", ErrUpdateLogKeyMismatch, idx, record.Key)
		}
		if record.Offset <= result.Through {
			return nil, fmt.Errorf("%w: offset %d after %d", ErrUpdateLogOffsetsOutOfOrder, record.Offset, result.Through)
		}
		if err := validateReplayEpochProgression(result.LastEpoch, record.Epoch); err != nil {
			return nil, fmt.Errorf("update log record %d: %w", idx, err)
		}

		updateV2, err := updateLogRecordV2(record)
		if err != nil {
			return nil, fmt.Errorf("update log record %d: %w", idx, err)
		}
		updates = append(updates, updateV2)
		result.Through = record.Offset
		result.Applied++
		if record.Epoch > 0 {
			result.LastEpoch = record.Epoch
		}
	}

	return yjsbridge.MergeUpdatesV2Context(ctx, updates...)
}

func updateLogRecordV2(record *UpdateLogRecord) ([]byte, error) {
	if record == nil {
		return nil, ErrInvalidUpdatePayload
	}
	if len(record.UpdateV2) != 0 {
		return record.UpdateV2, nil
	}
	return yjsbridge.ConvertUpdateToV2(record.UpdateV1)
}

func validateReplayEpochProgression(lastEpoch uint64, nextEpoch uint64) error {
	if lastEpoch == 0 || nextEpoch == 0 {
		return nil
	}
	if nextEpoch < lastEpoch {
		return fmt.Errorf("%w: current=%d next=%d", ErrUpdateLogEpochRegression, lastEpoch, nextEpoch)
	}
	return nil
}

func cloneUpdateLogRecords(records []*UpdateLogRecord) []*UpdateLogRecord {
	if len(records) == 0 {
		return nil
	}

	cloned := make([]*UpdateLogRecord, 0, len(records))
	for _, record := range records {
		if record == nil {
			cloned = append(cloned, nil)
			continue
		}
		cloned = append(cloned, record.Clone())
	}
	return cloned
}

func saveSnapshotCheckpoint(ctx context.Context, store SnapshotStore, key DocumentKey, snapshot *yjsbridge.PersistedSnapshot, through UpdateOffset, epoch uint64) (*SnapshotRecord, error) {
	checkpointEpochStore, ok := store.(SnapshotCheckpointEpochStore)
	if ok {
		return checkpointEpochStore.SaveSnapshotCheckpointEpoch(ctx, key, snapshot, through, epoch)
	}
	checkpointStore, ok := store.(SnapshotCheckpointStore)
	if !ok {
		return store.SaveSnapshot(ctx, key, snapshot)
	}
	return checkpointStore.SaveSnapshotCheckpoint(ctx, key, snapshot, through)
}
