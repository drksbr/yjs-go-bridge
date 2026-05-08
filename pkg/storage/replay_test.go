package storage

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/drksbr/yjs-crdt-golang-server/internal/varint"
	"github.com/drksbr/yjs-crdt-golang-server/internal/ytypes"
	"github.com/drksbr/yjs-crdt-golang-server/internal/yupdate"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/yjsbridge"
)

type testSnapshotLogStore struct {
	snapshot *SnapshotRecord
	records  []*UpdateLogRecord

	loadErr error
	listErr error
	saveErr error
	trimErr error

	listCalls              int
	saveCalls              int
	saveCheckpointCalls    int
	saveEpochCalls         int
	saveAuthoritativeCalls int
	trimCalls              int
	trimAuthoritativeCalls int

	trimmedKey     DocumentKey
	trimmedThrough UpdateOffset
	savedThrough   UpdateOffset
	savedEpoch     uint64
	lastFence      AuthorityFence
	listAfters     []UpdateOffset
	listLimits     []int
}

var _ SnapshotLogStore = (*testSnapshotLogStore)(nil)
var _ SnapshotCheckpointStore = (*testSnapshotLogStore)(nil)
var _ SnapshotCheckpointEpochStore = (*testSnapshotLogStore)(nil)
var _ AuthoritativeSnapshotLogStore = (*testSnapshotLogStore)(nil)

func (s *testSnapshotLogStore) SaveSnapshot(_ context.Context, key DocumentKey, snapshot *yjsbridge.PersistedSnapshot) (*SnapshotRecord, error) {
	return s.SaveSnapshotCheckpointEpoch(context.Background(), key, snapshot, 0, 0)
}

func (s *testSnapshotLogStore) SaveSnapshotCheckpoint(_ context.Context, key DocumentKey, snapshot *yjsbridge.PersistedSnapshot, through UpdateOffset) (*SnapshotRecord, error) {
	return s.SaveSnapshotCheckpointEpoch(context.Background(), key, snapshot, through, 0)
}

func (s *testSnapshotLogStore) SaveSnapshotCheckpointEpoch(_ context.Context, key DocumentKey, snapshot *yjsbridge.PersistedSnapshot, through UpdateOffset, epoch uint64) (*SnapshotRecord, error) {
	s.saveCalls++
	s.saveCheckpointCalls++
	s.saveEpochCalls++
	s.savedThrough = through
	s.savedEpoch = epoch
	if s.saveErr != nil {
		return nil, s.saveErr
	}

	record := &SnapshotRecord{
		Key:      key,
		Snapshot: snapshot.Clone(),
		Through:  through,
		Epoch:    epoch,
		StoredAt: time.Unix(500, 0).UTC(),
	}
	s.snapshot = record.Clone()
	return record, nil
}

func (s *testSnapshotLogStore) LoadSnapshot(_ context.Context, _ DocumentKey) (*SnapshotRecord, error) {
	if s.loadErr != nil {
		return nil, s.loadErr
	}
	return s.snapshot.Clone(), nil
}

func (s *testSnapshotLogStore) AppendUpdate(context.Context, DocumentKey, []byte) (*UpdateLogRecord, error) {
	return nil, nil
}

func (s *testSnapshotLogStore) AppendUpdateAuthoritative(ctx context.Context, key DocumentKey, update []byte, fence AuthorityFence) (*UpdateLogRecord, error) {
	if err := fence.Validate(); err != nil {
		return nil, err
	}
	s.lastFence = fence
	return s.AppendUpdate(ctx, key, update)
}

func (s *testSnapshotLogStore) ListUpdates(_ context.Context, _ DocumentKey, after UpdateOffset, limit int) ([]*UpdateLogRecord, error) {
	s.listCalls++
	s.listAfters = append(s.listAfters, after)
	s.listLimits = append(s.listLimits, limit)
	if s.listErr != nil {
		return nil, s.listErr
	}

	out := make([]*UpdateLogRecord, 0)
	for _, record := range s.records {
		if record == nil {
			out = append(out, nil)
		} else if record.Offset > after {
			out = append(out, record.Clone())
		}
		if limit > 0 && len(out) == limit {
			break
		}
	}
	return out, nil
}

func (s *testSnapshotLogStore) TrimUpdates(_ context.Context, key DocumentKey, through UpdateOffset) error {
	s.trimCalls++
	s.trimmedKey = key
	s.trimmedThrough = through
	if s.trimErr != nil {
		return s.trimErr
	}
	return nil
}

func (s *testSnapshotLogStore) SaveSnapshotAuthoritative(ctx context.Context, key DocumentKey, snapshot *yjsbridge.PersistedSnapshot, fence AuthorityFence) (*SnapshotRecord, error) {
	return s.SaveSnapshotCheckpointAuthoritative(ctx, key, snapshot, 0, fence)
}

func (s *testSnapshotLogStore) SaveSnapshotCheckpointAuthoritative(ctx context.Context, key DocumentKey, snapshot *yjsbridge.PersistedSnapshot, through UpdateOffset, fence AuthorityFence) (*SnapshotRecord, error) {
	if err := fence.Validate(); err != nil {
		return nil, err
	}
	s.saveAuthoritativeCalls++
	s.lastFence = fence
	return s.SaveSnapshotCheckpointEpoch(ctx, key, snapshot, through, fence.Owner.Epoch)
}

func (s *testSnapshotLogStore) TrimUpdatesAuthoritative(ctx context.Context, key DocumentKey, through UpdateOffset, fence AuthorityFence) error {
	if err := fence.Validate(); err != nil {
		return err
	}
	s.trimAuthoritativeCalls++
	s.lastFence = fence
	return s.TrimUpdates(ctx, key, through)
}

func TestReplaySnapshot(t *testing.T) {
	t.Parallel()

	baseUpdate := buildGCOnlyUpdate(1, 2)
	incremental := buildGCOnlyUpdate(4, 1)
	base := mustPersistedSnapshotFromUpdates(t, baseUpdate)

	replayed, err := ReplaySnapshot(context.Background(), base, &UpdateLogRecord{
		Key:      DocumentKey{Namespace: "team-a", DocumentID: "doc-1"},
		Offset:   7,
		UpdateV1: incremental,
	})
	if err != nil {
		t.Fatalf("ReplaySnapshot() unexpected error: %v", err)
	}

	want := mustPersistedSnapshotFromUpdates(t, baseUpdate, incremental)
	if !bytes.Equal(replayed.UpdateV1, want.UpdateV1) {
		t.Fatalf("ReplaySnapshot().UpdateV1 = %v, want %v", replayed.UpdateV1, want.UpdateV1)
	}
	if !bytes.Equal(replayed.UpdateV2, want.UpdateV2) {
		t.Fatalf("ReplaySnapshot().UpdateV2 = %v, want %v", replayed.UpdateV2, want.UpdateV2)
	}

	base.UpdateV1[0] ^= 0xff
	if bytes.Equal(replayed.UpdateV1, base.UpdateV1) {
		t.Fatal("ReplaySnapshot() retained mutable base payload reference")
	}
	base.UpdateV2[0] ^= 0xff
	if bytes.Equal(replayed.UpdateV2, base.UpdateV2) {
		t.Fatal("ReplaySnapshot() retained mutable base V2 payload reference")
	}
}

func TestReplaySnapshotErrors(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name    string
		ctx     context.Context
		base    *yjsbridge.PersistedSnapshot
		updates []*UpdateLogRecord
		wantErr error
	}{
		{
			name:    "canceled_context",
			ctx:     ctx,
			updates: []*UpdateLogRecord{{Key: DocumentKey{DocumentID: "doc-1"}, UpdateV1: buildGCOnlyUpdate(1, 1)}},
			wantErr: context.Canceled,
		},
		{
			name:    "invalid_record",
			ctx:     context.Background(),
			updates: []*UpdateLogRecord{{}},
			wantErr: ErrInvalidDocumentKey,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := ReplaySnapshot(tt.ctx, tt.base, tt.updates...)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("ReplaySnapshot() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestReplayUpdateLogContextRebuildsSnapshotFromBaseAndTail(t *testing.T) {
	t.Parallel()

	key := DocumentKey{Namespace: "tenant-a", DocumentID: "doc-2"}
	baseUpdate := buildGCOnlyUpdate(7, 1)
	base := mustPersistedSnapshotFromUpdates(t, baseUpdate)
	tailLeft := buildGCOnlyUpdate(8, 2)
	tailRight := buildGCOnlyUpdate(9, 1)

	store := &testSnapshotLogStore{
		records: []*UpdateLogRecord{
			{Key: key, Offset: 11, UpdateV1: tailLeft, Epoch: 3, StoredAt: time.Unix(11, 0).UTC()},
			{Key: key, Offset: 15, UpdateV1: tailRight, Epoch: 4, StoredAt: time.Unix(15, 0).UTC()},
		},
	}

	got, err := ReplayUpdateLogContext(context.Background(), store, key, base, 10, 1)
	if err != nil {
		t.Fatalf("ReplayUpdateLogContext() unexpected error: %v", err)
	}

	want := mustPersistedSnapshotFromUpdates(t, baseUpdate, tailLeft, tailRight)
	if got.Applied != 2 {
		t.Fatalf("ReplayUpdateLogContext().Applied = %d, want 2", got.Applied)
	}
	if got.Through != 15 {
		t.Fatalf("ReplayUpdateLogContext().Through = %d, want 15", got.Through)
	}
	if got.LastEpoch != 4 {
		t.Fatalf("ReplayUpdateLogContext().LastEpoch = %d, want 4", got.LastEpoch)
	}
	if !bytes.Equal(got.Snapshot.UpdateV1, want.UpdateV1) {
		t.Fatalf("ReplayUpdateLogContext().Snapshot.UpdateV1 = %v, want %v", got.Snapshot.UpdateV1, want.UpdateV1)
	}
	if store.listCalls != 3 {
		t.Fatalf("ListUpdates() calls = %d, want 3", store.listCalls)
	}
}

func TestReplayUpdateLogRebuildsFromNilBase(t *testing.T) {
	t.Parallel()

	key := DocumentKey{Namespace: "tenant-a", DocumentID: "doc-3"}
	left := buildGCOnlyUpdate(12, 1)
	right := buildGCOnlyUpdate(13, 3)

	store := &testSnapshotLogStore{
		records: []*UpdateLogRecord{
			{Key: key, Offset: 1, UpdateV1: left, Epoch: 1},
			{Key: key, Offset: 2, UpdateV1: right, Epoch: 1},
		},
	}

	got, err := ReplayUpdateLog(store, key, nil, 0, 0)
	if err != nil {
		t.Fatalf("ReplayUpdateLog() unexpected error: %v", err)
	}

	want := mustPersistedSnapshotFromUpdates(t, left, right)
	if got.Applied != 2 {
		t.Fatalf("ReplayUpdateLog().Applied = %d, want 2", got.Applied)
	}
	if got.Through != 2 {
		t.Fatalf("ReplayUpdateLog().Through = %d, want 2", got.Through)
	}
	if got.LastEpoch != 1 {
		t.Fatalf("ReplayUpdateLog().LastEpoch = %d, want 1", got.LastEpoch)
	}
	if !bytes.Equal(got.Snapshot.UpdateV1, want.UpdateV1) {
		t.Fatalf("ReplayUpdateLog().Snapshot.UpdateV1 = %v, want %v", got.Snapshot.UpdateV1, want.UpdateV1)
	}
}

func TestReplayUpdateLogContextNoTailReturnsIndependentSnapshot(t *testing.T) {
	t.Parallel()

	key := DocumentKey{Namespace: "tenant-a", DocumentID: "doc-4"}
	base := mustPersistedSnapshotFromUpdates(t, buildGCOnlyUpdate(14, 2))

	got, err := ReplayUpdateLogContext(context.Background(), &testSnapshotLogStore{}, key, base, 33, 2)
	if err != nil {
		t.Fatalf("ReplayUpdateLogContext() unexpected error: %v", err)
	}
	if got.Applied != 0 {
		t.Fatalf("ReplayUpdateLogContext().Applied = %d, want 0", got.Applied)
	}
	if got.Through != 33 {
		t.Fatalf("ReplayUpdateLogContext().Through = %d, want 33", got.Through)
	}
	if !bytes.Equal(got.Snapshot.UpdateV1, base.UpdateV1) {
		t.Fatalf("ReplayUpdateLogContext().Snapshot.UpdateV1 = %v, want %v", got.Snapshot.UpdateV1, base.UpdateV1)
	}

	got.Snapshot.UpdateV1[0] ^= 0xff
	if bytes.Equal(got.Snapshot.UpdateV1, base.UpdateV1) {
		t.Fatal("ReplayUpdateLogContext() reused base snapshot bytes")
	}
}

func TestReplayUpdateLogContextRejectsInconsistentTail(t *testing.T) {
	t.Parallel()

	key := DocumentKey{Namespace: "tenant-a", DocumentID: "doc-5"}
	otherKey := DocumentKey{Namespace: "tenant-a", DocumentID: "doc-6"}

	tests := []struct {
		name    string
		store   UpdateLogStore
		key     DocumentKey
		wantErr error
	}{
		{
			name:    "nil_store",
			store:   nil,
			key:     key,
			wantErr: ErrNilUpdateLogStore,
		},
		{
			name:    "invalid_key",
			store:   &testSnapshotLogStore{},
			key:     DocumentKey{},
			wantErr: ErrInvalidDocumentKey,
		},
		{
			name: "mismatched_key",
			store: &testSnapshotLogStore{
				records: []*UpdateLogRecord{
					{Key: otherKey, Offset: 1, UpdateV1: buildGCOnlyUpdate(15, 1)},
				},
			},
			key:     key,
			wantErr: ErrUpdateLogKeyMismatch,
		},
		{
			name: "out_of_order_offsets",
			store: &testSnapshotLogStore{
				records: []*UpdateLogRecord{
					{Key: key, Offset: 3, UpdateV1: buildGCOnlyUpdate(16, 1)},
					{Key: key, Offset: 2, UpdateV1: buildGCOnlyUpdate(17, 1)},
				},
			},
			key:     key,
			wantErr: ErrUpdateLogOffsetsOutOfOrder,
		},
		{
			name: "invalid_payload",
			store: &testSnapshotLogStore{
				records: []*UpdateLogRecord{
					{Key: key, Offset: 1, UpdateV1: nil},
				},
			},
			key:     key,
			wantErr: ErrInvalidUpdatePayload,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := ReplayUpdateLogContext(context.Background(), tt.store, tt.key, nil, 0, 0)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("ReplayUpdateLogContext() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestRecoverSnapshotPagesTailAndClonesUpdates(t *testing.T) {
	t.Parallel()

	key := DocumentKey{Namespace: "tenant-a", DocumentID: "doc-7"}
	baseUpdate := buildGCOnlyUpdate(20, 1)
	left := buildGCOnlyUpdate(21, 2)
	right := buildGCOnlyUpdate(22, 1)

	store := &testSnapshotLogStore{
		snapshot: &SnapshotRecord{
			Key:      key,
			Snapshot: mustPersistedSnapshotFromUpdates(t, baseUpdate),
			Through:  5,
			Epoch:    2,
			StoredAt: time.Unix(100, 0).UTC(),
		},
		records: []*UpdateLogRecord{
			{Key: key, Offset: 3, UpdateV1: buildGCOnlyUpdate(19, 1)},
			{Key: key, Offset: 7, UpdateV1: left, Epoch: 2},
			{Key: key, Offset: 9, UpdateV1: right, Epoch: 3},
		},
	}

	got, err := RecoverSnapshot(context.Background(), store, store, key, 0, 1)
	if err != nil {
		t.Fatalf("RecoverSnapshot() unexpected error: %v", err)
	}

	want := mustPersistedSnapshotFromUpdates(t, baseUpdate, left, right)
	if got.CheckpointThrough != 5 {
		t.Fatalf("RecoverSnapshot().CheckpointThrough = %d, want 5", got.CheckpointThrough)
	}
	if got.CheckpointEpoch != 2 {
		t.Fatalf("RecoverSnapshot().CheckpointEpoch = %d, want 2", got.CheckpointEpoch)
	}
	if got.LastOffset != 9 {
		t.Fatalf("RecoverSnapshot().LastOffset = %d, want 9", got.LastOffset)
	}
	if got.LastEpoch != 3 {
		t.Fatalf("RecoverSnapshot().LastEpoch = %d, want 3", got.LastEpoch)
	}
	if len(got.Updates) != 2 {
		t.Fatalf("len(RecoverSnapshot().Updates) = %d, want 2", len(got.Updates))
	}
	if !bytes.Equal(got.Snapshot.UpdateV1, want.UpdateV1) {
		t.Fatalf("RecoverSnapshot().Snapshot.UpdateV1 = %v, want %v", got.Snapshot.UpdateV1, want.UpdateV1)
	}
	if store.listCalls != 3 {
		t.Fatalf("ListUpdates() calls = %d, want 3", store.listCalls)
	}
	if len(store.listAfters) == 0 || store.listAfters[0] != 5 {
		t.Fatalf("ListUpdates() first after = %#v, want first call after 5", store.listAfters)
	}

	got.Updates[0].UpdateV1[0] ^= 0xff
	reloaded, err := RecoverSnapshot(context.Background(), store, store, key, 0, 1)
	if err != nil {
		t.Fatalf("RecoverSnapshot() reload unexpected error: %v", err)
	}
	if bytes.Equal(got.Updates[0].UpdateV1, reloaded.Updates[0].UpdateV1) {
		t.Fatal("RecoverSnapshot() leaked mutable update slice")
	}
}

func TestRecoverSnapshotStatePagesTailWithoutRetainingUpdates(t *testing.T) {
	t.Parallel()

	key := DocumentKey{Namespace: "tenant-a", DocumentID: "doc-state"}
	baseUpdate := buildGCOnlyUpdate(20, 1)
	left := buildGCOnlyUpdate(21, 2)
	right := buildGCOnlyUpdate(22, 1)

	store := &testSnapshotLogStore{
		snapshot: &SnapshotRecord{
			Key:      key,
			Snapshot: mustPersistedSnapshotFromUpdates(t, baseUpdate),
			Through:  5,
			Epoch:    2,
			StoredAt: time.Unix(100, 0).UTC(),
		},
		records: []*UpdateLogRecord{
			{Key: key, Offset: 3, UpdateV1: buildGCOnlyUpdate(19, 1)},
			{Key: key, Offset: 7, UpdateV1: left, Epoch: 2},
			{Key: key, Offset: 9, UpdateV1: right, Epoch: 3},
		},
	}

	got, err := RecoverSnapshotStateContext(context.Background(), store, store, key, 0, 1)
	if err != nil {
		t.Fatalf("RecoverSnapshotStateContext() unexpected error: %v", err)
	}

	want := mustPersistedSnapshotFromUpdates(t, baseUpdate, left, right)
	if got.CheckpointThrough != 5 {
		t.Fatalf("RecoverSnapshotStateContext().CheckpointThrough = %d, want 5", got.CheckpointThrough)
	}
	if got.CheckpointEpoch != 2 {
		t.Fatalf("RecoverSnapshotStateContext().CheckpointEpoch = %d, want 2", got.CheckpointEpoch)
	}
	if got.LastOffset != 9 {
		t.Fatalf("RecoverSnapshotStateContext().LastOffset = %d, want 9", got.LastOffset)
	}
	if got.LastEpoch != 3 {
		t.Fatalf("RecoverSnapshotStateContext().LastEpoch = %d, want 3", got.LastEpoch)
	}
	if got.Applied != 2 {
		t.Fatalf("RecoverSnapshotStateContext().Applied = %d, want 2", got.Applied)
	}
	if !bytes.Equal(got.Snapshot.UpdateV1, want.UpdateV1) {
		t.Fatalf("RecoverSnapshotStateContext().Snapshot.UpdateV1 = %v, want %v", got.Snapshot.UpdateV1, want.UpdateV1)
	}
	if store.listCalls != 3 {
		t.Fatalf("ListUpdates() calls = %d, want 3", store.listCalls)
	}
	if len(store.listAfters) == 0 || store.listAfters[0] != 5 {
		t.Fatalf("ListUpdates() first after = %#v, want first call after 5", store.listAfters)
	}
}

func TestRecoverSnapshotStateUsesDefaultPageLimit(t *testing.T) {
	t.Parallel()

	key := DocumentKey{DocumentID: "doc-state-default-limit"}
	store := &testSnapshotLogStore{
		records: []*UpdateLogRecord{
			{Key: key, Offset: 1, UpdateV1: buildGCOnlyUpdate(31, 1)},
			{Key: key, Offset: 2, UpdateV1: buildGCOnlyUpdate(32, 1)},
		},
	}

	got, err := RecoverSnapshotStateContext(context.Background(), nil, store, key, 0, 0)
	if err != nil {
		t.Fatalf("RecoverSnapshotStateContext() unexpected error: %v", err)
	}
	if got.Applied != 2 {
		t.Fatalf("RecoverSnapshotStateContext().Applied = %d, want 2", got.Applied)
	}
	if store.listCalls != 2 {
		t.Fatalf("ListUpdates() calls = %d, want 2", store.listCalls)
	}
	if len(store.listLimits) == 0 || store.listLimits[0] != defaultRecoverSnapshotStateLimit {
		t.Fatalf("ListUpdates() first limit = %#v, want %d", store.listLimits, defaultRecoverSnapshotStateLimit)
	}
}

func TestRecoverSnapshotWithoutSnapshotOrUpdates(t *testing.T) {
	t.Parallel()

	key := DocumentKey{DocumentID: "doc-8"}

	got, err := RecoverSnapshot(context.Background(), nil, nil, key, 9, 0)
	if err != nil {
		t.Fatalf("RecoverSnapshot() unexpected error: %v", err)
	}
	if got.LastOffset != 9 {
		t.Fatalf("RecoverSnapshot().LastOffset = %d, want 9", got.LastOffset)
	}
	if got.CheckpointThrough != 0 {
		t.Fatalf("RecoverSnapshot().CheckpointThrough = %d, want 0", got.CheckpointThrough)
	}
	if got.CheckpointEpoch != 0 {
		t.Fatalf("RecoverSnapshot().CheckpointEpoch = %d, want 0", got.CheckpointEpoch)
	}
	if got.LastEpoch != 0 {
		t.Fatalf("RecoverSnapshot().LastEpoch = %d, want 0", got.LastEpoch)
	}
	if got.Snapshot == nil || !got.Snapshot.IsEmpty() {
		t.Fatalf("RecoverSnapshot().Snapshot = %#v, want empty snapshot", got.Snapshot)
	}
}

func TestRecoverSnapshotRejectsConflictingCheckpointOffset(t *testing.T) {
	t.Parallel()

	key := DocumentKey{Namespace: "tenant-a", DocumentID: "doc-8b"}
	store := &testSnapshotLogStore{
		snapshot: &SnapshotRecord{
			Key:      key,
			Snapshot: mustPersistedSnapshotFromUpdates(t, buildGCOnlyUpdate(23, 1)),
			Through:  5,
			Epoch:    4,
			StoredAt: time.Unix(101, 0).UTC(),
		},
	}

	_, err := RecoverSnapshot(context.Background(), store, store, key, 3, 1)
	if !errors.Is(err, ErrSnapshotCheckpointMismatch) {
		t.Fatalf("RecoverSnapshot() error = %v, want %v", err, ErrSnapshotCheckpointMismatch)
	}
	if store.listCalls != 0 {
		t.Fatalf("ListUpdates() calls = %d, want 0", store.listCalls)
	}
}

func TestRecoverSnapshotRejectsEpochRegressionAcrossCheckpointAndTail(t *testing.T) {
	t.Parallel()

	key := DocumentKey{Namespace: "tenant-a", DocumentID: "doc-8c"}
	store := &testSnapshotLogStore{
		snapshot: &SnapshotRecord{
			Key:      key,
			Snapshot: mustPersistedSnapshotFromUpdates(t, buildGCOnlyUpdate(24, 1)),
			Through:  5,
			Epoch:    4,
			StoredAt: time.Unix(102, 0).UTC(),
		},
		records: []*UpdateLogRecord{
			{Key: key, Offset: 6, UpdateV1: buildGCOnlyUpdate(25, 1), Epoch: 3},
		},
	}

	_, err := RecoverSnapshot(context.Background(), store, store, key, 0, 1)
	if !errors.Is(err, ErrUpdateLogEpochRegression) {
		t.Fatalf("RecoverSnapshot() error = %v, want %v", err, ErrUpdateLogEpochRegression)
	}
}

func TestCompactUpdateLogContextSavesSnapshotAndTrimsTail(t *testing.T) {
	t.Parallel()

	key := DocumentKey{Namespace: "tenant-a", DocumentID: "doc-9"}
	baseUpdate := buildGCOnlyUpdate(30, 1)
	base := mustPersistedSnapshotFromUpdates(t, baseUpdate)
	tail := buildGCOnlyUpdate(31, 2)

	store := &testSnapshotLogStore{
		records: []*UpdateLogRecord{
			{Key: key, Offset: 8, UpdateV1: tail, Epoch: 5},
		},
	}

	got, err := CompactUpdateLogContext(context.Background(), store, key, base, 5, 1)
	if err != nil {
		t.Fatalf("CompactUpdateLogContext() unexpected error: %v", err)
	}

	want := mustPersistedSnapshotFromUpdates(t, baseUpdate, tail)
	if got.Record == nil {
		t.Fatal("CompactUpdateLogContext().Record = nil, want non-nil")
	}
	if got.Applied != 1 {
		t.Fatalf("CompactUpdateLogContext().Applied = %d, want 1", got.Applied)
	}
	if got.Through != 8 {
		t.Fatalf("CompactUpdateLogContext().Through = %d, want 8", got.Through)
	}
	if got.LastEpoch != 5 {
		t.Fatalf("CompactUpdateLogContext().LastEpoch = %d, want 5", got.LastEpoch)
	}
	if !bytes.Equal(got.Snapshot.UpdateV1, want.UpdateV1) {
		t.Fatalf("CompactUpdateLogContext().Snapshot.UpdateV1 = %v, want %v", got.Snapshot.UpdateV1, want.UpdateV1)
	}
	if !bytes.Equal(got.Record.Snapshot.UpdateV1, want.UpdateV1) {
		t.Fatalf("CompactUpdateLogContext().Record.Snapshot.UpdateV1 = %v, want %v", got.Record.Snapshot.UpdateV1, want.UpdateV1)
	}
	if got.Record.Through != 8 {
		t.Fatalf("CompactUpdateLogContext().Record.Through = %d, want 8", got.Record.Through)
	}
	if got.Record.Epoch != 5 {
		t.Fatalf("CompactUpdateLogContext().Record.Epoch = %d, want 5", got.Record.Epoch)
	}
	if store.saveCalls != 1 {
		t.Fatalf("SaveSnapshot() calls = %d, want 1", store.saveCalls)
	}
	if store.saveCheckpointCalls != 1 {
		t.Fatalf("SaveSnapshotCheckpoint() calls = %d, want 1", store.saveCheckpointCalls)
	}
	if store.saveEpochCalls != 1 {
		t.Fatalf("SaveSnapshotCheckpointEpoch() calls = %d, want 1", store.saveEpochCalls)
	}
	if store.savedThrough != 8 {
		t.Fatalf("SaveSnapshotCheckpoint() through = %d, want 8", store.savedThrough)
	}
	if store.savedEpoch != 5 {
		t.Fatalf("SaveSnapshotCheckpointEpoch() epoch = %d, want 5", store.savedEpoch)
	}
	if store.trimCalls != 1 {
		t.Fatalf("TrimUpdates() calls = %d, want 1", store.trimCalls)
	}
	if store.trimmedKey != key {
		t.Fatalf("TrimUpdates() key = %#v, want %#v", store.trimmedKey, key)
	}
	if store.trimmedThrough != 8 {
		t.Fatalf("TrimUpdates() through = %d, want 8", store.trimmedThrough)
	}
}

func TestCompactUpdateLogContextNoTailDoesNotPersistOrTrim(t *testing.T) {
	t.Parallel()

	key := DocumentKey{Namespace: "tenant-a", DocumentID: "doc-10"}
	base := mustPersistedSnapshotFromUpdates(t, buildGCOnlyUpdate(40, 1))
	store := &testSnapshotLogStore{}

	got, err := CompactUpdateLogContext(context.Background(), store, key, base, 12, 3)
	if err != nil {
		t.Fatalf("CompactUpdateLogContext() unexpected error: %v", err)
	}
	if got.Record != nil {
		t.Fatalf("CompactUpdateLogContext().Record = %#v, want nil", got.Record)
	}
	if got.Applied != 0 {
		t.Fatalf("CompactUpdateLogContext().Applied = %d, want 0", got.Applied)
	}
	if got.Through != 12 {
		t.Fatalf("CompactUpdateLogContext().Through = %d, want 12", got.Through)
	}
	if !bytes.Equal(got.Snapshot.UpdateV1, base.UpdateV1) {
		t.Fatalf("CompactUpdateLogContext().Snapshot.UpdateV1 = %v, want %v", got.Snapshot.UpdateV1, base.UpdateV1)
	}
	if store.saveCalls != 0 {
		t.Fatalf("SaveSnapshot() calls = %d, want 0", store.saveCalls)
	}
	if store.trimCalls != 0 {
		t.Fatalf("TrimUpdates() calls = %d, want 0", store.trimCalls)
	}
}

func TestCompactUpdateLogContextReturnsPartialResultOnTrimError(t *testing.T) {
	t.Parallel()

	key := DocumentKey{Namespace: "tenant-a", DocumentID: "doc-11"}
	store := &testSnapshotLogStore{
		records: []*UpdateLogRecord{
			{Key: key, Offset: 4, UpdateV1: buildGCOnlyUpdate(50, 1)},
		},
		trimErr: errors.New("boom"),
	}

	got, err := CompactUpdateLogContext(context.Background(), store, key, nil, 0, 0)
	if err == nil {
		t.Fatal("CompactUpdateLogContext() error = nil, want non-nil")
	}
	if got == nil {
		t.Fatal("CompactUpdateLogContext() result = nil, want partial result")
	}
	if got.Record == nil {
		t.Fatal("CompactUpdateLogContext().Record = nil, want saved record despite trim error")
	}
	if got.Through != 4 {
		t.Fatalf("CompactUpdateLogContext().Through = %d, want 4", got.Through)
	}
	if store.saveCalls != 1 {
		t.Fatalf("SaveSnapshot() calls = %d, want 1", store.saveCalls)
	}
	if store.trimCalls != 1 {
		t.Fatalf("TrimUpdates() calls = %d, want 1", store.trimCalls)
	}
}

func TestCompactUpdateLogContextUsesDefaultPageLimit(t *testing.T) {
	t.Parallel()

	key := DocumentKey{DocumentID: "doc-compact-default-limit"}
	store := &testSnapshotLogStore{
		records: []*UpdateLogRecord{
			{Key: key, Offset: 1, UpdateV1: buildGCOnlyUpdate(54, 1)},
			{Key: key, Offset: 2, UpdateV1: buildGCOnlyUpdate(55, 1)},
		},
	}

	got, err := CompactUpdateLogContext(context.Background(), store, key, nil, 0, 0)
	if err != nil {
		t.Fatalf("CompactUpdateLogContext() unexpected error: %v", err)
	}
	if got.Applied != 2 {
		t.Fatalf("CompactUpdateLogContext().Applied = %d, want 2", got.Applied)
	}
	if len(store.listLimits) == 0 || store.listLimits[0] != defaultRecoverSnapshotStateLimit {
		t.Fatalf("ListUpdates() first limit = %#v, want %d", store.listLimits, defaultRecoverSnapshotStateLimit)
	}
}

func TestCompactUpdateLogAuthoritativeContextSavesSnapshotAndTrimsTailWithFence(t *testing.T) {
	t.Parallel()

	key := DocumentKey{Namespace: "tenant-a", DocumentID: "doc-12"}
	baseUpdate := buildGCOnlyUpdate(60, 1)
	base := mustPersistedSnapshotFromUpdates(t, baseUpdate)
	tail := buildGCOnlyUpdate(61, 2)
	fence := AuthorityFence{
		ShardID: ShardID("7"),
		Owner: OwnerInfo{
			NodeID: NodeID("node-a"),
			Epoch:  9,
		},
		Token: "lease-node-a",
	}

	store := &testSnapshotLogStore{
		records: []*UpdateLogRecord{
			{Key: key, Offset: 13, UpdateV1: tail, Epoch: 8},
		},
	}

	got, err := CompactUpdateLogAuthoritativeContext(context.Background(), store, key, base, 10, 1, fence)
	if err != nil {
		t.Fatalf("CompactUpdateLogAuthoritativeContext() unexpected error: %v", err)
	}

	want := mustPersistedSnapshotFromUpdates(t, baseUpdate, tail)
	if got.Record == nil {
		t.Fatal("CompactUpdateLogAuthoritativeContext().Record = nil, want non-nil")
	}
	if got.Applied != 1 {
		t.Fatalf("CompactUpdateLogAuthoritativeContext().Applied = %d, want 1", got.Applied)
	}
	if got.Through != 13 {
		t.Fatalf("CompactUpdateLogAuthoritativeContext().Through = %d, want 13", got.Through)
	}
	if got.LastEpoch != fence.Owner.Epoch {
		t.Fatalf("CompactUpdateLogAuthoritativeContext().LastEpoch = %d, want %d", got.LastEpoch, fence.Owner.Epoch)
	}
	if !bytes.Equal(got.Snapshot.UpdateV1, want.UpdateV1) {
		t.Fatalf("CompactUpdateLogAuthoritativeContext().Snapshot.UpdateV1 = %v, want %v", got.Snapshot.UpdateV1, want.UpdateV1)
	}
	if got.Record.Through != 13 {
		t.Fatalf("CompactUpdateLogAuthoritativeContext().Record.Through = %d, want 13", got.Record.Through)
	}
	if got.Record.Epoch != fence.Owner.Epoch {
		t.Fatalf("CompactUpdateLogAuthoritativeContext().Record.Epoch = %d, want %d", got.Record.Epoch, fence.Owner.Epoch)
	}
	if store.saveAuthoritativeCalls != 1 {
		t.Fatalf("SaveSnapshotCheckpointAuthoritative() calls = %d, want 1", store.saveAuthoritativeCalls)
	}
	if store.trimAuthoritativeCalls != 1 {
		t.Fatalf("TrimUpdatesAuthoritative() calls = %d, want 1", store.trimAuthoritativeCalls)
	}
	if store.savedThrough != 13 {
		t.Fatalf("SaveSnapshotCheckpointAuthoritative() through = %d, want 13", store.savedThrough)
	}
	if store.savedEpoch != fence.Owner.Epoch {
		t.Fatalf("SaveSnapshotCheckpointAuthoritative() epoch = %d, want %d", store.savedEpoch, fence.Owner.Epoch)
	}
	if store.trimmedThrough != 13 {
		t.Fatalf("TrimUpdatesAuthoritative() through = %d, want 13", store.trimmedThrough)
	}
	if store.lastFence != fence {
		t.Fatalf("last fence = %#v, want %#v", store.lastFence, fence)
	}
}

func TestCompactUpdateLogAuthoritativeContextNoTailDoesNotPersistOrTrim(t *testing.T) {
	t.Parallel()

	key := DocumentKey{Namespace: "tenant-a", DocumentID: "doc-13"}
	base := mustPersistedSnapshotFromUpdates(t, buildGCOnlyUpdate(62, 1))
	fence := AuthorityFence{
		ShardID: ShardID("8"),
		Owner: OwnerInfo{
			NodeID: NodeID("node-a"),
			Epoch:  10,
		},
		Token: "lease-node-a",
	}
	store := &testSnapshotLogStore{}

	got, err := CompactUpdateLogAuthoritativeContext(context.Background(), store, key, base, 7, 0, fence)
	if err != nil {
		t.Fatalf("CompactUpdateLogAuthoritativeContext() unexpected error: %v", err)
	}
	if got.Record != nil {
		t.Fatalf("CompactUpdateLogAuthoritativeContext().Record = %#v, want nil", got.Record)
	}
	if got.Applied != 0 {
		t.Fatalf("CompactUpdateLogAuthoritativeContext().Applied = %d, want 0", got.Applied)
	}
	if got.Through != 7 {
		t.Fatalf("CompactUpdateLogAuthoritativeContext().Through = %d, want 7", got.Through)
	}
	if store.saveAuthoritativeCalls != 0 {
		t.Fatalf("SaveSnapshotCheckpointAuthoritative() calls = %d, want 0", store.saveAuthoritativeCalls)
	}
	if store.trimAuthoritativeCalls != 0 {
		t.Fatalf("TrimUpdatesAuthoritative() calls = %d, want 0", store.trimAuthoritativeCalls)
	}
}

func TestCompactUpdateLogAuthoritativeContextReturnsPartialResultOnTrimError(t *testing.T) {
	t.Parallel()

	key := DocumentKey{Namespace: "tenant-a", DocumentID: "doc-14"}
	fence := AuthorityFence{
		ShardID: ShardID("9"),
		Owner: OwnerInfo{
			NodeID: NodeID("node-a"),
			Epoch:  11,
		},
		Token: "lease-node-a",
	}
	store := &testSnapshotLogStore{
		records: []*UpdateLogRecord{
			{Key: key, Offset: 15, UpdateV1: buildGCOnlyUpdate(63, 1), Epoch: 11},
		},
		trimErr: ErrAuthorityLost,
	}

	got, err := CompactUpdateLogAuthoritativeContext(context.Background(), store, key, nil, 0, 0, fence)
	if !errors.Is(err, ErrAuthorityLost) {
		t.Fatalf("CompactUpdateLogAuthoritativeContext() error = %v, want %v", err, ErrAuthorityLost)
	}
	if got == nil {
		t.Fatal("CompactUpdateLogAuthoritativeContext() result = nil, want partial result")
	}
	if got.Record == nil {
		t.Fatal("CompactUpdateLogAuthoritativeContext().Record = nil, want saved record despite trim error")
	}
	if got.Record.Epoch != fence.Owner.Epoch {
		t.Fatalf("CompactUpdateLogAuthoritativeContext().Record.Epoch = %d, want %d", got.Record.Epoch, fence.Owner.Epoch)
	}
	if store.saveAuthoritativeCalls != 1 {
		t.Fatalf("SaveSnapshotCheckpointAuthoritative() calls = %d, want 1", store.saveAuthoritativeCalls)
	}
	if store.trimAuthoritativeCalls != 1 {
		t.Fatalf("TrimUpdatesAuthoritative() calls = %d, want 1", store.trimAuthoritativeCalls)
	}
}

func TestCompactUpdateLogAuthoritativeContextUsesDefaultPageLimit(t *testing.T) {
	t.Parallel()

	key := DocumentKey{DocumentID: "doc-compact-authoritative-default-limit"}
	fence := AuthorityFence{
		ShardID: ShardID("10"),
		Owner: OwnerInfo{
			NodeID: NodeID("node-a"),
			Epoch:  12,
		},
		Token: "lease-node-a",
	}
	store := &testSnapshotLogStore{
		records: []*UpdateLogRecord{
			{Key: key, Offset: 1, UpdateV1: buildGCOnlyUpdate(64, 1), Epoch: 12},
			{Key: key, Offset: 2, UpdateV1: buildGCOnlyUpdate(65, 1), Epoch: 12},
		},
	}

	got, err := CompactUpdateLogAuthoritativeContext(context.Background(), store, key, nil, 0, 0, fence)
	if err != nil {
		t.Fatalf("CompactUpdateLogAuthoritativeContext() unexpected error: %v", err)
	}
	if got.Applied != 2 {
		t.Fatalf("CompactUpdateLogAuthoritativeContext().Applied = %d, want 2", got.Applied)
	}
	if len(store.listLimits) == 0 || store.listLimits[0] != defaultRecoverSnapshotStateLimit {
		t.Fatalf("ListUpdates() first limit = %#v, want %d", store.listLimits, defaultRecoverSnapshotStateLimit)
	}
}

func TestCompactUpdateLogAuthoritativeContextRejectsInvalidFence(t *testing.T) {
	t.Parallel()

	key := DocumentKey{Namespace: "tenant-a", DocumentID: "doc-15"}
	store := &testSnapshotLogStore{}

	_, err := CompactUpdateLogAuthoritativeContext(context.Background(), store, key, nil, 0, 0, AuthorityFence{})
	if !errors.Is(err, ErrInvalidShardID) {
		t.Fatalf("CompactUpdateLogAuthoritativeContext() error = %v, want %v", err, ErrInvalidShardID)
	}
	if store.listCalls != 0 {
		t.Fatalf("ListUpdates() calls = %d, want 0", store.listCalls)
	}
}

func mustPersistedSnapshotFromUpdates(t *testing.T, updates ...[]byte) *yjsbridge.PersistedSnapshot {
	t.Helper()

	snapshot, err := yjsbridge.PersistedSnapshotFromUpdates(updates...)
	if err != nil {
		t.Fatalf("PersistedSnapshotFromUpdates() unexpected error: %v", err)
	}
	return snapshot
}

func buildGCOnlyUpdate(client, length uint32) []byte {
	update := varint.Append(nil, 1)
	update = varint.Append(update, 1)
	update = varint.Append(update, client)
	update = varint.Append(update, 0)
	update = append(update, 0)
	update = varint.Append(update, length)
	return append(update, yupdate.EncodeDeleteSetBlockV1(ytypes.NewDeleteSet())...)
}
