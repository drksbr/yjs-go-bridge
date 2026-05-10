package postgres

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/drksbr/yjs-crdt-golang-server/pkg/storage"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/yjsbridge"
)

func TestStoreSaveAndLoadSnapshotRoundTrip(t *testing.T) {
	store, _ := newTestStore(t, false)
	ctx := context.Background()

	snapshot, err := yjsbridge.PersistedSnapshotFromUpdates()
	if err != nil {
		t.Fatalf("PersistedSnapshotFromUpdates() unexpected error: %v", err)
	}

	key := storage.DocumentKey{
		Namespace:  "integration",
		DocumentID: "save-load-round-trip",
	}

	saved, err := store.SaveSnapshot(ctx, key, snapshot)
	if err != nil {
		t.Fatalf("SaveSnapshot() unexpected error: %v", err)
	}
	if saved.StoredAt.IsZero() {
		t.Fatal("SaveSnapshot().StoredAt is zero")
	}
	if saved.Through != 0 {
		t.Fatalf("SaveSnapshot().Through = %d, want 0", saved.Through)
	}
	if saved.Epoch != 0 {
		t.Fatalf("SaveSnapshot().Epoch = %d, want 0", saved.Epoch)
	}

	loaded, err := store.LoadSnapshot(ctx, key)
	if err != nil {
		t.Fatalf("LoadSnapshot() unexpected error: %v", err)
	}
	if !bytes.Equal(loaded.Snapshot.UpdateV1, snapshot.UpdateV1) {
		t.Fatalf("LoadSnapshot().Snapshot.UpdateV1 = %v, want %v", loaded.Snapshot.UpdateV1, snapshot.UpdateV1)
	}
	if !bytes.Equal(loaded.Snapshot.UpdateV2, snapshot.UpdateV2) {
		t.Fatalf("LoadSnapshot().Snapshot.UpdateV2 = %v, want %v", loaded.Snapshot.UpdateV2, snapshot.UpdateV2)
	}
	if loaded.Through != 0 {
		t.Fatalf("LoadSnapshot().Through = %d, want 0", loaded.Through)
	}
	if loaded.Epoch != 0 {
		t.Fatalf("LoadSnapshot().Epoch = %d, want 0", loaded.Epoch)
	}
	if len(loaded.Snapshot.UpdateV1) == 0 {
		t.Fatalf("LoadSnapshot().Snapshot.UpdateV1 is empty")
	}

	loaded.Snapshot.UpdateV1[0] = ^loaded.Snapshot.UpdateV1[0]
	reloaded, err := store.LoadSnapshot(ctx, key)
	if err != nil {
		t.Fatalf("LoadSnapshot() unexpected error after mutation: %v", err)
	}
	if bytes.Equal(reloaded.Snapshot.UpdateV1, loaded.Snapshot.UpdateV1) {
		t.Fatal("mutação vazou do retorno de LoadSnapshot()")
	}

	time.Sleep(20 * time.Millisecond)
	again, err := store.SaveSnapshot(ctx, key, snapshot)
	if err != nil {
		t.Fatalf("SaveSnapshot() second call unexpected error: %v", err)
	}
	if !again.StoredAt.After(saved.StoredAt) {
		t.Fatalf("segunda SaveSnapshot().StoredAt = %v, want after %v", again.StoredAt, saved.StoredAt)
	}
}

func TestStoreSaveAndLoadSnapshotCheckpointRoundTrip(t *testing.T) {
	store, _ := newTestStore(t, false)
	ctx := context.Background()

	snapshot, err := yjsbridge.PersistedSnapshotFromUpdates()
	if err != nil {
		t.Fatalf("PersistedSnapshotFromUpdates() unexpected error: %v", err)
	}

	key := storage.DocumentKey{
		Namespace:  "integration",
		DocumentID: "save-load-checkpoint-round-trip",
	}

	saved, err := store.SaveSnapshotCheckpoint(ctx, key, snapshot, 19)
	if err != nil {
		t.Fatalf("SaveSnapshotCheckpoint() unexpected error: %v", err)
	}
	if saved.Through != 19 {
		t.Fatalf("SaveSnapshotCheckpoint().Through = %d, want 19", saved.Through)
	}
	if saved.Epoch != 0 {
		t.Fatalf("SaveSnapshotCheckpoint().Epoch = %d, want 0", saved.Epoch)
	}

	loaded, err := store.LoadSnapshot(ctx, key)
	if err != nil {
		t.Fatalf("LoadSnapshot() unexpected error: %v", err)
	}
	if loaded.Through != 19 {
		t.Fatalf("LoadSnapshot().Through = %d, want 19", loaded.Through)
	}
	if loaded.Epoch != 0 {
		t.Fatalf("LoadSnapshot().Epoch = %d, want 0", loaded.Epoch)
	}

	saved, err = store.SaveSnapshotCheckpointEpoch(ctx, key, snapshot, 23, 7)
	if err != nil {
		t.Fatalf("SaveSnapshotCheckpointEpoch() unexpected error: %v", err)
	}
	if saved.Through != 23 {
		t.Fatalf("SaveSnapshotCheckpointEpoch().Through = %d, want 23", saved.Through)
	}
	if saved.Epoch != 7 {
		t.Fatalf("SaveSnapshotCheckpointEpoch().Epoch = %d, want 7", saved.Epoch)
	}

	loaded, err = store.LoadSnapshot(ctx, key)
	if err != nil {
		t.Fatalf("LoadSnapshot() unexpected error after epoch save: %v", err)
	}
	if loaded.Through != 23 {
		t.Fatalf("LoadSnapshot().Through after epoch save = %d, want 23", loaded.Through)
	}
	if loaded.Epoch != 7 {
		t.Fatalf("LoadSnapshot().Epoch after epoch save = %d, want 7", loaded.Epoch)
	}
}

func TestSaveSnapshotQueryWritesOnlyV2SnapshotPayload(t *testing.T) {
	t.Parallel()

	snapshot, err := yjsbridge.PersistedSnapshotFromUpdates()
	if err != nil {
		t.Fatalf("PersistedSnapshotFromUpdates() unexpected error: %v", err)
	}
	payloadV1, payloadV2, err := encodePersistedSnapshotPayloads(snapshot)
	if err != nil {
		t.Fatalf("encodePersistedSnapshotPayloads() unexpected error: %v", err)
	}

	store := &Store{schema: "tenant_app"}
	query, args, err := store.saveSnapshotQuery(
		storage.DocumentKey{Namespace: "team-a", DocumentID: "doc-1"},
		payloadV1,
		payloadV2,
		19,
		7,
	)
	if err != nil {
		t.Fatalf("saveSnapshotQuery() unexpected error: %v", err)
	}
	if !strings.Contains(query, "snapshot_v2") {
		t.Fatalf("saveSnapshotQuery() query = %q, want snapshot_v2 column", query)
	}
	if len(args) != 6 {
		t.Fatalf("saveSnapshotQuery() args len = %d, want 6", len(args))
	}
	if payloadV1 != nil {
		t.Fatalf("encodePersistedSnapshotPayloads() V1 payload = %v, want nil", payloadV1)
	}
	if args[2] != nil {
		t.Fatalf("saveSnapshotQuery() V1 arg = %v, want nil", args[2])
	}
	if !bytes.Equal(args[3].([]byte), payloadV2) {
		t.Fatalf("saveSnapshotQuery() V2 arg = %v, want %v", args[3], payloadV2)
	}
	if args[4] != int64(19) {
		t.Fatalf("saveSnapshotQuery() through arg = %v, want 19", args[4])
	}
	if args[5] != int64(7) {
		t.Fatalf("saveSnapshotQuery() epoch arg = %v, want 7", args[5])
	}
}

func TestStoreSaveSnapshotStoresOnlyV2Payload(t *testing.T) {
	store, schema := newTestStore(t, false)
	ctx := context.Background()

	snapshot, err := yjsbridge.PersistedSnapshotFromUpdates()
	if err != nil {
		t.Fatalf("PersistedSnapshotFromUpdates() unexpected error: %v", err)
	}
	key := storage.DocumentKey{
		Namespace:  "integration",
		DocumentID: "save-snapshot-v2-only",
	}
	if _, err := store.SaveSnapshot(ctx, key, snapshot); err != nil {
		t.Fatalf("SaveSnapshot() unexpected error: %v", err)
	}

	query := fmt.Sprintf(`
SELECT snapshot_v1 IS NULL, octet_length(snapshot_v2)
FROM %s.document_snapshots
WHERE namespace = $1 AND document_id = $2
`, quoteIdentifier(schema))
	var v1IsNull bool
	var v2Bytes int
	if err := store.pool.QueryRow(ctx, query, key.Namespace, key.DocumentID).Scan(&v1IsNull, &v2Bytes); err != nil {
		t.Fatalf("query persisted snapshot payloads unexpected error: %v", err)
	}
	if !v1IsNull {
		t.Fatal("snapshot_v1 is not null")
	}
	if v2Bytes == 0 {
		t.Fatal("snapshot_v2 is empty")
	}
}

func TestSnapshotPayloadsUseV2StorageWithCompatibilityNormalization(t *testing.T) {
	t.Parallel()

	oldUpdate := mustDecodePostgresHex(t, "0101b4ece9cb0500040107636f6e74656e74084c696e686120310a00")
	newUpdate := mustDecodePostgresHex(t, "0101b4ece9cb050884b4ece9cb0507274c696e6861203220636f6d206163656e746f733a2061c3a7c3a36f2c20636f7261c3a7c3a36f0a00")
	fullSnapshot, err := yjsbridge.PersistedSnapshotFromUpdates(oldUpdate, newUpdate)
	if err != nil {
		t.Fatalf("PersistedSnapshotFromUpdates() unexpected error: %v", err)
	}
	staleSnapshot, err := yjsbridge.PersistedSnapshotFromUpdates(oldUpdate)
	if err != nil {
		t.Fatalf("PersistedSnapshotFromUpdates(stale) unexpected error: %v", err)
	}

	staleInput := *fullSnapshot
	staleInput.UpdateV2 = staleSnapshot.UpdateV2
	payloadV1, payloadV2, err := encodePersistedSnapshotPayloads(&staleInput)
	if err != nil {
		t.Fatalf("encodePersistedSnapshotPayloads(stale input) unexpected error: %v", err)
	}
	if payloadV1 != nil {
		t.Fatalf("encodePersistedSnapshotPayloads(stale input) V1 payload = %v, want nil", payloadV1)
	}
	decodedPayloadV2, err := decodePersistedSnapshotPayload(nil, payloadV2)
	if err != nil {
		t.Fatalf("decodePersistedSnapshotPayload(canonical v2) unexpected error: %v", err)
	}
	if !bytes.Equal(decodedPayloadV2.UpdateV1, fullSnapshot.UpdateV1) {
		t.Fatalf("canonical V2 decoded UpdateV1 = %x, want full %x", decodedPayloadV2.UpdateV1, fullSnapshot.UpdateV1)
	}

	payloadV1, err = yjsbridge.EncodePersistedSnapshotV1(fullSnapshot)
	if err != nil {
		t.Fatalf("EncodePersistedSnapshotV1(full) unexpected error: %v", err)
	}
	_, stalePayloadV2, err := encodePersistedSnapshotPayloads(staleSnapshot)
	if err != nil {
		t.Fatalf("encodePersistedSnapshotPayloads(stale) unexpected error: %v", err)
	}

	reconciled, err := decodePersistedSnapshotPayload(payloadV1, stalePayloadV2)
	if err != nil {
		t.Fatalf("decodePersistedSnapshotPayload(reconcile) unexpected error: %v", err)
	}
	if !bytes.Equal(reconciled.UpdateV1, fullSnapshot.UpdateV1) {
		t.Fatalf("reconciled.UpdateV1 = %x, want full %x", reconciled.UpdateV1, fullSnapshot.UpdateV1)
	}

	fromV2, err := decodePersistedSnapshotPayload(nil, stalePayloadV2)
	if err != nil {
		t.Fatalf("decodePersistedSnapshotPayload(v2) unexpected error: %v", err)
	}
	if !bytes.Equal(fromV2.UpdateV1, staleSnapshot.UpdateV1) {
		t.Fatalf("decodePersistedSnapshotPayload(v2).UpdateV1 = %v, want %v", fromV2.UpdateV1, staleSnapshot.UpdateV1)
	}
	if !bytes.Equal(fromV2.UpdateV2, stalePayloadV2) {
		t.Fatalf("decodePersistedSnapshotPayload(v2).UpdateV2 = %v, want %v", fromV2.UpdateV2, stalePayloadV2)
	}

	fromV1, err := decodePersistedSnapshotPayload(payloadV1, nil)
	if err != nil {
		t.Fatalf("decodePersistedSnapshotPayload(v1 fallback) unexpected error: %v", err)
	}
	if !bytes.Equal(fromV1.UpdateV1, fullSnapshot.UpdateV1) {
		t.Fatalf("decodePersistedSnapshotPayload(v1 fallback).UpdateV1 = %v, want %v", fromV1.UpdateV1, fullSnapshot.UpdateV1)
	}
	if !bytes.Equal(fromV1.UpdateV2, fullSnapshot.UpdateV2) {
		t.Fatalf("decodePersistedSnapshotPayload(v1 fallback).UpdateV2 = %v, want %v", fromV1.UpdateV2, fullSnapshot.UpdateV2)
	}
}

func mustDecodePostgresHex(t *testing.T, value string) []byte {
	t.Helper()

	decoded, err := hex.DecodeString(value)
	if err != nil {
		t.Fatalf("hex.DecodeString(%q) unexpected error: %v", value, err)
	}
	return decoded
}

func TestStoreLoadMissingSnapshot(t *testing.T) {
	store, _ := newTestStore(t, true)
	ctx := context.Background()

	_, err := store.LoadSnapshot(ctx, storage.DocumentKey{DocumentID: "not-found"})
	if !errors.Is(err, storage.ErrSnapshotNotFound) {
		t.Fatalf("LoadSnapshot() error = %v, want %v", err, storage.ErrSnapshotNotFound)
	}
}

func TestStoreErrorContracts(t *testing.T) {
	t.Parallel()

	store := &Store{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	snapshot, err := yjsbridge.PersistedSnapshotFromUpdates()
	if err != nil {
		t.Fatalf("PersistedSnapshotFromUpdates() unexpected error: %v", err)
	}

	tests := []struct {
		name    string
		run     func() error
		wantErr error
	}{
		{
			name: "save_respects_context",
			run: func() error {
				_, err := store.SaveSnapshot(ctx, storage.DocumentKey{DocumentID: "doc-1"}, snapshot)
				return err
			},
			wantErr: context.Canceled,
		},
		{
			name: "load_respects_context",
			run: func() error {
				_, err := store.LoadSnapshot(ctx, storage.DocumentKey{DocumentID: "doc-1"})
				return err
			},
			wantErr: context.Canceled,
		},
		{
			name: "save_rejects_nil_snapshot",
			run: func() error {
				_, err := store.SaveSnapshot(context.Background(), storage.DocumentKey{DocumentID: "doc-1"}, nil)
				return err
			},
			wantErr: storage.ErrNilPersistedSnapshot,
		},
		{
			name: "save_rejects_invalid_key",
			run: func() error {
				_, err := store.SaveSnapshot(context.Background(), storage.DocumentKey{}, snapshot)
				return err
			},
			wantErr: storage.ErrInvalidDocumentKey,
		},
		{
			name: "load_rejects_invalid_key",
			run: func() error {
				_, err := store.LoadSnapshot(context.Background(), storage.DocumentKey{})
				return err
			},
			wantErr: storage.ErrInvalidDocumentKey,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if err := tt.run(); !errors.Is(err, tt.wantErr) {
				t.Fatalf("erro = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestStoreRequiresInitializedPool(t *testing.T) {
	t.Parallel()

	store := &Store{}
	snapshot, err := yjsbridge.PersistedSnapshotFromUpdates()
	if err != nil {
		t.Fatalf("PersistedSnapshotFromUpdates() unexpected error: %v", err)
	}

	if _, err := store.SaveSnapshot(context.Background(), storage.DocumentKey{DocumentID: "doc-1"}, snapshot); !errors.Is(err, errUninitializedStore) {
		t.Fatalf("SaveSnapshot() error = %v, want %v", err, errUninitializedStore)
	}
	if _, err := store.LoadSnapshot(context.Background(), storage.DocumentKey{DocumentID: "doc-1"}); !errors.Is(err, errUninitializedStore) {
		t.Fatalf("LoadSnapshot() error = %v, want %v", err, errUninitializedStore)
	}
}

func TestStoreConcurrentSaveLoadSmoke(t *testing.T) {
	t.Parallel()

	store, _ := newTestStore(t, false)
	ctx := context.Background()

	snapshot, err := yjsbridge.PersistedSnapshotFromUpdates()
	if err != nil {
		t.Fatalf("PersistedSnapshotFromUpdates() unexpected error: %v", err)
	}

	const workers = 6
	const iterations = 30
	var wg sync.WaitGroup
	errCh := make(chan error, workers*iterations*2)

	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := storage.DocumentKey{DocumentID: fmt.Sprintf("doc-%d", worker%3)}
			for i := 0; i < iterations; i++ {
				current := snapshot.Clone()
				if _, err := store.SaveSnapshot(ctx, key, current); err != nil {
					errCh <- err
					return
				}
				if _, err := store.LoadSnapshot(ctx, key); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Errorf("erro concorrente: %v", err)
	}
}
