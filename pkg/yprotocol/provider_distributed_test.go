package yprotocol

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/drksbr/yjs-crdt-golang-server/pkg/storage"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/storage/memory"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/ycluster"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/yjsbridge"
)

func TestProviderOpenCoalescesConcurrentRoomLoads(t *testing.T) {
	t.Parallel()

	key := storage.DocumentKey{
		Namespace:  "tests",
		DocumentID: "provider-concurrent-room-load",
	}
	snapshot, err := yjsbridge.PersistedSnapshotFromUpdate(buildGCOnlyUpdate(97, 1))
	if err != nil {
		t.Fatalf("PersistedSnapshotFromUpdate() unexpected error: %v", err)
	}

	releaseLoad := make(chan struct{})
	var loadCalls atomic.Int32
	store := testSnapshotStore{
		loadSnapshot: func(ctx context.Context, got storage.DocumentKey) (*storage.SnapshotRecord, error) {
			loadCalls.Add(1)
			select {
			case <-releaseLoad:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return &storage.SnapshotRecord{
				Key:      got,
				Snapshot: snapshot.Clone(),
			}, nil
		},
	}
	provider := NewProvider(ProviderConfig{Store: store})

	const workers = 8
	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	for idx := 0; idx < workers; idx++ {
		idx := idx
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := provider.Open(context.Background(), key, "conn-"+string(rune('a'+idx)), uint32(100+idx))
			if err == nil {
				_, _ = conn.Close()
			}
			errCh <- err
		}()
	}

	waitForProviderLoadCall(t, &loadCalls)
	time.Sleep(20 * time.Millisecond)
	close(releaseLoad)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("Open() concurrent unexpected error: %v", err)
		}
	}
	if got := loadCalls.Load(); got != 1 {
		t.Fatalf("LoadSnapshot() calls = %d, want 1 coalesced load", got)
	}
}

func waitForProviderLoadCall(t *testing.T, calls *atomic.Int32) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if calls.Load() > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("LoadSnapshot() was not called before timeout")
}

func TestProviderOpenRecoversSnapshotPlusTail(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	key := storage.DocumentKey{
		Namespace:  "tests",
		DocumentID: "provider-recover-snapshot-tail",
	}
	store := memory.New()

	baseUpdate := buildGCOnlyUpdate(11, 1)
	tailUpdate := buildGCOnlyUpdate(22, 2)
	baseSnapshot, err := yjsbridge.PersistedSnapshotFromUpdate(baseUpdate)
	if err != nil {
		t.Fatalf("PersistedSnapshotFromUpdate(baseUpdate) unexpected error: %v", err)
	}
	if _, err := store.SaveSnapshot(ctx, key, baseSnapshot); err != nil {
		t.Fatalf("SaveSnapshot() unexpected error: %v", err)
	}
	if _, err := store.AppendUpdate(ctx, key, tailUpdate); err != nil {
		t.Fatalf("AppendUpdate() unexpected error: %v", err)
	}

	provider := NewProvider(ProviderConfig{Store: store})
	conn, err := provider.Open(ctx, key, "conn-a", 801)
	if err != nil {
		t.Fatalf("provider.Open() unexpected error: %v", err)
	}

	reply, err := conn.HandleEncodedMessages(EncodeProtocolSyncStep1([]byte{0x00}))
	if err != nil {
		t.Fatalf("conn.HandleEncodedMessages(step1) unexpected error: %v", err)
	}
	messages, err := DecodeProtocolMessages(reply.Direct)
	if err != nil {
		t.Fatalf("DecodeProtocolMessages(reply.Direct) unexpected error: %v", err)
	}
	if len(messages) != 1 || messages[0].Sync == nil {
		t.Fatalf("messages = %#v, want single sync step2", messages)
	}

	expectedUpdate, err := yjsbridge.MergeUpdates(baseUpdate, tailUpdate)
	if err != nil {
		t.Fatalf("MergeUpdates() unexpected error: %v", err)
	}
	expectedStep2, err := yjsbridge.DiffUpdate(expectedUpdate, []byte{0x00})
	if err != nil {
		t.Fatalf("DiffUpdate() unexpected error: %v", err)
	}
	if !bytes.Equal(messages[0].Sync.Payload, expectedStep2) {
		t.Fatalf("messages[0].Sync.Payload = %v, want %v", messages[0].Sync.Payload, expectedStep2)
	}
}

func TestProviderSyncUpdateAppendsLogAndPersistTrimsTail(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	key := storage.DocumentKey{
		Namespace:  "tests",
		DocumentID: "provider-append-log-persist-trim",
	}
	store := memory.New()
	provider := NewProvider(ProviderConfig{Store: store})

	conn, err := provider.Open(ctx, key, "conn-a", 802)
	if err != nil {
		t.Fatalf("provider.Open() unexpected error: %v", err)
	}

	update := buildGCOnlyUpdate(31, 3)
	if _, err := conn.HandleEncodedMessages(EncodeProtocolSyncUpdate(update)); err != nil {
		t.Fatalf("conn.HandleEncodedMessages(sync-update) unexpected error: %v", err)
	}

	records, err := store.ListUpdates(ctx, key, 0, 0)
	if err != nil {
		t.Fatalf("store.ListUpdates() unexpected error: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("len(records) = %d, want 1", len(records))
	}
	if len(records[0].UpdateV1) != 0 {
		t.Fatalf("records[0].UpdateV1 = %v, want empty V2-only storage payload", records[0].UpdateV1)
	}
	assertProtocolV2PayloadEquivalentToV1(t, records[0].UpdateV2, update)
	if len(records[0].UpdateV2) == 0 {
		t.Fatal("records[0].UpdateV2 is empty, want canonical V2 payload")
	}
	if records[0].Epoch != 0 {
		t.Fatalf("records[0].Epoch = %d, want 0", records[0].Epoch)
	}

	record, err := conn.Persist(ctx)
	if err != nil {
		t.Fatalf("conn.Persist() unexpected error: %v", err)
	}
	if record == nil || record.Snapshot == nil {
		t.Fatalf("conn.Persist() = %#v, want persisted snapshot", record)
	}
	if record.Through != 1 {
		t.Fatalf("conn.Persist().Through = %d, want 1", record.Through)
	}
	if record.Epoch != 0 {
		t.Fatalf("conn.Persist().Epoch = %d, want 0", record.Epoch)
	}

	trimmed, err := store.ListUpdates(ctx, key, 0, 0)
	if err != nil {
		t.Fatalf("store.ListUpdates() after persist unexpected error: %v", err)
	}
	if len(trimmed) != 0 {
		t.Fatalf("len(trimmed) = %d, want 0", len(trimmed))
	}

	loaded, err := store.LoadSnapshot(ctx, key)
	if err != nil {
		t.Fatalf("store.LoadSnapshot() unexpected error: %v", err)
	}
	expectedSnapshot, err := yjsbridge.PersistedSnapshotFromUpdate(update)
	if err != nil {
		t.Fatalf("PersistedSnapshotFromUpdate() unexpected error: %v", err)
	}
	if !bytes.Equal(loaded.Snapshot.UpdateV1, expectedSnapshot.UpdateV1) {
		t.Fatalf("loaded.Snapshot.UpdateV1 = %v, want %v", loaded.Snapshot.UpdateV1, expectedSnapshot.UpdateV1)
	}
	if loaded.Through != 1 {
		t.Fatalf("loaded.Through = %d, want 1", loaded.Through)
	}
	if loaded.Epoch != 0 {
		t.Fatalf("loaded.Epoch = %d, want 0", loaded.Epoch)
	}
}

func TestProviderSyncUpdateAppendsCanonicalV1ForV2Input(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	key := storage.DocumentKey{
		Namespace:  "tests",
		DocumentID: "provider-append-v2-as-v1",
	}
	store := memory.New()
	provider := NewProvider(ProviderConfig{Store: store})

	conn, err := provider.Open(ctx, key, "conn-a", 812)
	if err != nil {
		t.Fatalf("provider.Open() unexpected error: %v", err)
	}

	v2Update := mustDecodeProtocolHex(t, "000002a50100000104060374686901020101000001010000")
	v1Update, err := yjsbridge.ConvertUpdateToV1(v2Update)
	if err != nil {
		t.Fatalf("ConvertUpdateToV1(v2) unexpected error: %v", err)
	}
	if _, err := conn.HandleEncodedMessages(EncodeProtocolSyncUpdate(v2Update)); err != nil {
		t.Fatalf("conn.HandleEncodedMessages(v2 sync-update) unexpected error: %v", err)
	}

	records, err := store.ListUpdates(ctx, key, 0, 0)
	if err != nil {
		t.Fatalf("store.ListUpdates() unexpected error: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("len(records) = %d, want 1", len(records))
	}
	if len(records[0].UpdateV1) != 0 {
		t.Fatalf("records[0].UpdateV1 = %x, want empty V2-only storage payload", records[0].UpdateV1)
	}
	assertProtocolV2PayloadEquivalentToV1(t, records[0].UpdateV2, v1Update)
	if bytes.Equal(records[0].UpdateV2, v1Update) {
		t.Fatalf("records[0].UpdateV2 preserved V1 bytes: %x", records[0].UpdateV2)
	}
	if !bytes.Equal(records[0].UpdateV2, v2Update) {
		t.Fatalf("records[0].UpdateV2 = %x, want canonical V2 %x", records[0].UpdateV2, v2Update)
	}
}

func TestProviderPersistKeepsYjsV2KanbanObjectRewrites(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	key := storage.DocumentKey{
		Namespace:  "tests",
		DocumentID: "provider-persist-kanban-v2-rewrites",
	}
	store := memory.New()
	provider := NewProvider(ProviderConfig{Store: store})

	conn, err := provider.Open(ctx, key, "conn-a", 813)
	if err != nil {
		t.Fatalf("provider.Open() unexpected error: %v", err)
	}

	updates := [][]byte{
		mustDecodeProtocolHex(t, "00000597d3ca951d0000012813106b616e62616e2d636f6c73636f6c2d310b05010100010101010076050269647705636f6c2d31057469746c65770b4e6f766120636f6c756e610b6465736372697074696f6e770005636f6c6f72770723363437343862056f726465727da80f00"),
		mustDecodeProtocolHex(t, "00000597d3ca951d0000012815126b616e62616e2d6974656d736974656d2d310c060101000101010101760802696477066974656d2d31057469746c6577044974656d0b6465736372697074696f6e770008636f6c756d6e49647705636f6c2d31056f726465727da80f05636f6c6f727700106c696e6b6564446f63756d656e7449647700156c696e6b6564537562646f63756d656e744e616d65770000"),
		mustDecodeProtocolHex(t, "000006d7d3ca951d0001000001a801000000010101010276050269647705636f6c2d31057469746c65770b4e6f766120636f6c756e610b6465736372697074696f6e770005636f6c6f72770723656634343434056f726465727da80f01d7a9e5ca0e010000"),
		mustDecodeProtocolHex(t, "000006d7d3ca951d0001020001a8010000000101010103760802696477066974656d2d31057469746c6577044974656d0b6465736372697074696f6e770008636f6c756d6e49647705636f6c2d31056f726465727da80f05636f6c6f72770723323263353565106c696e6b6564446f63756d656e7449647700156c696e6b6564537562646f63756d656e744e616d65770001d7a9e5ca0e010100"),
	}

	for idx, update := range updates {
		if _, err := conn.HandleEncodedMessages(EncodeProtocolSyncUpdate(update)); err != nil {
			t.Fatalf("conn.HandleEncodedMessages(update %d) unexpected error: %v", idx, err)
		}
	}

	record, err := conn.Persist(ctx)
	if err != nil {
		t.Fatalf("conn.Persist() unexpected error: %v", err)
	}
	if record == nil || record.Snapshot == nil {
		t.Fatalf("conn.Persist() = %#v, want persisted snapshot", record)
	}

	expected, err := yjsbridge.MergeUpdatesV2(updates...)
	if err != nil {
		t.Fatalf("MergeUpdatesV2() unexpected error: %v", err)
	}
	if !bytes.Equal(record.Snapshot.UpdateV2, expected) {
		t.Fatalf("record.Snapshot.UpdateV2 mismatch:\n got: %x\nwant: %x", record.Snapshot.UpdateV2, expected)
	}
}

func TestProviderPersistCompactsRecoveredSnapshotPlusTail(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	key := storage.DocumentKey{
		Namespace:  "tests",
		DocumentID: "provider-persist-compacts-recovered-tail",
	}
	store := memory.New()

	baseUpdate := buildGCOnlyUpdate(32, 1)
	tailUpdate := buildGCOnlyUpdate(33, 2)
	baseSnapshot, err := yjsbridge.PersistedSnapshotFromUpdate(baseUpdate)
	if err != nil {
		t.Fatalf("PersistedSnapshotFromUpdate(baseUpdate) unexpected error: %v", err)
	}
	if _, err := store.SaveSnapshot(ctx, key, baseSnapshot); err != nil {
		t.Fatalf("store.SaveSnapshot() unexpected error: %v", err)
	}
	if _, err := store.AppendUpdate(ctx, key, tailUpdate); err != nil {
		t.Fatalf("store.AppendUpdate() unexpected error: %v", err)
	}

	provider := NewProvider(ProviderConfig{Store: store})
	conn, err := provider.Open(ctx, key, "conn-a", 805)
	if err != nil {
		t.Fatalf("provider.Open() unexpected error: %v", err)
	}

	record, err := conn.Persist(ctx)
	if err != nil {
		t.Fatalf("conn.Persist() unexpected error: %v", err)
	}
	if record == nil || record.Snapshot == nil {
		t.Fatalf("conn.Persist() = %#v, want persisted snapshot", record)
	}
	if record.Through != 1 {
		t.Fatalf("conn.Persist().Through = %d, want 1", record.Through)
	}
	if record.Epoch != 0 {
		t.Fatalf("conn.Persist().Epoch = %d, want 0", record.Epoch)
	}

	expectedSnapshot, err := yjsbridge.PersistedSnapshotFromUpdates(baseUpdate, tailUpdate)
	if err != nil {
		t.Fatalf("PersistedSnapshotFromUpdates() unexpected error: %v", err)
	}
	if !bytes.Equal(record.Snapshot.UpdateV1, expectedSnapshot.UpdateV1) {
		t.Fatalf("record.Snapshot.UpdateV1 = %v, want %v", record.Snapshot.UpdateV1, expectedSnapshot.UpdateV1)
	}
	if loaded, err := store.LoadSnapshot(ctx, key); err != nil {
		t.Fatalf("store.LoadSnapshot() unexpected error: %v", err)
	} else if loaded.Through != 1 {
		t.Fatalf("store.LoadSnapshot().Through = %d, want 1", loaded.Through)
	} else if loaded.Epoch != 0 {
		t.Fatalf("store.LoadSnapshot().Epoch = %d, want 0", loaded.Epoch)
	}

	records, err := store.ListUpdates(ctx, key, 0, 0)
	if err != nil {
		t.Fatalf("store.ListUpdates() unexpected error: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("len(records) = %d, want 0", len(records))
	}

	reopened := NewProvider(ProviderConfig{Store: store})
	recovered, err := reopened.Open(ctx, key, "conn-b", 806)
	if err != nil {
		t.Fatalf("reopened.Open() unexpected error: %v", err)
	}

	reply, err := recovered.HandleEncodedMessages(EncodeProtocolSyncStep1([]byte{0x00}))
	if err != nil {
		t.Fatalf("recovered.HandleEncodedMessages(step1) unexpected error: %v", err)
	}
	messages, err := DecodeProtocolMessages(reply.Direct)
	if err != nil {
		t.Fatalf("DecodeProtocolMessages(reply.Direct) unexpected error: %v", err)
	}
	if len(messages) != 1 || messages[0].Sync == nil {
		t.Fatalf("messages = %#v, want single sync step2", messages)
	}

	expectedStep2, err := yjsbridge.DiffUpdate(expectedSnapshot.UpdateV1, []byte{0x00})
	if err != nil {
		t.Fatalf("DiffUpdate() unexpected error: %v", err)
	}
	if !bytes.Equal(messages[0].Sync.Payload, expectedStep2) {
		t.Fatalf("messages[0].Sync.Payload = %v, want %v", messages[0].Sync.Payload, expectedStep2)
	}
}

func TestProviderSyncUpdateDoesNotAdvanceSessionsWhenAppendFails(t *testing.T) {
	t.Parallel()

	appendErr := errors.New("append failed")
	store := failingDistributedStore{appendErr: appendErr}
	provider := NewProvider(ProviderConfig{Store: store})
	key := storage.DocumentKey{
		Namespace:  "tests",
		DocumentID: "provider-append-failure",
	}

	sender, err := provider.Open(context.Background(), key, "conn-a", 803)
	if err != nil {
		t.Fatalf("provider.Open(sender) unexpected error: %v", err)
	}
	peer, err := provider.Open(context.Background(), key, "conn-b", 804)
	if err != nil {
		t.Fatalf("provider.Open(peer) unexpected error: %v", err)
	}

	beforeSender := sender.session.UpdateV1()
	beforePeer := peer.session.UpdateV1()
	update := buildGCOnlyUpdate(41, 2)

	if _, err := sender.HandleEncodedMessages(EncodeProtocolSyncUpdate(update)); !errors.Is(err, appendErr) {
		t.Fatalf("sender.HandleEncodedMessages() error = %v, want %v", err, appendErr)
	}
	if !bytes.Equal(sender.session.UpdateV1(), beforeSender) {
		t.Fatalf("sender.session.UpdateV1() = %v, want %v", sender.session.UpdateV1(), beforeSender)
	}
	if !bytes.Equal(peer.session.UpdateV1(), beforePeer) {
		t.Fatalf("peer.session.UpdateV1() = %v, want %v", peer.session.UpdateV1(), beforePeer)
	}
}

func TestConnectionHandleEncodedMessagesContextCancellationPreventsAppend(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	key := storage.DocumentKey{
		Namespace:  "tests",
		DocumentID: "provider-context-cancel-append",
	}
	store := memory.New()
	provider := NewProvider(ProviderConfig{Store: store})

	conn, err := provider.Open(ctx, key, "conn-a", 807)
	if err != nil {
		t.Fatalf("provider.Open() unexpected error: %v", err)
	}
	before := conn.session.UpdateV1()

	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	if _, err := conn.HandleEncodedMessagesContext(cancelled, EncodeProtocolSyncUpdate(buildGCOnlyUpdate(42, 2))); !errors.Is(err, context.Canceled) {
		t.Fatalf("conn.HandleEncodedMessagesContext() error = %v, want %v", err, context.Canceled)
	}
	if !bytes.Equal(conn.session.UpdateV1(), before) {
		t.Fatalf("conn.session.UpdateV1() = %v, want %v", conn.session.UpdateV1(), before)
	}

	records, err := store.ListUpdates(ctx, key, 0, 0)
	if err != nil {
		t.Fatalf("store.ListUpdates() unexpected error: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("len(store.ListUpdates()) = %d, want 0", len(records))
	}
}

func TestProviderOpenRejectsAuthorityFenceWithoutAuthoritativeStore(t *testing.T) {
	t.Parallel()

	provider := NewProvider(ProviderConfig{
		Store: testSnapshotStore{},
		ResolveAuthorityFence: func(context.Context, storage.DocumentKey) (*storage.AuthorityFence, error) {
			return &storage.AuthorityFence{
				ShardID: storage.ShardID("7"),
				Owner: storage.OwnerInfo{
					NodeID: storage.NodeID("node-a"),
					Epoch:  1,
				},
				Token: "lease-a",
			}, nil
		},
	})

	_, err := provider.Open(context.Background(), storage.DocumentKey{
		Namespace:  "tests",
		DocumentID: "provider-authority-unsupported",
	}, "conn-a", 901)
	if !errors.Is(err, ErrAuthorityFenceUnsupported) {
		t.Fatalf("provider.Open() error = %v, want %v", err, ErrAuthorityFenceUnsupported)
	}
}

func TestProviderAuthorityLossOnAppendMarksRoomStale(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	key := storage.DocumentKey{
		Namespace:  "tests",
		DocumentID: "provider-authority-loss-append",
	}
	store, resolver, provider := newAuthoritativeMemoryProvider(t, "node-a")
	seedAuthoritativeDocument(t, ctx, store, resolver, key, "node-a", 1, "lease-node-a")

	sender, err := provider.Open(ctx, key, "conn-a", 902)
	if err != nil {
		t.Fatalf("provider.Open(sender) unexpected error: %v", err)
	}
	peer, err := provider.Open(ctx, key, "conn-b", 903)
	if err != nil {
		t.Fatalf("provider.Open(peer) unexpected error: %v", err)
	}

	beforeSender := sender.session.UpdateV1()
	beforePeer := peer.session.UpdateV1()
	handoffAuthority(t, ctx, store, resolver, key, "lease-node-a", "node-b", 2, "lease-node-b")

	update := buildGCOnlyUpdate(51, 2)
	if _, err := sender.HandleEncodedMessages(EncodeProtocolSyncUpdate(update)); !errors.Is(err, ErrAuthorityLost) {
		t.Fatalf("sender.HandleEncodedMessages() error = %v, want %v", err, ErrAuthorityLost)
	}
	if !sender.AuthorityLost() {
		t.Fatal("sender.AuthorityLost() = false, want true")
	}
	if !bytes.Equal(sender.session.UpdateV1(), beforeSender) {
		t.Fatalf("sender.session.UpdateV1() = %v, want %v", sender.session.UpdateV1(), beforeSender)
	}
	if !bytes.Equal(peer.session.UpdateV1(), beforePeer) {
		t.Fatalf("peer.session.UpdateV1() = %v, want %v", peer.session.UpdateV1(), beforePeer)
	}

	records, err := store.ListUpdates(ctx, key, 0, 0)
	if err != nil {
		t.Fatalf("store.ListUpdates() unexpected error: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("len(store.ListUpdates()) = %d, want 0", len(records))
	}

	if _, err := sender.Persist(ctx); !errors.Is(err, ErrAuthorityLost) {
		t.Fatalf("sender.Persist() error = %v, want %v", err, ErrAuthorityLost)
	}
	if _, err := provider.Open(ctx, key, "conn-c", 904); !errors.Is(err, ErrAuthorityLost) {
		t.Fatalf("provider.Open(conn-c) error = %v, want %v", err, ErrAuthorityLost)
	}
}

func TestProviderAuthorityLossOnPersistPreservesTail(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	key := storage.DocumentKey{
		Namespace:  "tests",
		DocumentID: "provider-authority-loss-persist",
	}
	store, resolver, provider := newAuthoritativeMemoryProvider(t, "node-a")
	seedAuthoritativeDocument(t, ctx, store, resolver, key, "node-a", 1, "lease-node-a")

	conn, err := provider.Open(ctx, key, "conn-a", 905)
	if err != nil {
		t.Fatalf("provider.Open() unexpected error: %v", err)
	}

	update := buildGCOnlyUpdate(61, 3)
	if _, err := conn.HandleEncodedMessages(EncodeProtocolSyncUpdate(update)); err != nil {
		t.Fatalf("conn.HandleEncodedMessages(sync-update) unexpected error: %v", err)
	}

	handoffAuthority(t, ctx, store, resolver, key, "lease-node-a", "node-b", 2, "lease-node-b")
	if _, err := conn.Persist(ctx); !errors.Is(err, ErrAuthorityLost) {
		t.Fatalf("conn.Persist() error = %v, want %v", err, ErrAuthorityLost)
	}
	if !conn.AuthorityLost() {
		t.Fatal("conn.AuthorityLost() = false, want true")
	}

	records, err := store.ListUpdates(ctx, key, 0, 0)
	if err != nil {
		t.Fatalf("store.ListUpdates() unexpected error: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("len(store.ListUpdates()) = %d, want 1", len(records))
	}
	if len(records[0].UpdateV1) != 0 {
		t.Fatalf("records[0].UpdateV1 = %v, want empty V2-only storage payload", records[0].UpdateV1)
	}
	assertProtocolV2PayloadEquivalentToV1(t, records[0].UpdateV2, update)
	if _, err := conn.HandleEncodedMessages(EncodeProtocolQueryAwareness()); !errors.Is(err, ErrAuthorityLost) {
		t.Fatalf("conn.HandleEncodedMessages(query-awareness) error = %v, want %v", err, ErrAuthorityLost)
	}
}

func TestConnectionRevalidateAuthorityMarksRoomStaleAfterHandoff(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	key := storage.DocumentKey{
		Namespace:  "tests",
		DocumentID: "provider-revalidate-authority",
	}
	store, resolver, provider := newAuthoritativeMemoryProvider(t, "node-a")
	seedAuthoritativeDocument(t, ctx, store, resolver, key, "node-a", 1, "lease-node-a")

	conn, err := provider.Open(ctx, key, "conn-a", 906)
	if err != nil {
		t.Fatalf("provider.Open() unexpected error: %v", err)
	}
	if err := conn.RevalidateAuthority(ctx); err != nil {
		t.Fatalf("conn.RevalidateAuthority(initial) unexpected error: %v", err)
	}

	handoffAuthority(t, ctx, store, resolver, key, "lease-node-a", "node-b", 2, "lease-node-b")
	if err := conn.RevalidateAuthority(ctx); !errors.Is(err, ErrAuthorityLost) {
		t.Fatalf("conn.RevalidateAuthority(after handoff) error = %v, want %v", err, ErrAuthorityLost)
	}
	if !conn.AuthorityLost() {
		t.Fatal("conn.AuthorityLost() = false, want true")
	}
	if _, err := provider.Open(ctx, key, "conn-b", 907); !errors.Is(err, ErrAuthorityLost) {
		t.Fatalf("provider.Open(conn-b) error = %v, want %v", err, ErrAuthorityLost)
	}
}

type failingDistributedStore struct {
	appendErr error
}

func (f failingDistributedStore) SaveSnapshot(context.Context, storage.DocumentKey, *yjsbridge.PersistedSnapshot) (*storage.SnapshotRecord, error) {
	return &storage.SnapshotRecord{
		Snapshot: yjsbridge.NewPersistedSnapshot(),
	}, nil
}

func (f failingDistributedStore) LoadSnapshot(context.Context, storage.DocumentKey) (*storage.SnapshotRecord, error) {
	return nil, storage.ErrSnapshotNotFound
}

func (f failingDistributedStore) AppendUpdate(context.Context, storage.DocumentKey, []byte) (*storage.UpdateLogRecord, error) {
	return nil, f.appendErr
}

func (f failingDistributedStore) ListUpdates(context.Context, storage.DocumentKey, storage.UpdateOffset, int) ([]*storage.UpdateLogRecord, error) {
	return nil, nil
}

func (f failingDistributedStore) TrimUpdates(context.Context, storage.DocumentKey, storage.UpdateOffset) error {
	return nil
}

func newAuthoritativeMemoryProvider(t *testing.T, localNode ycluster.NodeID) (*memory.Store, ycluster.ShardResolver, *Provider) {
	t.Helper()

	store := memory.New()
	resolver, err := ycluster.NewDeterministicShardResolver(32)
	if err != nil {
		t.Fatalf("NewDeterministicShardResolver() unexpected error: %v", err)
	}
	lookup, err := ycluster.NewStorageOwnerLookup(localNode, resolver, store, store)
	if err != nil {
		t.Fatalf("NewStorageOwnerLookup(%s) unexpected error: %v", localNode, err)
	}

	provider := NewProvider(ProviderConfig{
		Store: store,
		ResolveAuthorityFence: func(ctx context.Context, key storage.DocumentKey) (*storage.AuthorityFence, error) {
			return ycluster.ResolveStorageAuthorityFence(ctx, lookup, key)
		},
	})
	return store, resolver, provider
}

func seedAuthoritativeDocument(
	t *testing.T,
	ctx context.Context,
	store *memory.Store,
	resolver ycluster.ShardResolver,
	key storage.DocumentKey,
	node ycluster.NodeID,
	epoch uint64,
	token string,
) {
	t.Helper()

	shardID, err := resolver.ResolveShard(key)
	if err != nil {
		t.Fatalf("ResolveShard(%#v) unexpected error: %v", key, err)
	}
	if _, err := store.SavePlacement(ctx, storage.PlacementRecord{
		Key:     key,
		ShardID: ycluster.StorageShardID(shardID),
		Version: 1,
	}); err != nil {
		t.Fatalf("store.SavePlacement() unexpected error: %v", err)
	}
	if _, err := store.SaveLease(ctx, storage.LeaseRecord{
		ShardID: ycluster.StorageShardID(shardID),
		Owner: storage.OwnerInfo{
			NodeID: ycluster.StorageNodeID(node),
			Epoch:  epoch,
		},
		Token:      token,
		ExpiresAt:  time.Now().UTC().Add(time.Hour),
		AcquiredAt: time.Now().UTC().Add(-time.Minute),
	}); err != nil {
		t.Fatalf("store.SaveLease() unexpected error: %v", err)
	}
}

func handoffAuthority(
	t *testing.T,
	ctx context.Context,
	store *memory.Store,
	resolver ycluster.ShardResolver,
	key storage.DocumentKey,
	oldToken string,
	nextNode ycluster.NodeID,
	nextEpoch uint64,
	nextToken string,
) {
	t.Helper()

	shardID, err := resolver.ResolveShard(key)
	if err != nil {
		t.Fatalf("ResolveShard(%#v) unexpected error: %v", key, err)
	}
	if err := store.ReleaseLease(ctx, ycluster.StorageShardID(shardID), oldToken); err != nil {
		t.Fatalf("store.ReleaseLease() unexpected error: %v", err)
	}
	if _, err := store.SaveLease(ctx, storage.LeaseRecord{
		ShardID: ycluster.StorageShardID(shardID),
		Owner: storage.OwnerInfo{
			NodeID: ycluster.StorageNodeID(nextNode),
			Epoch:  nextEpoch,
		},
		Token:      nextToken,
		ExpiresAt:  time.Now().UTC().Add(2 * time.Hour),
		AcquiredAt: time.Now().UTC().Add(time.Minute),
	}); err != nil {
		t.Fatalf("store.SaveLease() handoff unexpected error: %v", err)
	}
}
