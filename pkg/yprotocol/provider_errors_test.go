package yprotocol

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/drksbr/yjs-crdt-golang-server/pkg/storage"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/yjsbridge"
)

func TestProviderConnectionErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "open rejects blank connection id",
			run: func(t *testing.T) {
				t.Parallel()

				provider := NewProvider(ProviderConfig{})
				_, err := provider.Open(context.Background(), storage.DocumentKey{
					Namespace:  "tests",
					DocumentID: "provider-errors-invalid-id",
				}, " \t\n ", 1)
				if !errors.Is(err, ErrInvalidConnectionID) {
					t.Fatalf("Open() error = %v, want %v", err, ErrInvalidConnectionID)
				}
			},
		},
		{
			name: "open rejects duplicate connection id within the same document",
			run: func(t *testing.T) {
				t.Parallel()

				provider := NewProvider(ProviderConfig{})
				key := storage.DocumentKey{
					Namespace:  "tests",
					DocumentID: "provider-errors-duplicate-id",
				}

				first, err := provider.Open(context.Background(), key, "dup", 1)
				if err != nil {
					t.Fatalf("first Open() unexpected error: %v", err)
				}
				t.Cleanup(func() {
					if _, closeErr := first.Close(); closeErr != nil && !errors.Is(closeErr, ErrConnectionClosed) {
						t.Fatalf("first Close() cleanup unexpected error: %v", closeErr)
					}
				})

				_, err = provider.Open(context.Background(), key, "dup", 2)
				if !errors.Is(err, ErrConnectionExists) {
					t.Fatalf("second Open() error = %v, want %v", err, ErrConnectionExists)
				}
			},
		},
		{
			name: "open rejects duplicate client id within the same document",
			run: func(t *testing.T) {
				t.Parallel()

				provider := NewProvider(ProviderConfig{})
				key := storage.DocumentKey{
					Namespace:  "tests",
					DocumentID: "provider-errors-duplicate-client-id",
				}

				first, err := provider.Open(context.Background(), key, "conn-a", 42)
				if err != nil {
					t.Fatalf("first Open() unexpected error: %v", err)
				}
				t.Cleanup(func() {
					if _, closeErr := first.Close(); closeErr != nil && !errors.Is(closeErr, ErrConnectionClosed) {
						t.Fatalf("first Close() cleanup unexpected error: %v", closeErr)
					}
				})

				_, err = provider.Open(context.Background(), key, "conn-b", 42)
				if !errors.Is(err, ErrClientIDExists) {
					t.Fatalf("second Open() error = %v, want %v", err, ErrClientIDExists)
				}
			},
		},
		{
			name: "persist fails when provider store is disabled",
			run: func(t *testing.T) {
				t.Parallel()

				provider := NewProvider(ProviderConfig{})
				key := storage.DocumentKey{
					Namespace:  "tests",
					DocumentID: "provider-errors-persist-disabled",
				}

				conn, err := provider.Open(context.Background(), key, "persist-disabled", 3)
				if err != nil {
					t.Fatalf("Open() unexpected error: %v", err)
				}
				t.Cleanup(func() {
					if _, closeErr := conn.Close(); closeErr != nil && !errors.Is(closeErr, ErrConnectionClosed) {
						t.Fatalf("Close() cleanup unexpected error: %v", closeErr)
					}
				})

				_, err = conn.Persist(context.Background())
				if !errors.Is(err, ErrPersistenceDisabled) {
					t.Fatalf("Persist() error = %v, want %v", err, ErrPersistenceDisabled)
				}
			},
		},
		{
			name: "closed connection rejects close persist and message handling",
			run: func(t *testing.T) {
				t.Parallel()

				provider := NewProvider(ProviderConfig{})
				key := storage.DocumentKey{
					Namespace:  "tests",
					DocumentID: "provider-errors-closed-connection",
				}

				conn, err := provider.Open(context.Background(), key, "closed", 4)
				if err != nil {
					t.Fatalf("Open() unexpected error: %v", err)
				}

				if _, err := conn.Close(); err != nil {
					t.Fatalf("first Close() unexpected error: %v", err)
				}
				if _, err := conn.Close(); !errors.Is(err, ErrConnectionClosed) {
					t.Fatalf("second Close() error = %v, want %v", err, ErrConnectionClosed)
				}
				if _, err := conn.Persist(context.Background()); !errors.Is(err, ErrConnectionClosed) {
					t.Fatalf("Persist() after Close error = %v, want %v", err, ErrConnectionClosed)
				}
				if _, err := conn.HandleEncodedMessages(EncodeProtocolQueryAwareness()); !errors.Is(err, ErrConnectionClosed) {
					t.Fatalf("HandleEncodedMessages() after Close error = %v, want %v", err, ErrConnectionClosed)
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, tt.run)
	}
}

func TestProviderStoreContracts(t *testing.T) {
	t.Parallel()

	t.Run("open propagates store load error and normalizes nil context", func(t *testing.T) {
		t.Parallel()

		expected := errors.New("load failed")
		store := testSnapshotStore{
			loadSnapshot: func(ctx context.Context, key storage.DocumentKey) (*storage.SnapshotRecord, error) {
				if ctx == nil {
					t.Fatal("LoadSnapshot() recebeu nil context, want background context")
				}
				return nil, expected
			},
		}

		provider := NewProvider(ProviderConfig{Store: store})
		var nilCtx context.Context
		_, err := provider.Open(nilCtx, storage.DocumentKey{
			Namespace:  "tests",
			DocumentID: "provider-store-open-error",
		}, "conn-load", 11)
		if !errors.Is(err, expected) {
			t.Fatalf("Open() error = %v, want %v", err, expected)
		}
	})

	t.Run("persist propagates store save error and normalizes nil context", func(t *testing.T) {
		t.Parallel()

		expected := errors.New("save failed")
		store := testSnapshotStore{
			loadSnapshot: func(ctx context.Context, key storage.DocumentKey) (*storage.SnapshotRecord, error) {
				if ctx == nil {
					t.Fatal("LoadSnapshot() recebeu nil context, want background context")
				}
				return nil, storage.ErrSnapshotNotFound
			},
			saveSnapshot: func(ctx context.Context, key storage.DocumentKey, snapshot *storageSnapshot) (*storage.SnapshotRecord, error) {
				if ctx == nil {
					t.Fatal("SaveSnapshot() recebeu nil context, want background context")
				}
				if snapshot == nil {
					t.Fatal("SaveSnapshot() recebeu nil snapshot")
				}
				return nil, expected
			},
		}

		provider := NewProvider(ProviderConfig{Store: store})
		var nilCtx context.Context
		conn, err := provider.Open(nilCtx, storage.DocumentKey{
			Namespace:  "tests",
			DocumentID: "provider-store-persist-error",
		}, "conn-save", 12)
		if err != nil {
			t.Fatalf("Open() unexpected error: %v", err)
		}
		t.Cleanup(func() {
			if _, closeErr := conn.Close(); closeErr != nil && !errors.Is(closeErr, ErrConnectionClosed) {
				t.Fatalf("conn.Close() cleanup unexpected error: %v", closeErr)
			}
		})

		if _, err := conn.Persist(nilCtx); !errors.Is(err, expected) {
			t.Fatalf("Persist(nil) error = %v, want %v", err, expected)
		}
	})
}

type storageSnapshot = yjsbridge.PersistedSnapshot

type testSnapshotStore struct {
	loadSnapshot func(ctx context.Context, key storage.DocumentKey) (*storage.SnapshotRecord, error)
	saveSnapshot func(ctx context.Context, key storage.DocumentKey, snapshot *storageSnapshot) (*storage.SnapshotRecord, error)
}

func (s testSnapshotStore) SaveSnapshot(ctx context.Context, key storage.DocumentKey, snapshot *storageSnapshot) (*storage.SnapshotRecord, error) {
	if s.saveSnapshot == nil {
		return nil, storage.ErrSnapshotNotFound
	}
	return s.saveSnapshot(ctx, key, snapshot)
}

func (s testSnapshotStore) LoadSnapshot(ctx context.Context, key storage.DocumentKey) (*storage.SnapshotRecord, error) {
	if s.loadSnapshot == nil {
		return nil, storage.ErrSnapshotNotFound
	}
	return s.loadSnapshot(ctx, key)
}

func TestConnectionCloseBroadcastsAwarenessTombstone(t *testing.T) {
	t.Parallel()

	provider := NewProvider(ProviderConfig{})
	key := storage.DocumentKey{
		Namespace:  "tests",
		DocumentID: "provider-close-awareness-tombstone",
	}

	publisher, err := provider.Open(context.Background(), key, "publisher", 10)
	if err != nil {
		t.Fatalf("Open(publisher) unexpected error: %v", err)
	}
	if err := publisher.session.Awareness().SetLocalState(json.RawMessage(`{"name":"alice","cursor":1}`)); err != nil {
		t.Fatalf("publisher.session.Awareness().SetLocalState() unexpected error: %v", err)
	}

	listener, err := provider.Open(context.Background(), key, "listener", 20)
	if err != nil {
		t.Fatalf("Open(listener) unexpected error: %v", err)
	}
	t.Cleanup(func() {
		if _, closeErr := listener.Close(); closeErr != nil && !errors.Is(closeErr, ErrConnectionClosed) {
			t.Fatalf("listener Close() cleanup unexpected error: %v", closeErr)
		}
	})

	queryBeforeClose, err := listener.HandleEncodedMessages(EncodeProtocolQueryAwareness())
	if err != nil {
		t.Fatalf("listener.HandleEncodedMessages(query-awareness) unexpected error: %v", err)
	}
	queryMessages, err := DecodeProtocolMessages(queryBeforeClose.Direct)
	if err != nil {
		t.Fatalf("DecodeProtocolMessages(query before close) unexpected error: %v", err)
	}
	if len(queryMessages) != 1 || queryMessages[0].Awareness == nil {
		t.Fatalf("queryMessages = %#v, want single awareness response", queryMessages)
	}
	beforeStates := awarenessStatesByClient(queryMessages[0].Awareness)
	if !bytes.Equal(beforeStates[publisher.ClientID()], []byte(`{"name":"alice","cursor":1}`)) {
		t.Fatalf("publisher awareness state = %s, want publisher awareness payload", beforeStates[publisher.ClientID()])
	}

	result, err := publisher.Close()
	if err != nil {
		t.Fatalf("publisher.Close() unexpected error: %v", err)
	}
	if len(result.Direct) != 0 {
		t.Fatalf("len(result.Direct) = %d, want 0", len(result.Direct))
	}
	if len(result.Broadcast) == 0 {
		t.Fatal("len(result.Broadcast) = 0, want tombstone awareness broadcast")
	}

	message, err := DecodeProtocolMessage(result.Broadcast)
	if err != nil {
		t.Fatalf("DecodeProtocolMessage(result.Broadcast) unexpected error: %v", err)
	}
	if message.Protocol != ProtocolTypeAwareness || message.Awareness == nil {
		t.Fatalf("message = %#v, want awareness tombstone envelope", message)
	}
	if len(message.Awareness.Clients) != 1 {
		t.Fatalf("len(message.Awareness.Clients) = %d, want 1", len(message.Awareness.Clients))
	}

	tombstone := message.Awareness.Clients[0]
	if tombstone.ClientID != publisher.ClientID() {
		t.Fatalf("tombstone.ClientID = %d, want %d", tombstone.ClientID, publisher.ClientID())
	}
	if tombstone.Clock != 1 {
		t.Fatalf("tombstone.Clock = %d, want 1", tombstone.Clock)
	}
	if !tombstone.IsNull() {
		t.Fatalf("tombstone = %#v, want null awareness state", tombstone)
	}

	queryAfterClose, err := listener.HandleEncodedMessages(EncodeProtocolQueryAwareness())
	if err != nil {
		t.Fatalf("listener.HandleEncodedMessages(query-awareness after close) unexpected error: %v", err)
	}
	afterMessages, err := DecodeProtocolMessages(queryAfterClose.Direct)
	if err != nil {
		t.Fatalf("DecodeProtocolMessages(query after close) unexpected error: %v", err)
	}
	if len(afterMessages) != 1 || afterMessages[0].Awareness == nil {
		t.Fatalf("afterMessages = %#v, want single awareness response", afterMessages)
	}
	afterStates := awarenessStatesByClient(afterMessages[0].Awareness)
	if _, ok := afterStates[publisher.ClientID()]; ok {
		t.Fatal("query-awareness includes closed publisher, want removed")
	}
}

func TestConnectionCloseReturnsTombstoneWhenLastLocalPeerLeaves(t *testing.T) {
	t.Parallel()

	provider := NewProvider(ProviderConfig{})
	key := storage.DocumentKey{
		Namespace:  "tests",
		DocumentID: "provider-close-last-local-peer-tombstone",
	}

	conn, err := provider.Open(context.Background(), key, "solo", 30)
	if err != nil {
		t.Fatalf("Open(solo) unexpected error: %v", err)
	}
	if err := conn.session.Awareness().SetLocalState(json.RawMessage(`{"name":"solo","cursor":7}`)); err != nil {
		t.Fatalf("conn.session.Awareness().SetLocalState() unexpected error: %v", err)
	}

	result, err := conn.Close()
	if err != nil {
		t.Fatalf("conn.Close() unexpected error: %v", err)
	}
	if len(result.Direct) != 0 {
		t.Fatalf("len(result.Direct) = %d, want 0", len(result.Direct))
	}
	if len(result.Broadcast) == 0 {
		t.Fatal("len(result.Broadcast) = 0, want tombstone awareness even without local peers")
	}

	message, err := DecodeProtocolMessage(result.Broadcast)
	if err != nil {
		t.Fatalf("DecodeProtocolMessage(result.Broadcast) unexpected error: %v", err)
	}
	if message.Protocol != ProtocolTypeAwareness || message.Awareness == nil {
		t.Fatalf("message = %#v, want awareness tombstone envelope", message)
	}
	if len(message.Awareness.Clients) != 1 {
		t.Fatalf("len(message.Awareness.Clients) = %d, want 1", len(message.Awareness.Clients))
	}

	tombstone := message.Awareness.Clients[0]
	if tombstone.ClientID != conn.ClientID() {
		t.Fatalf("tombstone.ClientID = %d, want %d", tombstone.ClientID, conn.ClientID())
	}
	if tombstone.Clock != 1 {
		t.Fatalf("tombstone.Clock = %d, want 1", tombstone.Clock)
	}
	if !tombstone.IsNull() {
		t.Fatalf("tombstone = %#v, want null awareness state", tombstone)
	}
}
