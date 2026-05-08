package yprotocol

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/drksbr/yjs-crdt-golang-server/pkg/storage"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/storage/memory"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/yjsbridge"
)

func TestConnectionHandleEncodedMessagesV2DirectOutputOptIn(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	key := storage.DocumentKey{Namespace: "tests", DocumentID: "provider-v2-direct"}
	provider := NewProvider(ProviderConfig{})
	conn, err := provider.Open(ctx, key, "conn-a", 901)
	if err != nil {
		t.Fatalf("provider.Open() unexpected error: %v", err)
	}

	update := buildGCOnlyUpdate(91, 2)
	if _, err := conn.HandleEncodedMessages(EncodeProtocolSyncUpdate(update)); err != nil {
		t.Fatalf("HandleEncodedMessages(sync-update) unexpected error: %v", err)
	}

	result, err := conn.HandleEncodedMessagesWithOptions(
		EncodeProtocolSyncStep1([]byte{0x00}),
		ConnectionHandleOptions{DirectSyncOutputFormat: yjsbridge.UpdateFormatV2},
	)
	if err != nil {
		t.Fatalf("HandleEncodedMessagesWithOptions(step1 v2) unexpected error: %v", err)
	}
	if len(result.Broadcast) != 0 {
		t.Fatalf("len(result.Broadcast) = %d, want 0", len(result.Broadcast))
	}

	messages, err := DecodeProtocolMessages(result.Direct)
	if err != nil {
		t.Fatalf("DecodeProtocolMessages(direct) unexpected error: %v", err)
	}
	if len(messages) != 1 || messages[0].Sync == nil {
		t.Fatalf("direct messages = %#v, want single sync step2", messages)
	}
	if messages[0].Sync.Type != SyncMessageTypeStep2 {
		t.Fatalf("direct sync type = %v, want %v", messages[0].Sync.Type, SyncMessageTypeStep2)
	}
	assertProtocolV2PayloadEquivalentToV1(t, messages[0].Sync.Payload, update)
	assertProtocolV2PayloadEquivalentToV1(t, conn.room.updateV2, update)
	assertConnectionSyncStep2EquivalentToV1(t, conn, update)
}

func TestConnectionV2DirectOutputUsesYjsWireFormatForTextFormatting(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	key := storage.DocumentKey{Namespace: "tests", DocumentID: "provider-v2-direct-quill-format"}
	provider := NewProvider(ProviderConfig{})
	conn, err := provider.Open(ctx, key, "conn-a", 931)
	if err != nil {
		t.Fatalf("provider.Open() unexpected error: %v", err)
	}

	update := mustDecodeProtocolHex(t, "01039fecb8ca09000601047465787404626f6c640474727565849fecb8ca09000568656c6c6f869fecb8ca090504626f6c64046e756c6c00")
	if _, err := conn.HandleEncodedMessages(EncodeProtocolSyncUpdate(update)); err != nil {
		t.Fatalf("HandleEncodedMessages(sync-update) unexpected error: %v", err)
	}

	result, err := conn.HandleEncodedMessagesWithOptions(
		EncodeProtocolSyncStep1([]byte{0x00}),
		ConnectionHandleOptions{DirectSyncOutputFormat: yjsbridge.UpdateFormatV2},
	)
	if err != nil {
		t.Fatalf("HandleEncodedMessagesWithOptions(step1 v2) unexpected error: %v", err)
	}

	messages, err := DecodeProtocolMessages(result.Direct)
	if err != nil {
		t.Fatalf("DecodeProtocolMessages(direct) unexpected error: %v", err)
	}
	if len(messages) != 1 || messages[0].Sync == nil {
		t.Fatalf("direct messages = %#v, want single sync step2", messages)
	}
	roundTrip, err := yjsbridge.ConvertUpdateToV1YjsWire(messages[0].Sync.Payload)
	if err != nil {
		t.Fatalf("ConvertUpdateToV1YjsWire(v2 payload) unexpected error: %v", err)
	}
	if !bytes.Equal(roundTrip, update) {
		t.Fatalf("direct V2 wire payload round-trip mismatch:\n got: %x\nwant: %x", roundTrip, update)
	}
}

func TestConnectionHandleEncodedMessagesV2BroadcastOutputOptInKeepsStorageV1(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	key := storage.DocumentKey{Namespace: "tests", DocumentID: "provider-v2-broadcast"}
	store := memory.New()
	provider := NewProvider(ProviderConfig{Store: store})
	conn, err := provider.Open(ctx, key, "conn-a", 902)
	if err != nil {
		t.Fatalf("provider.Open() unexpected error: %v", err)
	}

	v2Update := mustDecodeProtocolHex(t, "000002a50100000104060374686901020101000001010000")
	v1Update, err := yjsbridge.ConvertUpdateToV1(v2Update)
	if err != nil {
		t.Fatalf("ConvertUpdateToV1(v2Update) unexpected error: %v", err)
	}
	wantBroadcastV2, err := yjsbridge.ConvertUpdateToV2(v2Update)
	if err != nil {
		t.Fatalf("ConvertUpdateToV2(v2Update) unexpected error: %v", err)
	}
	result, err := conn.HandleEncodedMessagesWithOptions(
		EncodeProtocolSyncUpdate(v2Update),
		ConnectionHandleOptions{BroadcastSyncOutputFormat: yjsbridge.UpdateFormatV2},
	)
	if err != nil {
		t.Fatalf("HandleEncodedMessagesWithOptions(sync-update v2 broadcast) unexpected error: %v", err)
	}
	if len(result.Direct) != 0 {
		t.Fatalf("len(result.Direct) = %d, want 0", len(result.Direct))
	}

	messages, err := DecodeProtocolMessages(result.Broadcast)
	if err != nil {
		t.Fatalf("DecodeProtocolMessages(broadcast) unexpected error: %v", err)
	}
	if len(messages) != 1 || messages[0].Sync == nil {
		t.Fatalf("broadcast messages = %#v, want single sync update", messages)
	}
	if messages[0].Sync.Type != SyncMessageTypeUpdate {
		t.Fatalf("broadcast sync type = %v, want %v", messages[0].Sync.Type, SyncMessageTypeUpdate)
	}
	if !bytes.Equal(messages[0].Sync.Payload, wantBroadcastV2) {
		t.Fatalf("broadcast sync payload = %x, want canonical V2 %x", messages[0].Sync.Payload, wantBroadcastV2)
	}
	assertProtocolV2PayloadEquivalentToV1(t, messages[0].Sync.Payload, v1Update)
	assertProtocolV2PayloadEquivalentToV1(t, conn.room.updateV2, v1Update)
	assertConnectionSyncStep2EquivalentToV1(t, conn, v1Update)

	records, err := store.ListUpdates(ctx, key, 0, 0)
	if err != nil {
		t.Fatalf("store.ListUpdates() unexpected error: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("len(records) = %d, want 1", len(records))
	}
	if !bytes.Equal(records[0].UpdateV1, v1Update) {
		t.Fatalf("records[0].UpdateV1 = %x, want canonical V1 %x", records[0].UpdateV1, v1Update)
	}
	if bytes.Equal(records[0].UpdateV1, v2Update) {
		t.Fatalf("records[0].UpdateV1 preserved V2 bytes: %x", records[0].UpdateV1)
	}
}

func TestConnectionHandleEncodedMessagesV2OptionsDefaultAndInvalid(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	key := storage.DocumentKey{Namespace: "tests", DocumentID: "provider-v2-options"}
	store := memory.New()
	provider := NewProvider(ProviderConfig{Store: store})
	conn, err := provider.Open(ctx, key, "conn-a", 903)
	if err != nil {
		t.Fatalf("provider.Open() unexpected error: %v", err)
	}

	update := buildGCOnlyUpdate(93, 1)
	defaultResult, err := conn.HandleEncodedMessagesWithOptions(EncodeProtocolSyncUpdate(update), ConnectionHandleOptions{})
	if err != nil {
		t.Fatalf("HandleEncodedMessagesWithOptions(default) unexpected error: %v", err)
	}
	defaultMessages, err := DecodeProtocolMessages(defaultResult.Broadcast)
	if err != nil {
		t.Fatalf("DecodeProtocolMessages(default broadcast) unexpected error: %v", err)
	}
	if len(defaultMessages) != 1 || defaultMessages[0].Sync == nil {
		t.Fatalf("default broadcast messages = %#v, want single sync update", defaultMessages)
	}
	if !bytes.Equal(defaultMessages[0].Sync.Payload, update) {
		t.Fatalf("default broadcast payload = %x, want V1 %x", defaultMessages[0].Sync.Payload, update)
	}

	badFormat := yjsbridge.UpdateFormat(99)
	if _, err := conn.HandleEncodedMessagesWithOptions(
		EncodeProtocolSyncUpdate(buildGCOnlyUpdate(94, 1)),
		ConnectionHandleOptions{BroadcastSyncOutputFormat: badFormat},
	); !errors.Is(err, yjsbridge.ErrUnknownUpdateFormat) {
		t.Fatalf("HandleEncodedMessagesWithOptions(invalid) error = %v, want %v", err, yjsbridge.ErrUnknownUpdateFormat)
	}
	records, err := store.ListUpdates(ctx, key, 0, 0)
	if err != nil {
		t.Fatalf("store.ListUpdates() unexpected error: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("len(records) = %d, want only the default write", len(records))
	}
}

func TestProviderOpenHydratesRoomV2FromPersistedSnapshot(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	key := storage.DocumentKey{Namespace: "tests", DocumentID: "provider-v2-hydrate"}
	store := memory.New()
	update := buildGCOnlyUpdate(95, 3)
	snapshot, err := yjsbridge.PersistedSnapshotFromUpdate(update)
	if err != nil {
		t.Fatalf("PersistedSnapshotFromUpdate(update) unexpected error: %v", err)
	}
	if _, err := store.SaveSnapshot(ctx, key, snapshot); err != nil {
		t.Fatalf("store.SaveSnapshot() unexpected error: %v", err)
	}

	provider := NewProvider(ProviderConfig{Store: store})
	conn, err := provider.Open(ctx, key, "conn-a", 905)
	if err != nil {
		t.Fatalf("provider.Open() unexpected error: %v", err)
	}

	assertProtocolV2PayloadEquivalentToV1(t, conn.room.updateV2, update)
	assertConnectionSyncStep2EquivalentToV1(t, conn, update)
}

func assertProtocolV2PayloadEquivalentToV1(t *testing.T, gotV2, wantV1 []byte) {
	t.Helper()

	format, err := yjsbridge.FormatFromUpdate(gotV2)
	if err != nil {
		t.Fatalf("FormatFromUpdate(gotV2) unexpected error: %v", err)
	}
	if format != yjsbridge.UpdateFormatV2 {
		t.Fatalf("FormatFromUpdate(gotV2) = %s, want %s", format, yjsbridge.UpdateFormatV2)
	}
	gotV1, err := yjsbridge.ConvertUpdateToV1(gotV2)
	if err != nil {
		t.Fatalf("ConvertUpdateToV1(gotV2) unexpected error: %v", err)
	}
	if !bytes.Equal(gotV1, wantV1) {
		t.Fatalf("ConvertUpdateToV1(gotV2) = %x, want %x", gotV1, wantV1)
	}
}

func assertConnectionSyncStep2EquivalentToV1(t *testing.T, conn *Connection, wantV1 []byte) {
	t.Helper()

	result, err := conn.HandleEncodedMessages(EncodeProtocolSyncStep1([]byte{0x00}))
	if err != nil {
		t.Fatalf("HandleEncodedMessages(step1) unexpected error: %v", err)
	}
	messages, err := DecodeProtocolMessages(result.Direct)
	if err != nil {
		t.Fatalf("DecodeProtocolMessages(step1 direct) unexpected error: %v", err)
	}
	if len(messages) != 1 || messages[0].Sync == nil || messages[0].Sync.Type != SyncMessageTypeStep2 {
		t.Fatalf("step1 direct messages = %#v, want single sync step2", messages)
	}
	expected, err := yjsbridge.DiffUpdate(wantV1, []byte{0x00})
	if err != nil {
		t.Fatalf("DiffUpdate() unexpected error: %v", err)
	}
	if !bytes.Equal(messages[0].Sync.Payload, expected) {
		t.Fatalf("step2 payload = %x, want %x", messages[0].Sync.Payload, expected)
	}
}
