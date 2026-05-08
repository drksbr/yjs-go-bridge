package yprotocol

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"

	"github.com/drksbr/yjs-crdt-golang-server/pkg/storage"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/yjsbridge"
)

func TestProviderOpenLateJoinerLoadsLiveRoomState(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	key := storage.DocumentKey{
		Namespace:  "tests",
		DocumentID: "provider-open-late-joiner-live-room-state",
	}
	provider := NewProvider(ProviderConfig{})

	author, err := provider.Open(ctx, key, "conn-author", 601)
	if err != nil {
		t.Fatalf("provider.Open(conn-author) unexpected error: %v", err)
	}
	t.Cleanup(func() {
		if _, closeErr := author.Close(); closeErr != nil && !errors.Is(closeErr, ErrConnectionClosed) {
			t.Fatalf("author.Close() cleanup unexpected error: %v", closeErr)
		}
	})

	update := mustDecodeProtocolHex(t, "01020100040103646f630161030103646f6302112200")
	if _, err := author.HandleEncodedMessages(EncodeProtocolSyncUpdate(update)); err != nil {
		t.Fatalf("author.HandleEncodedMessages(sync-update) unexpected error: %v", err)
	}

	presenceState := json.RawMessage(`{"name":"author","cursor":9}`)
	presenceEnvelope, err := EncodeProtocolAwarenessUpdate(&AwarenessMessage{
		Clients: []AwarenessClient{{
			ClientID: author.ClientID(),
			Clock:    1,
			State:    presenceState,
		}},
	})
	if err != nil {
		t.Fatalf("EncodeProtocolAwarenessUpdate() unexpected error: %v", err)
	}
	if _, err := author.HandleEncodedMessages(presenceEnvelope); err != nil {
		t.Fatalf("author.HandleEncodedMessages(awareness) unexpected error: %v", err)
	}

	lateJoiner, err := provider.Open(ctx, key, "conn-late", 602)
	if err != nil {
		t.Fatalf("provider.Open(conn-late) unexpected error: %v", err)
	}
	t.Cleanup(func() {
		if _, closeErr := lateJoiner.Close(); closeErr != nil && !errors.Is(closeErr, ErrConnectionClosed) {
			t.Fatalf("lateJoiner.Close() cleanup unexpected error: %v", closeErr)
		}
	})

	reply, err := lateJoiner.HandleEncodedMessages(EncodeProtocolSyncStep1([]byte{0x00}))
	if err != nil {
		t.Fatalf("lateJoiner.HandleEncodedMessages(step1) unexpected error: %v", err)
	}
	if len(reply.Broadcast) != 0 {
		t.Fatalf("len(reply.Broadcast) = %d, want 0", len(reply.Broadcast))
	}

	replyMessages, err := DecodeProtocolMessages(reply.Direct)
	if err != nil {
		t.Fatalf("DecodeProtocolMessages(reply.Direct) unexpected error: %v", err)
	}
	if len(replyMessages) != 1 || replyMessages[0].Sync == nil {
		t.Fatalf("replyMessages = %#v, want single sync step2 reply", replyMessages)
	}
	if replyMessages[0].Sync.Type != SyncMessageTypeStep2 {
		t.Fatalf("replyMessages[0].Sync.Type = %v, want %v", replyMessages[0].Sync.Type, SyncMessageTypeStep2)
	}

	expectedStep2, err := yjsbridge.DiffUpdate(update, []byte{0x00})
	if err != nil {
		t.Fatalf("DiffUpdate() unexpected error: %v", err)
	}
	if !bytes.Equal(replyMessages[0].Sync.Payload, expectedStep2) {
		t.Fatalf("reply step2 payload = %v, want %v", replyMessages[0].Sync.Payload, expectedStep2)
	}

	awarenessReply, err := lateJoiner.HandleEncodedMessages(EncodeProtocolQueryAwareness())
	if err != nil {
		t.Fatalf("lateJoiner.HandleEncodedMessages(query-awareness) unexpected error: %v", err)
	}
	awarenessMessages, err := DecodeProtocolMessages(awarenessReply.Direct)
	if err != nil {
		t.Fatalf("DecodeProtocolMessages(awareness reply) unexpected error: %v", err)
	}
	if len(awarenessMessages) != 1 || awarenessMessages[0].Awareness == nil {
		t.Fatalf("awarenessMessages = %#v, want single awareness response", awarenessMessages)
	}
	awarenessStates := awarenessStatesByClient(awarenessMessages[0].Awareness)
	if !bytes.Equal(awarenessStates[author.ClientID()], presenceState) {
		t.Fatalf("late join awareness state = %s, want %s", awarenessStates[author.ClientID()], presenceState)
	}
}

func TestProviderHandleEncodedMessagesBatchedEnvelope(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	key := storage.DocumentKey{
		Namespace:  "tests",
		DocumentID: "provider-batched-envelope",
	}
	provider := NewProvider(ProviderConfig{})

	sender, err := provider.Open(ctx, key, "conn-sender", 701)
	if err != nil {
		t.Fatalf("provider.Open(conn-sender) unexpected error: %v", err)
	}
	t.Cleanup(func() {
		if _, closeErr := sender.Close(); closeErr != nil && !errors.Is(closeErr, ErrConnectionClosed) {
			t.Fatalf("sender.Close() cleanup unexpected error: %v", closeErr)
		}
	})

	peer, err := provider.Open(ctx, key, "conn-peer", 702)
	if err != nil {
		t.Fatalf("provider.Open(conn-peer) unexpected error: %v", err)
	}
	t.Cleanup(func() {
		if _, closeErr := peer.Close(); closeErr != nil && !errors.Is(closeErr, ErrConnectionClosed) {
			t.Fatalf("peer.Close() cleanup unexpected error: %v", closeErr)
		}
	})

	update := buildGCOnlyUpdate(71, 3)
	presenceState := json.RawMessage(`{"name":"sender","cursor":12}`)
	batch, err := EncodeProtocolEnvelopes(
		&ProtocolMessage{
			Protocol: ProtocolTypeSync,
			Sync: &SyncMessage{
				Type:    SyncMessageTypeUpdate,
				Payload: update,
			},
		},
		&ProtocolMessage{
			Protocol: ProtocolTypeAwareness,
			Awareness: &AwarenessMessage{
				Clients: []AwarenessClient{{
					ClientID: sender.ClientID(),
					Clock:    1,
					State:    presenceState,
				}},
			},
		},
		&ProtocolMessage{
			Protocol:       ProtocolTypeQueryAwareness,
			QueryAwareness: &QueryAwarenessMessage{},
		},
	)
	if err != nil {
		t.Fatalf("EncodeProtocolEnvelopes(batch) unexpected error: %v", err)
	}

	result, err := sender.HandleEncodedMessages(batch)
	if err != nil {
		t.Fatalf("sender.HandleEncodedMessages(batch) unexpected error: %v", err)
	}
	if len(result.Broadcast) == 0 {
		t.Fatal("len(result.Broadcast) = 0, want batched outbound sync+awareness")
	}
	if len(result.Direct) == 0 {
		t.Fatal("len(result.Direct) = 0, want direct query-awareness reply")
	}

	directMessages, err := DecodeProtocolMessages(result.Direct)
	if err != nil {
		t.Fatalf("DecodeProtocolMessages(result.Direct) unexpected error: %v", err)
	}
	if len(directMessages) != 1 || directMessages[0].Awareness == nil {
		t.Fatalf("directMessages = %#v, want single awareness reply", directMessages)
	}
	directStates := awarenessStatesByClient(directMessages[0].Awareness)
	if len(directStates) != 1 {
		t.Fatalf("len(directStates) = %d, want 1", len(directStates))
	}
	if !bytes.Equal(directStates[sender.ClientID()], presenceState) {
		t.Fatalf("direct awareness state = %s, want %s", directStates[sender.ClientID()], presenceState)
	}

	broadcastMessages, err := DecodeProtocolMessages(result.Broadcast)
	if err != nil {
		t.Fatalf("DecodeProtocolMessages(result.Broadcast) unexpected error: %v", err)
	}
	if len(broadcastMessages) != 2 {
		t.Fatalf("len(broadcastMessages) = %d, want 2", len(broadcastMessages))
	}
	if broadcastMessages[0].Sync == nil || broadcastMessages[0].Sync.Type != SyncMessageTypeUpdate {
		t.Fatalf("broadcastMessages[0] = %#v, want sync update", broadcastMessages[0])
	}
	if !bytes.Equal(broadcastMessages[0].Sync.Payload, update) {
		t.Fatalf("broadcast sync payload = %v, want %v", broadcastMessages[0].Sync.Payload, update)
	}
	if broadcastMessages[1].Awareness == nil {
		t.Fatalf("broadcastMessages[1] = %#v, want awareness message", broadcastMessages[1])
	}
	broadcastStates := awarenessStatesByClient(broadcastMessages[1].Awareness)
	if !bytes.Equal(broadcastStates[sender.ClientID()], presenceState) {
		t.Fatalf("broadcast awareness state = %s, want %s", broadcastStates[sender.ClientID()], presenceState)
	}

	assertConnectionSyncStep2EquivalentToV1(t, peer, update)
}

func TestProviderSyncUpdateNormalizesV2BroadcastAndRoomState(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	key := storage.DocumentKey{
		Namespace:  "tests",
		DocumentID: "provider-v2-sync-normalization",
	}
	provider := NewProvider(ProviderConfig{})

	sender, err := provider.Open(ctx, key, "conn-sender", 711)
	if err != nil {
		t.Fatalf("provider.Open(conn-sender) unexpected error: %v", err)
	}
	t.Cleanup(func() {
		if _, closeErr := sender.Close(); closeErr != nil && !errors.Is(closeErr, ErrConnectionClosed) {
			t.Fatalf("sender.Close() cleanup unexpected error: %v", closeErr)
		}
	})

	peer, err := provider.Open(ctx, key, "conn-peer", 712)
	if err != nil {
		t.Fatalf("provider.Open(conn-peer) unexpected error: %v", err)
	}
	t.Cleanup(func() {
		if _, closeErr := peer.Close(); closeErr != nil && !errors.Is(closeErr, ErrConnectionClosed) {
			t.Fatalf("peer.Close() cleanup unexpected error: %v", closeErr)
		}
	})

	v2Update := mustDecodeProtocolHex(t, "000002a50100000104060374686901020101000001010000")
	v1Update, err := yjsbridge.ConvertUpdateToV1(v2Update)
	if err != nil {
		t.Fatalf("ConvertUpdateToV1(v2) unexpected error: %v", err)
	}

	result, err := sender.HandleEncodedMessages(EncodeProtocolSyncUpdate(v2Update))
	if err != nil {
		t.Fatalf("sender.HandleEncodedMessages(v2 sync-update) unexpected error: %v", err)
	}
	if len(result.Broadcast) == 0 {
		t.Fatal("len(result.Broadcast) = 0, want canonical V1 broadcast")
	}

	broadcastMessages, err := DecodeProtocolMessages(result.Broadcast)
	if err != nil {
		t.Fatalf("DecodeProtocolMessages(result.Broadcast) unexpected error: %v", err)
	}
	if len(broadcastMessages) != 1 || broadcastMessages[0].Sync == nil {
		t.Fatalf("broadcastMessages = %#v, want single sync message", broadcastMessages)
	}
	if broadcastMessages[0].Sync.Type != SyncMessageTypeUpdate {
		t.Fatalf("broadcast sync type = %v, want %v", broadcastMessages[0].Sync.Type, SyncMessageTypeUpdate)
	}
	if !bytes.Equal(broadcastMessages[0].Sync.Payload, v1Update) {
		t.Fatalf("broadcast sync payload = %x, want canonical V1 %x", broadcastMessages[0].Sync.Payload, v1Update)
	}
	if bytes.Equal(broadcastMessages[0].Sync.Payload, v2Update) {
		t.Fatalf("broadcast sync payload preserved V2 bytes: %x", broadcastMessages[0].Sync.Payload)
	}
	assertConnectionSyncStep2EquivalentToV1(t, sender, v1Update)
	assertConnectionSyncStep2EquivalentToV1(t, peer, v1Update)
}

func mustDecodeProtocolHex(t *testing.T, value string) []byte {
	t.Helper()

	decoded, err := hex.DecodeString(value)
	if err != nil {
		t.Fatalf("hex.DecodeString(%q) unexpected error: %v", value, err)
	}
	return decoded
}
