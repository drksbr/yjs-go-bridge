package yhttp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/drksbr/yjs-crdt-golang-server/internal/varint"
	"github.com/drksbr/yjs-crdt-golang-server/internal/ytypes"
	"github.com/drksbr/yjs-crdt-golang-server/internal/yupdate"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/storage"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/storage/memory"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/yawareness"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/ycluster"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/yjsbridge"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/yprotocol"
)

const testIOTimeout = 15 * time.Second

func TestHTTPServerBroadcastsLocalSyncAndAwareness(t *testing.T) {
	t.Parallel()

	srv := newHTTPTestServer(t, nil)
	left := dialWS(t, srv.URL+"/ws?doc=room-a&client=401&conn=left")
	right := dialWS(t, srv.URL+"/ws?doc=room-a&client=402&conn=right")

	writeBinary(t, left, yprotocol.EncodeProtocolSyncStep1([]byte{0x00}))
	_ = readBinary(t, left)
	writeBinary(t, right, yprotocol.EncodeProtocolSyncStep1([]byte{0x00}))
	_ = readBinary(t, right)

	update := buildGCOnlyUpdate(19, 2)
	writeBinary(t, left, yprotocol.EncodeProtocolSyncUpdate(update))

	broadcast := readBinary(t, right)
	messages, err := yprotocol.DecodeProtocolMessages(broadcast)
	if err != nil {
		t.Fatalf("DecodeProtocolMessages(sync broadcast) unexpected error: %v", err)
	}
	if len(messages) != 1 || messages[0].Sync == nil {
		t.Fatalf("sync broadcast messages = %#v, want single sync message", messages)
	}
	if messages[0].Sync.Type != yprotocol.SyncMessageTypeUpdate {
		t.Fatalf("sync broadcast type = %v, want %v", messages[0].Sync.Type, yprotocol.SyncMessageTypeUpdate)
	}
	if !bytes.Equal(messages[0].Sync.Payload, update) {
		t.Fatalf("sync broadcast payload = %v, want %v", messages[0].Sync.Payload, update)
	}

	awarenessPayload, err := yprotocol.EncodeProtocolAwarenessUpdate(&yawareness.Update{
		Clients: []yawareness.ClientState{{
			ClientID: 401,
			Clock:    1,
			State:    json.RawMessage(`{"name":"left"}`),
		}},
	})
	if err != nil {
		t.Fatalf("EncodeProtocolAwarenessUpdate() unexpected error: %v", err)
	}
	writeBinary(t, left, awarenessPayload)

	awarenessBroadcast := readBinary(t, right)
	awarenessMessages, err := yprotocol.DecodeProtocolMessages(awarenessBroadcast)
	if err != nil {
		t.Fatalf("DecodeProtocolMessages(awareness broadcast) unexpected error: %v", err)
	}
	if len(awarenessMessages) != 1 || awarenessMessages[0].Awareness == nil {
		t.Fatalf("awareness broadcast messages = %#v, want single awareness message", awarenessMessages)
	}
	if len(awarenessMessages[0].Awareness.Clients) != 1 {
		t.Fatalf("len(awareness clients) = %d, want 1", len(awarenessMessages[0].Awareness.Clients))
	}
	client := awarenessMessages[0].Awareness.Clients[0]
	if client.ClientID != 401 || client.Clock != 1 {
		t.Fatalf("awareness client = %#v, want clientID=401 clock=1", client)
	}
	if !bytes.Equal(client.State, []byte(`{"name":"left"}`)) {
		t.Fatalf("awareness state = %s, want %s", client.State, `{"name":"left"}`)
	}
}

func TestWriteBroadcastPeersPrecomputesSyncOutputPayloads(t *testing.T) {
	t.Parallel()

	updateV1 := buildGCOnlyUpdate(411, 2)
	payloadV1 := yprotocol.EncodeProtocolSyncUpdate(updateV1)
	wantV2, err := protocolPayloadForSyncOutputFormat(payloadV1, yjsbridge.UpdateFormatV2)
	if err != nil {
		t.Fatalf("protocolPayloadForSyncOutputFormat() unexpected error: %v", err)
	}

	v1Peer := &recordingRoomPeer{}
	v2Peer := &recordingRoomPeer{}
	payloads := newBroadcastPayloads(payloadV1)
	peers := []roomPeer{
		v1Peer,
		syncOutputFormatPeer{base: v2Peer, format: yjsbridge.UpdateFormatV2},
	}
	if err := payloads.prepare(peers); err != nil {
		t.Fatalf("prepare() unexpected error: %v", err)
	}

	server := &Server{
		writeTimeout:      time.Second,
		fanoutConcurrency: 2,
	}
	errs := server.writeBroadcastPeers(peers, payloads)
	for idx, err := range errs {
		if err != nil {
			t.Fatalf("writeBroadcastPeers()[%d] error = %v", idx, err)
		}
	}

	if got := v1Peer.payload(); !bytes.Equal(got, payloadV1) {
		t.Fatalf("v1 payload = %x, want %x", got, payloadV1)
	}
	if got := v2Peer.payload(); !bytes.Equal(got, wantV2) {
		t.Fatalf("v2 payload = %x, want %x", got, wantV2)
	}
}

func TestProtocolPayloadForSyncOutputFormatKeepsAwarenessPayload(t *testing.T) {
	t.Parallel()

	payload, err := yprotocol.EncodeProtocolAwarenessUpdate(&yawareness.Update{
		Clients: []yawareness.ClientState{{
			ClientID: 778,
			Clock:    1,
			State:    json.RawMessage(`{"status":"online"}`),
		}},
	})
	if err != nil {
		t.Fatalf("EncodeProtocolAwarenessUpdate() unexpected error: %v", err)
	}
	got, err := protocolPayloadForSyncOutputFormat(payload, yjsbridge.UpdateFormatV2)
	if err != nil {
		t.Fatalf("protocolPayloadForSyncOutputFormat() unexpected error: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("awareness payload = %x, want original %x", got, payload)
	}
}

type recordingRoomPeer struct {
	mu     sync.Mutex
	writes [][]byte
	closed string
	err    error
}

func (p *recordingRoomPeer) deliver(_ context.Context, payload []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	p.writes = append(p.writes, append([]byte(nil), payload...))
	return nil
}

func (p *recordingRoomPeer) close(reason string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = reason
	return nil
}

func (p *recordingRoomPeer) payload() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.writes) == 0 {
		return nil
	}
	return append([]byte(nil), p.writes[len(p.writes)-1]...)
}

func TestHTTPServerBootstrapOnConnectSendsExistingAwareness(t *testing.T) {
	t.Parallel()

	provider := yprotocol.NewProvider(yprotocol.ProviderConfig{Store: memory.New()})
	handler, err := NewServer(ServerConfig{
		Provider:           provider,
		ResolveRequest:     resolveTestRequest,
		BootstrapOnConnect: true,
	})
	if err != nil {
		t.Fatalf("NewServer() unexpected error: %v", err)
	}
	mux := http.NewServeMux()
	mux.Handle("/ws", handler)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	left := dialWS(t, srv.URL+"/ws?doc=room-awareness-bootstrap&client=501&conn=left")
	awarenessPayload, err := yprotocol.EncodeProtocolAwarenessUpdate(&yawareness.Update{
		Clients: []yawareness.ClientState{{
			ClientID: 501,
			Clock:    1,
			State:    json.RawMessage(`{"name":"left"}`),
		}},
	})
	if err != nil {
		t.Fatalf("EncodeProtocolAwarenessUpdate() unexpected error: %v", err)
	}
	writeBinary(t, left, awarenessPayload)

	right := dialWS(t, srv.URL+"/ws?doc=room-awareness-bootstrap&client=502&conn=right")
	for i := 0; i < 2; i++ {
		payload := readBinary(t, right)
		messages, err := yprotocol.DecodeProtocolMessages(payload)
		if err != nil {
			t.Fatalf("DecodeProtocolMessages(bootstrap frame %d) unexpected error: %v", i, err)
		}
		for _, message := range messages {
			if message.Awareness == nil {
				continue
			}
			for _, client := range message.Awareness.Clients {
				if client.ClientID == 501 && bytes.Equal(client.State, []byte(`{"name":"left"}`)) {
					return
				}
			}
		}
	}
	t.Fatal("right connection did not receive existing awareness during bootstrap")
}

func TestHTTPServerPersistsSnapshotOnClose(t *testing.T) {
	t.Parallel()

	store := memory.New()
	firstServer := newHTTPTestServer(t, store)
	first := dialWS(t, firstServer.URL+"/ws?doc=doc-persist&client=501&persist=1")

	update := buildGCOnlyUpdate(29, 4)
	writeBinary(t, first, yprotocol.EncodeProtocolSyncUpdate(update))
	if err := first.Close(websocket.StatusNormalClosure, "done"); err != nil {
		t.Fatalf("first.Close() unexpected error: %v", err)
	}

	key := storage.DocumentKey{Namespace: "tests", DocumentID: "doc-persist"}
	waitForSnapshot(t, store, key)

	firstServer.Close()
	secondServer := newHTTPTestServer(t, store)
	probe := dialWS(t, secondServer.URL+"/ws?doc=doc-persist&client=502")

	writeBinary(t, probe, yprotocol.EncodeProtocolSyncStep1([]byte{0x00}))
	reply := readBinary(t, probe)

	messages, err := yprotocol.DecodeProtocolMessages(reply)
	if err != nil {
		t.Fatalf("DecodeProtocolMessages(step2 reply) unexpected error: %v", err)
	}
	if len(messages) != 1 || messages[0].Sync == nil {
		t.Fatalf("step2 reply messages = %#v, want single sync message", messages)
	}
	if messages[0].Sync.Type != yprotocol.SyncMessageTypeStep2 {
		t.Fatalf("step2 reply type = %v, want %v", messages[0].Sync.Type, yprotocol.SyncMessageTypeStep2)
	}

	expected, err := yjsbridge.DiffUpdate(update, []byte{0x00})
	if err != nil {
		t.Fatalf("DiffUpdate() unexpected error: %v", err)
	}
	if !bytes.Equal(messages[0].Sync.Payload, expected) {
		t.Fatalf("step2 reply payload = %v, want %v", messages[0].Sync.Payload, expected)
	}
}

func TestHTTPServerSyncOutputFormatV2OptIn(t *testing.T) {
	t.Parallel()

	srv := newHTTPTestServer(t, nil)
	left := dialWS(t, srv.URL+"/ws?doc=room-v2-output&client=451&conn=left")
	right := dialWS(t, srv.URL+"/ws?doc=room-v2-output&client=452&conn=right&sync=v2")

	writeBinary(t, left, yprotocol.EncodeProtocolSyncStep1([]byte{0x00}))
	_ = readBinary(t, left)
	writeBinary(t, right, yprotocol.EncodeProtocolSyncStep1([]byte{0x00}))
	_ = readBinary(t, right)

	update := buildGCOnlyUpdate(91, 3)
	writeBinary(t, left, yprotocol.EncodeProtocolSyncUpdate(update))

	broadcast := readBinary(t, right)
	broadcastMessages, err := yprotocol.DecodeProtocolMessages(broadcast)
	if err != nil {
		t.Fatalf("DecodeProtocolMessages(v2 broadcast) unexpected error: %v", err)
	}
	if len(broadcastMessages) != 1 || broadcastMessages[0].Sync == nil {
		t.Fatalf("v2 broadcast messages = %#v, want single sync message", broadcastMessages)
	}
	if broadcastMessages[0].Sync.Type != yprotocol.SyncMessageTypeUpdate {
		t.Fatalf("v2 broadcast type = %v, want %v", broadcastMessages[0].Sync.Type, yprotocol.SyncMessageTypeUpdate)
	}
	assertYHTTPV2EquivalentToV1(t, "broadcast", broadcastMessages[0].Sync.Payload, update)

	writeBinary(t, right, yprotocol.EncodeProtocolSyncStep1([]byte{0x00}))
	reply := readBinary(t, right)
	replyMessages, err := yprotocol.DecodeProtocolMessages(reply)
	if err != nil {
		t.Fatalf("DecodeProtocolMessages(v2 direct) unexpected error: %v", err)
	}
	if len(replyMessages) != 1 || replyMessages[0].Sync == nil {
		t.Fatalf("v2 direct messages = %#v, want single sync message", replyMessages)
	}
	if replyMessages[0].Sync.Type != yprotocol.SyncMessageTypeStep2 {
		t.Fatalf("v2 direct type = %v, want %v", replyMessages[0].Sync.Type, yprotocol.SyncMessageTypeStep2)
	}
	expectedDiff, err := yjsbridge.DiffUpdate(update, []byte{0x00})
	if err != nil {
		t.Fatalf("DiffUpdate() unexpected error: %v", err)
	}
	assertYHTTPV2EquivalentToV1(t, "direct", replyMessages[0].Sync.Payload, expectedDiff)
}

func TestHTTPServerRevalidatesAuthorityAndClosesIdleConnection(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	key := testDocumentKey("doc-authority-revalidate")
	store, resolver, provider := newAuthoritativeHTTPProvider(t, "node-a")
	seedAuthoritativeHTTPDocument(t, ctx, store, resolver, key, "node-a", 1, "lease-node-a")
	recorder := newRecordingMetrics()

	handler, err := NewServer(ServerConfig{
		Provider:                      provider,
		ResolveRequest:                resolveTestRequest,
		AuthorityRevalidationInterval: 10 * time.Millisecond,
		Metrics:                       recorder,
	})
	if err != nil {
		t.Fatalf("NewServer() unexpected error: %v", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/ws", handler)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	conn := dialWS(t, srv.URL+"/ws?doc=doc-authority-revalidate&client=611&conn=idle")
	writeBinary(t, conn, yprotocol.EncodeProtocolSyncStep1([]byte{0x00}))
	_ = readBinary(t, conn)
	handoffAuthoritativeHTTPDocument(t, ctx, store, resolver, key, "lease-node-a", "node-b", 2, "lease-node-b")

	closeErr := readCloseError(t, conn)
	if closeErr.Code != websocket.StatusTryAgainLater {
		t.Fatalf("closeErr.Code = %d, want %d", closeErr.Code, websocket.StatusTryAgainLater)
	}
	if closeErr.Reason != authorityLostCloseReason {
		t.Fatalf("closeErr.Reason = %q, want %q", closeErr.Reason, authorityLostCloseReason)
	}

	waitForCondition(t, 2*time.Second, func() bool {
		snapshot := recorder.snapshot()
		return snapshot.authorityRevalidations[recordingAuthorityRevalidationKey{
			role:   authorityRevalidationRoleLocal,
			result: "error",
		}] == 1
	})
}

func TestHTTPServerRevalidatesAuthorityFromRebalanceCallback(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	key := testDocumentKey("doc-authority-rebalance-callback")
	store, resolver, provider := newAuthoritativeHTTPProvider(t, "node-a")
	seedAuthoritativeHTTPDocument(t, ctx, store, resolver, key, "node-a", 1, "lease-node-a")

	handler, err := NewServer(ServerConfig{
		Provider:       provider,
		ResolveRequest: resolveTestRequest,
	})
	if err != nil {
		t.Fatalf("NewServer() unexpected error: %v", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/ws", handler)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	conn := dialWS(t, srv.URL+"/ws?doc=doc-authority-rebalance-callback&client=612&conn=idle")
	writeBinary(t, conn, yprotocol.EncodeProtocolSyncStep1([]byte{0x00}))
	_ = readBinary(t, conn)

	revalidation, err := handler.RevalidateDocumentAuthority(ctx, key)
	if err != nil {
		t.Fatalf("RevalidateDocumentAuthority(initial) unexpected error: %v", err)
	}
	if revalidation.Checked != 1 || revalidation.AuthorityLost != 0 {
		t.Fatalf("RevalidateDocumentAuthority(initial) = %#v, want one healthy connection", revalidation)
	}

	handoffAuthoritativeHTTPDocument(t, ctx, store, resolver, key, "lease-node-a", "node-b", 2, "lease-node-b")
	callback, err := NewRebalanceAuthorityRevalidationCallback(RebalanceAuthorityRevalidationConfig{
		Server:  handler,
		Timeout: time.Second,
	})
	if err != nil {
		t.Fatalf("NewRebalanceAuthorityRevalidationCallback() unexpected error: %v", err)
	}
	callback(ycluster.RebalanceControllerRunResult{
		Results: []ycluster.RebalancePlanExecutionResult{
			{
				Result: &ycluster.RebalanceDocumentResult{
					DocumentKey: key,
					Changed:     true,
					From:        "node-a",
					To:          "node-b",
				},
			},
		},
	}, nil)

	closeErr := readCloseError(t, conn)
	if closeErr.Code != websocket.StatusTryAgainLater {
		t.Fatalf("closeErr.Code = %d, want %d", closeErr.Code, websocket.StatusTryAgainLater)
	}
	if closeErr.Reason != authorityLostCloseReason {
		t.Fatalf("closeErr.Reason = %q, want %q", closeErr.Reason, authorityLostCloseReason)
	}
}

func TestHTTPServerOwnershipRuntimeClaimsAndReleasesOnConnectionClose(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := memory.New()
	srv, coordinator, _ := newOwnershipHTTPTestServer(t, store, "node-a")
	key := testDocumentKey("doc-http-ownership")

	conn := dialWS(t, srv.URL+"/ws?doc=doc-http-ownership&client=701&conn=owned")
	writeBinary(t, conn, yprotocol.EncodeProtocolSyncStep1([]byte{0x00}))
	_ = readBinary(t, conn)

	resolution, err := coordinator.LookupOwner(ctx, ycluster.OwnerLookupRequest{DocumentKey: key})
	if err != nil {
		t.Fatalf("LookupOwner(open connection) unexpected error: %v", err)
	}
	if !resolution.Local || resolution.Placement.Lease == nil {
		t.Fatalf("LookupOwner(open connection) = %#v, want local owner with lease", resolution)
	}

	if err := conn.Close(websocket.StatusNormalClosure, "done"); err != nil {
		t.Fatalf("conn.Close() unexpected error: %v", err)
	}
	waitForCondition(t, 2*time.Second, func() bool {
		_, err := coordinator.LookupOwner(ctx, ycluster.OwnerLookupRequest{DocumentKey: key})
		return errors.Is(err, ycluster.ErrOwnerNotFound)
	})
}

func TestHTTPServerOwnershipRuntimeSharesLeaseAcrossConnections(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := memory.New()
	srv, coordinator, _ := newOwnershipHTTPTestServer(t, store, "node-a")
	key := testDocumentKey("doc-http-ownership-shared")

	first := dialWS(t, srv.URL+"/ws?doc=doc-http-ownership-shared&client=711&conn=first")
	writeBinary(t, first, yprotocol.EncodeProtocolSyncStep1([]byte{0x00}))
	_ = readBinary(t, first)

	second := dialWS(t, srv.URL+"/ws?doc=doc-http-ownership-shared&client=712&conn=second")
	writeBinary(t, second, yprotocol.EncodeProtocolSyncStep1([]byte{0x00}))
	_ = readBinary(t, second)

	initial, err := coordinator.LookupOwner(ctx, ycluster.OwnerLookupRequest{DocumentKey: key})
	if err != nil {
		t.Fatalf("LookupOwner(two connections) unexpected error: %v", err)
	}
	if !initial.Local || initial.Placement.Lease == nil {
		t.Fatalf("LookupOwner(two connections) = %#v, want local owner with lease", initial)
	}
	initialToken := initial.Placement.Lease.Token
	if initialToken == "" {
		t.Fatal("LookupOwner(two connections).Placement.Lease.Token is empty")
	}

	if err := first.Close(websocket.StatusNormalClosure, "done"); err != nil {
		t.Fatalf("first.Close() unexpected error: %v", err)
	}
	waitForCondition(t, 2*time.Second, func() bool {
		resolution, err := coordinator.LookupOwner(ctx, ycluster.OwnerLookupRequest{DocumentKey: key})
		return err == nil && resolution.Local && resolution.Placement.Lease.Token == initialToken
	})

	if err := second.Close(websocket.StatusNormalClosure, "done"); err != nil {
		t.Fatalf("second.Close() unexpected error: %v", err)
	}
	waitForCondition(t, 2*time.Second, func() bool {
		_, err := coordinator.LookupOwner(ctx, ycluster.OwnerLookupRequest{DocumentKey: key})
		return errors.Is(err, ycluster.ErrOwnerNotFound)
	})
}

func TestHTTPServerOwnershipRuntimeReturnsUnavailableWhenLeaseHeld(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := memory.New()
	srv, _, resolver := newOwnershipHTTPTestServer(t, store, "node-a")
	key := testDocumentKey("doc-http-ownership-held")
	seedAuthoritativeHTTPDocument(t, ctx, store, resolver, key, "node-b", 1, "lease-node-b")

	dialCtx, cancel := context.WithTimeout(ctx, testIOTimeout)
	defer cancel()
	wsURL := "ws" + strings.TrimPrefix(srv.URL+"/ws?doc=doc-http-ownership-held&client=721&conn=blocked", "http")
	conn, response, err := websocket.Dial(dialCtx, wsURL, nil)
	if err == nil {
		_ = conn.CloseNow()
		t.Fatal("websocket.Dial() succeeded, want lease-held failure")
	}
	if response == nil {
		t.Fatalf("websocket.Dial() response = nil, error = %v", err)
	}
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("websocket.Dial() status = %d, want %d", response.StatusCode, http.StatusServiceUnavailable)
	}
	if response.Header.Get("Retry-After") != "1" {
		t.Fatalf("Retry-After = %q, want 1", response.Header.Get("Retry-After"))
	}
}

func newHTTPTestServer(t *testing.T, store storage.SnapshotStore) *httptest.Server {
	t.Helper()

	provider := yprotocol.NewProvider(yprotocol.ProviderConfig{Store: store})
	handler, err := NewServer(ServerConfig{
		Provider:       provider,
		ResolveRequest: resolveTestRequest,
	})
	if err != nil {
		t.Fatalf("NewServer() unexpected error: %v", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/ws", handler)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func newOwnershipHTTPTestServer(
	t *testing.T,
	store *memory.Store,
	localNode ycluster.NodeID,
) (*httptest.Server, *ycluster.StorageOwnershipCoordinator, ycluster.ShardResolver) {
	t.Helper()

	handler, coordinator, resolver := newOwnershipHTTPServer(t, store, localNode)
	mux := http.NewServeMux()
	mux.Handle("/ws", handler)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, coordinator, resolver
}

func newOwnershipHTTPServer(
	t *testing.T,
	store *memory.Store,
	localNode ycluster.NodeID,
) (*Server, *ycluster.StorageOwnershipCoordinator, ycluster.ShardResolver) {
	t.Helper()

	resolver, err := ycluster.NewDeterministicShardResolver(32)
	if err != nil {
		t.Fatalf("NewDeterministicShardResolver() unexpected error: %v", err)
	}
	coordinator, err := ycluster.NewStorageOwnershipCoordinator(ycluster.StorageOwnershipCoordinatorConfig{
		LocalNode:  localNode,
		Resolver:   resolver,
		Placements: store,
		Leases:     store,
		TTL:        time.Minute,
	})
	if err != nil {
		t.Fatalf("NewStorageOwnershipCoordinator() unexpected error: %v", err)
	}
	runtime, err := ycluster.NewDocumentOwnershipRuntime(ycluster.DocumentOwnershipRuntimeConfig{
		Coordinator: coordinator,
		Lease: ycluster.LeaseManagerRunConfig{
			RenewWithin: 30 * time.Second,
			Interval:    10 * time.Millisecond,
		},
		ReleaseTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("NewDocumentOwnershipRuntime() unexpected error: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = runtime.Close(ctx)
	})

	provider := yprotocol.NewProvider(yprotocol.ProviderConfig{
		Store: store,
		ResolveAuthorityFence: func(ctx context.Context, key storage.DocumentKey) (*storage.AuthorityFence, error) {
			return coordinator.ResolveAuthorityFence(ctx, key)
		},
	})
	handler, err := NewServer(ServerConfig{
		Provider:         provider,
		OwnershipRuntime: runtime,
		ResolveRequest:   resolveTestRequest,
	})
	if err != nil {
		t.Fatalf("NewServer() unexpected error: %v", err)
	}
	return handler, coordinator, resolver
}

func resolveTestRequest(r *http.Request) (Request, error) {
	query := r.URL.Query()
	documentID := strings.TrimSpace(query.Get("doc"))
	if documentID == "" {
		return Request{}, errors.New("doc obrigatorio")
	}

	clientRaw := strings.TrimSpace(query.Get("client"))
	if clientRaw == "" {
		return Request{}, errors.New("client obrigatorio")
	}

	clientValue, err := strconv.ParseUint(clientRaw, 10, 32)
	if err != nil {
		return Request{}, err
	}

	return Request{
		DocumentKey: storage.DocumentKey{
			Namespace:  "tests",
			DocumentID: documentID,
		},
		ConnectionID:     strings.TrimSpace(query.Get("conn")),
		ClientID:         uint32(clientValue),
		PersistOnClose:   query.Get("persist") == "1",
		SyncOutputFormat: mustResolveTestSyncOutputFormat(r),
	}, nil
}

func mustResolveTestSyncOutputFormat(r *http.Request) yjsbridge.UpdateFormat {
	format, err := SyncOutputFormatFromHTTPRequest(r)
	if err != nil {
		return yjsbridge.UpdateFormat(255)
	}
	return format
}

func dialWS(t *testing.T, rawURL string) *websocket.Conn {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), testIOTimeout)
	defer cancel()

	wsURL := "ws" + strings.TrimPrefix(rawURL, "http")
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("websocket.Dial(%q) unexpected error: %v", wsURL, err)
	}
	t.Cleanup(func() {
		_ = conn.CloseNow()
	})
	return conn
}

func writeBinary(t *testing.T, conn *websocket.Conn, payload []byte) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), testIOTimeout)
	defer cancel()
	if err := conn.Write(ctx, websocket.MessageBinary, payload); err != nil {
		t.Fatalf("conn.Write() unexpected error: %v", err)
	}
}

func readBinary(t *testing.T, conn *websocket.Conn) []byte {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), testIOTimeout)
	defer cancel()

	msgType, payload, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("conn.Read() unexpected error: %v", err)
	}
	if msgType != websocket.MessageBinary {
		t.Fatalf("conn.Read() type = %v, want %v", msgType, websocket.MessageBinary)
	}
	return payload
}

func assertYHTTPV2EquivalentToV1(t *testing.T, label string, gotV2, wantV1 []byte) {
	t.Helper()

	format, err := yjsbridge.FormatFromUpdate(gotV2)
	if err != nil {
		t.Fatalf("%s FormatFromUpdate() unexpected error: %v", label, err)
	}
	if format != yjsbridge.UpdateFormatV2 {
		t.Fatalf("%s format = %s, want %s", label, format, yjsbridge.UpdateFormatV2)
	}
	converted, err := yjsbridge.ConvertUpdateToV1(gotV2)
	if err != nil {
		t.Fatalf("%s ConvertUpdateToV1() unexpected error: %v", label, err)
	}
	if !bytes.Equal(converted, wantV1) {
		t.Fatalf("%s V2 converted to V1 = %x, want %x", label, converted, wantV1)
	}
	if bytes.Equal(gotV2, wantV1) {
		t.Fatalf("%s payload preserved V1 bytes: %x", label, gotV2)
	}
}

func waitForSnapshot(t *testing.T, store storage.SnapshotStore, key storage.DocumentKey) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		record, err := store.LoadSnapshot(context.Background(), key)
		if err == nil && record != nil && record.Snapshot != nil && !record.Snapshot.IsEmpty() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("snapshot for %v was not persisted before timeout", key)
}

func newAuthoritativeHTTPProvider(t *testing.T, localNode ycluster.NodeID) (*memory.Store, ycluster.ShardResolver, *yprotocol.Provider) {
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

	provider := yprotocol.NewProvider(yprotocol.ProviderConfig{
		Store: store,
		ResolveAuthorityFence: func(ctx context.Context, key storage.DocumentKey) (*storage.AuthorityFence, error) {
			return ycluster.ResolveStorageAuthorityFence(ctx, lookup, key)
		},
	})
	return store, resolver, provider
}

func newAuthoritativeLocalHTTPServer(t *testing.T, localNode ycluster.NodeID, store *memory.Store) (*Server, ycluster.ShardResolver) {
	return newAuthoritativeLocalHTTPServerWithMetrics(t, localNode, store, nil)
}

func newAuthoritativeLocalHTTPServerWithMetrics(t *testing.T, localNode ycluster.NodeID, store *memory.Store, metrics Metrics) (*Server, ycluster.ShardResolver) {
	t.Helper()

	resolver, err := ycluster.NewDeterministicShardResolver(32)
	if err != nil {
		t.Fatalf("NewDeterministicShardResolver() unexpected error: %v", err)
	}
	lookup, err := ycluster.NewStorageOwnerLookup(localNode, resolver, store, store)
	if err != nil {
		t.Fatalf("NewStorageOwnerLookup(%s) unexpected error: %v", localNode, err)
	}
	handler, err := NewServer(ServerConfig{
		Provider: yprotocol.NewProvider(yprotocol.ProviderConfig{
			Store: store,
			ResolveAuthorityFence: func(ctx context.Context, key storage.DocumentKey) (*storage.AuthorityFence, error) {
				return ycluster.ResolveStorageAuthorityFence(ctx, lookup, key)
			},
		}),
		ResolveRequest:                resolveTestRequest,
		AuthorityRevalidationInterval: 10 * time.Millisecond,
		Metrics:                       metrics,
	})
	if err != nil {
		t.Fatalf("NewServer() unexpected error: %v", err)
	}
	return handler, resolver
}

func seedAuthoritativeHTTPDocument(
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
		AcquiredAt: time.Now().UTC().Add(-time.Minute),
		ExpiresAt:  time.Now().UTC().Add(time.Hour),
	}); err != nil {
		t.Fatalf("store.SaveLease() unexpected error: %v", err)
	}
}

func handoffAuthoritativeHTTPDocument(
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
	current, err := store.LoadLease(ctx, ycluster.StorageShardID(shardID))
	if err != nil {
		t.Fatalf("store.LoadLease() unexpected error: %v", err)
	}
	if current.Token != oldToken {
		t.Fatalf("store.LoadLease().Token = %q, want %q", current.Token, oldToken)
	}
	now := time.Now().UTC()
	if _, err := store.HandoffLease(ctx, ycluster.StorageShardID(shardID), oldToken, storage.LeaseRecord{
		ShardID: ycluster.StorageShardID(shardID),
		Owner: storage.OwnerInfo{
			NodeID: ycluster.StorageNodeID(nextNode),
			Epoch:  nextEpoch,
		},
		Token:      nextToken,
		AcquiredAt: now,
		ExpiresAt:  now.Add(2 * time.Hour),
	}); err != nil {
		t.Fatalf("store.HandoffLease() unexpected error: %v", err)
	}
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
