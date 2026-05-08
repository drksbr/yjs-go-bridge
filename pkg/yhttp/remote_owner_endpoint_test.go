package yhttp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/drksbr/yjs-crdt-golang-server/pkg/storage/memory"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/yawareness"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/ycluster"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/yjsbridge"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/ynodeproto"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/yprotocol"
)

func TestRemoteOwnerEndpointSharesRoomWithLocalWebSocketPeers(t *testing.T) {
	t.Parallel()

	recorder := newRecordingMetrics()
	local := newLocalHTTPServerWithMetrics(t, nil, recorder)
	endpoint, err := NewRemoteOwnerEndpoint(RemoteOwnerEndpointConfig{
		Local:       local,
		LocalNodeID: "node-owner",
	})
	if err != nil {
		t.Fatalf("NewRemoteOwnerEndpoint() unexpected error: %v", err)
	}

	srv := newHTTPTestServerWithHandler(t, local)
	localPeer := dialWS(t, srv.URL+"/ws?doc=room-remote-owner&client=902&conn=local")
	writeBinary(t, localPeer, yprotocol.EncodeProtocolQueryAwareness())
	_ = readBinary(t, localPeer)

	stream := newFakeRemoteOwnerStream()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- endpoint.ServeStream(ctx, stream)
	}()

	key := testDocumentKey("room-remote-owner")
	stream.pushReceive(&ynodeproto.Handshake{
		NodeID:       "node-edge",
		DocumentKey:  key,
		ConnectionID: "remote-conn",
		ClientID:     901,
		Epoch:        41,
	})

	ack := readRemoteStreamMessage(t, stream)
	handshakeAck, ok := ack.(*ynodeproto.HandshakeAck)
	if !ok {
		t.Fatalf("first stream message = %T, want *ynodeproto.HandshakeAck", ack)
	}
	if handshakeAck.NodeID != "node-owner" {
		t.Fatalf("handshakeAck.NodeID = %q, want %q", handshakeAck.NodeID, "node-owner")
	}
	if handshakeAck.DocumentKey != key {
		t.Fatalf("handshakeAck.DocumentKey = %#v, want %#v", handshakeAck.DocumentKey, key)
	}
	if handshakeAck.ConnectionID != "remote-conn" {
		t.Fatalf("handshakeAck.ConnectionID = %q, want %q", handshakeAck.ConnectionID, "remote-conn")
	}
	if handshakeAck.ClientID != 901 {
		t.Fatalf("handshakeAck.ClientID = %d, want %d", handshakeAck.ClientID, 901)
	}
	if handshakeAck.Epoch != 41 {
		t.Fatalf("handshakeAck.Epoch = %d, want %d", handshakeAck.Epoch, 41)
	}

	remoteUpdate := buildGCOnlyUpdate(91, 2)
	stream.pushReceive(&ynodeproto.DocumentUpdate{
		DocumentKey:  key,
		ConnectionID: "remote-conn",
		Epoch:        41,
		UpdateV1:     remoteUpdate,
	})

	syncBroadcast := readBinary(t, localPeer)
	syncMessages, err := yprotocol.DecodeProtocolMessages(syncBroadcast)
	if err != nil {
		t.Fatalf("DecodeProtocolMessages(syncBroadcast) unexpected error: %v", err)
	}
	if len(syncMessages) != 1 || syncMessages[0].Sync == nil {
		t.Fatalf("syncMessages = %#v, want single sync message", syncMessages)
	}
	if syncMessages[0].Sync.Type != yprotocol.SyncMessageTypeUpdate {
		t.Fatalf("syncMessages[0].Sync.Type = %v, want %v", syncMessages[0].Sync.Type, yprotocol.SyncMessageTypeUpdate)
	}
	if !bytes.Equal(syncMessages[0].Sync.Payload, remoteUpdate) {
		t.Fatalf("syncMessages[0].Sync.Payload = %v, want %v", syncMessages[0].Sync.Payload, remoteUpdate)
	}

	remoteAwareness, err := yawareness.EncodeUpdate(&yawareness.Update{
		Clients: []yawareness.ClientState{{
			ClientID: 901,
			Clock:    1,
			State:    json.RawMessage(`{"name":"remote"}`),
		}},
	})
	if err != nil {
		t.Fatalf("yawareness.EncodeUpdate(remote) unexpected error: %v", err)
	}
	stream.pushReceive(&ynodeproto.AwarenessUpdate{
		DocumentKey:  key,
		ConnectionID: "remote-conn",
		Epoch:        41,
		Payload:      remoteAwareness,
	})

	remoteAwarenessBroadcast := readBinary(t, localPeer)
	assertProtocolAwarenessState(t, remoteAwarenessBroadcast, 901, 1, `{"name":"remote"}`, false)

	localAwareness, err := yprotocol.EncodeProtocolAwarenessUpdate(&yawareness.Update{
		Clients: []yawareness.ClientState{{
			ClientID: 902,
			Clock:    3,
			State:    json.RawMessage(`{"name":"local"}`),
		}},
	})
	if err != nil {
		t.Fatalf("EncodeProtocolAwarenessUpdate(local) unexpected error: %v", err)
	}
	writeBinary(t, localPeer, localAwareness)

	localAwarenessForwarded := readRemoteStreamMessage(t, stream)
	localAwarenessMessage, ok := localAwarenessForwarded.(*ynodeproto.AwarenessUpdate)
	if !ok {
		t.Fatalf("second stream message = %T, want *ynodeproto.AwarenessUpdate", localAwarenessForwarded)
	}
	assertTypedAwarenessState(t, localAwarenessMessage.Payload, 902, 3, `{"name":"local"}`, false)

	stream.pushReceive(&ynodeproto.QueryAwarenessRequest{
		DocumentKey:  key,
		ConnectionID: "remote-conn",
		Epoch:        41,
	})

	queryReply := readRemoteStreamMessage(t, stream)
	queryAwareness, ok := queryReply.(*ynodeproto.QueryAwarenessResponse)
	if !ok {
		t.Fatalf("query reply = %T, want *ynodeproto.QueryAwarenessResponse", queryReply)
	}
	assertTypedAwarenessContains(t, queryAwareness.Payload, map[uint32]string{
		901: `{"name":"remote"}`,
		902: `{"name":"local"}`,
	})

	stream.pushReceive(&ynodeproto.Disconnect{
		DocumentKey:  key,
		ConnectionID: "remote-conn",
		Epoch:        41,
	})

	tombstone := readBinary(t, localPeer)
	assertProtocolAwarenessState(t, tombstone, 901, 2, "", true)

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("ServeStream() unexpected error: %v", err)
		}
	case <-time.After(testIOTimeout):
		t.Fatal("ServeStream() did not return after disconnect")
	}

	select {
	case <-stream.closeCh:
	case <-time.After(testIOTimeout):
		t.Fatal("remote stream was not closed after disconnect")
	}

	waitForCondition(t, 2*time.Second, func() bool {
		snapshot := recorder.snapshot()
		return snapshot.remoteOwnerConnectionsOpen[remoteOwnerMetricsRoleOwner] == 1 &&
			snapshot.remoteOwnerConnectionsClose[remoteOwnerMetricsRoleOwner] == 1 &&
			snapshot.remoteOwnerCloses[recordingRemoteOwnerCloseKey{
				role:   remoteOwnerMetricsRoleOwner,
				reason: "disconnect",
			}] == 1
	})

	snapshot := recorder.snapshot()
	if snapshot.remoteOwnerHandshakes[recordingRemoteOwnerHandshakeKey{
		role:   remoteOwnerMetricsRoleOwner,
		result: "ok",
	}] != 1 {
		t.Fatalf("remoteOwnerHandshakes[owner ok] = %d, want 1", snapshot.remoteOwnerHandshakes[recordingRemoteOwnerHandshakeKey{
			role:   remoteOwnerMetricsRoleOwner,
			result: "ok",
		}])
	}
	if snapshot.remoteOwnerMessages[recordingRemoteOwnerMessageKey{
		role:      remoteOwnerMetricsRoleOwner,
		direction: remoteOwnerMetricsDirectionIn,
		kind:      "handshake",
	}] != 1 {
		t.Fatal("missing owner inbound handshake metric")
	}
	if snapshot.remoteOwnerMessages[recordingRemoteOwnerMessageKey{
		role:      remoteOwnerMetricsRoleOwner,
		direction: remoteOwnerMetricsDirectionOut,
		kind:      "handshake_ack",
	}] != 1 {
		t.Fatal("missing owner outbound handshake_ack metric")
	}
	if snapshot.remoteOwnerMessages[recordingRemoteOwnerMessageKey{
		role:      remoteOwnerMetricsRoleOwner,
		direction: remoteOwnerMetricsDirectionIn,
		kind:      "document_update",
	}] != 1 {
		t.Fatal("missing owner inbound document_update metric")
	}
	if snapshot.remoteOwnerMessages[recordingRemoteOwnerMessageKey{
		role:      remoteOwnerMetricsRoleOwner,
		direction: remoteOwnerMetricsDirectionIn,
		kind:      "awareness_update",
	}] != 1 {
		t.Fatal("missing owner inbound awareness_update metric")
	}
	if snapshot.remoteOwnerMessages[recordingRemoteOwnerMessageKey{
		role:      remoteOwnerMetricsRoleOwner,
		direction: remoteOwnerMetricsDirectionOut,
		kind:      "awareness_update",
	}] != 1 {
		t.Fatal("missing owner outbound awareness_update metric")
	}
	if snapshot.remoteOwnerMessages[recordingRemoteOwnerMessageKey{
		role:      remoteOwnerMetricsRoleOwner,
		direction: remoteOwnerMetricsDirectionIn,
		kind:      "query_awareness_request",
	}] != 1 {
		t.Fatal("missing owner inbound query_awareness_request metric")
	}
	if snapshot.remoteOwnerMessages[recordingRemoteOwnerMessageKey{
		role:      remoteOwnerMetricsRoleOwner,
		direction: remoteOwnerMetricsDirectionOut,
		kind:      "query_awareness_response",
	}] != 1 {
		t.Fatal("missing owner outbound query_awareness_response metric")
	}
	if snapshot.remoteOwnerMessages[recordingRemoteOwnerMessageKey{
		role:      remoteOwnerMetricsRoleOwner,
		direction: remoteOwnerMetricsDirectionIn,
		kind:      "disconnect",
	}] != 1 {
		t.Fatal("missing owner inbound disconnect metric")
	}
}

func TestRemoteOwnerEndpointAuthenticatesHandshake(t *testing.T) {
	t.Parallel()

	local := newLocalHTTPServer(t, nil)
	authCalls := make(chan RemoteOwnerAuthRequest, 1)
	endpoint, err := NewRemoteOwnerEndpoint(RemoteOwnerEndpointConfig{
		Local:       local,
		LocalNodeID: "node-owner",
		Authenticate: func(_ context.Context, req RemoteOwnerAuthRequest) error {
			authCalls <- req
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewRemoteOwnerEndpoint() unexpected error: %v", err)
	}

	stream := newFakeRemoteOwnerStream()
	errCh := make(chan error, 1)
	go func() {
		errCh <- endpoint.ServeStream(context.Background(), stream)
	}()

	key := testDocumentKey("room-remote-owner-auth")
	stream.pushReceive(&ynodeproto.Handshake{
		NodeID:       "node-edge",
		DocumentKey:  key,
		ConnectionID: "remote-auth",
		ClientID:     910,
		Epoch:        61,
		Flags:        ynodeproto.FlagPersistOnClose,
	})

	if _, ok := readRemoteStreamMessage(t, stream).(*ynodeproto.HandshakeAck); !ok {
		t.Fatal("first stream message is not HandshakeAck")
	}

	select {
	case call := <-authCalls:
		if call.NodeID != "node-edge" || call.DocumentKey != key || call.ConnectionID != "remote-auth" || call.ClientID != 910 || call.Epoch != 61 {
			t.Fatalf("auth request = %#v, want node-edge/%#v/remote-auth/910/61", call, key)
		}
		if call.Flags&ynodeproto.FlagPersistOnClose == 0 {
			t.Fatalf("auth request Flags = %v, want FlagPersistOnClose", call.Flags)
		}
	case <-time.After(testIOTimeout):
		t.Fatal("authenticator was not called")
	}

	stream.pushReceive(&ynodeproto.Disconnect{
		DocumentKey:  key,
		ConnectionID: "remote-auth",
		Epoch:        61,
	})

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("ServeStream() unexpected error: %v", err)
		}
	case <-time.After(testIOTimeout):
		t.Fatal("ServeStream() did not return after disconnect")
	}
}

func TestRemoteOwnerEndpointOwnershipRuntimeClaimsAndReleasesRemoteSession(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := memory.New()
	local, coordinator, _ := newOwnershipHTTPServer(t, store, "node-owner")
	endpoint, err := NewRemoteOwnerEndpoint(RemoteOwnerEndpointConfig{
		Local:       local,
		LocalNodeID: "node-owner",
	})
	if err != nil {
		t.Fatalf("NewRemoteOwnerEndpoint() unexpected error: %v", err)
	}

	stream := newFakeRemoteOwnerStream()
	errCh := make(chan error, 1)
	go func() {
		errCh <- endpoint.ServeStream(ctx, stream)
	}()

	key := testDocumentKey("room-remote-owner-runtime")
	stream.pushReceive(&ynodeproto.Handshake{
		NodeID:       "node-edge",
		DocumentKey:  key,
		ConnectionID: "remote-runtime",
		ClientID:     912,
		Epoch:        1,
	})
	if _, ok := readRemoteStreamMessage(t, stream).(*ynodeproto.HandshakeAck); !ok {
		t.Fatal("expected handshake ack after ownership claim")
	}

	resolution, err := coordinator.LookupOwner(ctx, ycluster.OwnerLookupRequest{DocumentKey: key})
	if err != nil {
		t.Fatalf("LookupOwner(remote session open) unexpected error: %v", err)
	}
	if !resolution.Local || resolution.Placement.Lease == nil {
		t.Fatalf("LookupOwner(remote session open) = %#v, want local owner with lease", resolution)
	}

	stream.pushReceive(&ynodeproto.Disconnect{
		DocumentKey:  key,
		ConnectionID: "remote-runtime",
		Epoch:        1,
	})

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("ServeStream() unexpected error: %v", err)
		}
	case <-time.After(testIOTimeout):
		t.Fatal("ServeStream() did not return after disconnect")
	}
	waitForCondition(t, 2*time.Second, func() bool {
		_, err := coordinator.LookupOwner(ctx, ycluster.OwnerLookupRequest{DocumentKey: key})
		return errors.Is(err, ycluster.ErrOwnerNotFound)
	})
}

func TestRemoteOwnerEndpointRejectsUnauthenticatedHandshake(t *testing.T) {
	t.Parallel()

	authErr := errors.New("node not allowed")
	local := newLocalHTTPServer(t, nil)
	endpoint, err := NewRemoteOwnerEndpoint(RemoteOwnerEndpointConfig{
		Local:       local,
		LocalNodeID: "node-owner",
		Authenticate: func(context.Context, RemoteOwnerAuthRequest) error {
			return authErr
		},
	})
	if err != nil {
		t.Fatalf("NewRemoteOwnerEndpoint() unexpected error: %v", err)
	}

	stream := newFakeRemoteOwnerStream()
	errCh := make(chan error, 1)
	go func() {
		errCh <- endpoint.ServeStream(context.Background(), stream)
	}()

	stream.pushReceive(&ynodeproto.Handshake{
		NodeID:       "node-edge",
		DocumentKey:  testDocumentKey("room-remote-owner-auth-reject"),
		ConnectionID: "remote-auth-reject",
		ClientID:     911,
		Epoch:        62,
	})

	select {
	case err := <-errCh:
		if !errors.Is(err, authErr) {
			t.Fatalf("ServeStream() error = %v, want %v", err, authErr)
		}
	case <-time.After(testIOTimeout):
		t.Fatal("ServeStream() did not return after auth rejection")
	}

	select {
	case message := <-stream.sends:
		t.Fatalf("unexpected message sent after rejected auth: %T", message)
	default:
	}
}

func TestRemoteOwnerEndpointAuthenticatesHTTPHeaderBearer(t *testing.T) {
	t.Parallel()

	local := newLocalHTTPServer(t, nil)
	endpoint, err := NewRemoteOwnerEndpoint(RemoteOwnerEndpointConfig{
		Local:        local,
		LocalNodeID:  "node-owner",
		Authenticate: RemoteOwnerBearerAuthenticator("node-token"),
	})
	if err != nil {
		t.Fatalf("NewRemoteOwnerEndpoint() unexpected error: %v", err)
	}
	srv := newHTTPTestServerWithHandler(t, endpoint)

	dialer, err := NewWebSocketRemoteOwnerDialer(WebSocketRemoteOwnerDialerConfig{
		ResolveURL: func(context.Context, RemoteOwnerDialRequest) (string, error) {
			return "ws" + strings.TrimPrefix(srv.URL+"/ws", "http"), nil
		},
		AuthHeaders: RemoteOwnerBearerAuthHeaders("node-token"),
	})
	if err != nil {
		t.Fatalf("NewWebSocketRemoteOwnerDialer() unexpected error: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), testIOTimeout)
	defer cancel()
	key := testDocumentKey("room-remote-owner-http-bearer")
	stream, err := dialer.DialRemoteOwner(ctx, RemoteOwnerDialRequest{
		Request: Request{
			DocumentKey:  key,
			ConnectionID: "remote-http-bearer",
			ClientID:     913,
		},
		Resolution: ycluster.OwnerResolution{
			Placement: ycluster.Placement{NodeID: "node-owner"},
		},
	})
	if err != nil {
		t.Fatalf("DialRemoteOwner() unexpected error: %v", err)
	}
	defer func() {
		_ = stream.Close()
	}()

	if err := stream.Send(ctx, &ynodeproto.Handshake{
		NodeID:       "node-edge",
		DocumentKey:  key,
		ConnectionID: "remote-http-bearer",
		ClientID:     913,
		Epoch:        63,
	}); err != nil {
		t.Fatalf("stream.Send(handshake) unexpected error: %v", err)
	}
	message, err := stream.Receive(ctx)
	if err != nil {
		t.Fatalf("stream.Receive() unexpected error: %v", err)
	}
	if _, ok := message.(*ynodeproto.HandshakeAck); !ok {
		t.Fatal("first owner response is not HandshakeAck")
	}
}

func TestRemoteOwnerEndpointRejectsMismatchedRoute(t *testing.T) {
	t.Parallel()

	local := newLocalHTTPServer(t, nil)
	endpoint, err := NewRemoteOwnerEndpoint(RemoteOwnerEndpointConfig{
		Local:       local,
		LocalNodeID: "node-owner",
	})
	if err != nil {
		t.Fatalf("NewRemoteOwnerEndpoint() unexpected error: %v", err)
	}

	stream := newFakeRemoteOwnerStream()
	errCh := make(chan error, 1)
	go func() {
		errCh <- endpoint.ServeStream(context.Background(), stream)
	}()

	key := testDocumentKey("room-route-mismatch")
	stream.pushReceive(&ynodeproto.Handshake{
		NodeID:       "node-edge",
		DocumentKey:  key,
		ConnectionID: "remote-conn",
		ClientID:     903,
		Epoch:        51,
	})
	if _, ok := readRemoteStreamMessage(t, stream).(*ynodeproto.HandshakeAck); !ok {
		t.Fatal("expected handshake ack before route mismatch")
	}

	stream.pushReceive(&ynodeproto.DocumentUpdate{
		DocumentKey:  key,
		ConnectionID: "wrong-conn",
		Epoch:        51,
		UpdateV1:     buildGCOnlyUpdate(77, 1),
	})

	closeMessage := readRemoteStreamMessage(t, stream)
	closeFrame, ok := closeMessage.(*ynodeproto.Close)
	if !ok {
		t.Fatalf("close message = %T, want *ynodeproto.Close", closeMessage)
	}
	if closeFrame.ConnectionID != "remote-conn" {
		t.Fatalf("closeFrame.ConnectionID = %q, want %q", closeFrame.ConnectionID, "remote-conn")
	}
	if closeFrame.DocumentKey != key {
		t.Fatalf("closeFrame.DocumentKey = %#v, want %#v", closeFrame.DocumentKey, key)
	}
	if closeFrame.Epoch != 51 {
		t.Fatalf("closeFrame.Epoch = %d, want %d", closeFrame.Epoch, 51)
	}
	if closeFrame.Retryable {
		t.Fatal("closeFrame.Retryable = true, want false")
	}
	if closeFrame.Reason != "route_mismatch" {
		t.Fatalf("closeFrame.Reason = %q, want %q", closeFrame.Reason, "route_mismatch")
	}

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("ServeStream() error = nil, want mismatch error")
		}
		if !strings.Contains(err.Error(), "mismatch") {
			t.Fatalf("ServeStream() error = %v, want route mismatch context", err)
		}
	case <-time.After(testIOTimeout):
		t.Fatal("ServeStream() did not fail after route mismatch")
	}
}

func TestRemoteOwnerEndpointRevalidatesAuthorityAndClosesIdleRemoteSession(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := memory.New()
	recorder := newRecordingMetrics()
	local, resolver := newAuthoritativeLocalHTTPServerWithMetrics(t, "node-owner", store, recorder)
	key := testDocumentKey("room-owner-revalidate")
	seedAuthoritativeHTTPDocument(t, ctx, store, resolver, key, "node-owner", 1, "lease-owner-a")

	endpoint, err := NewRemoteOwnerEndpoint(RemoteOwnerEndpointConfig{
		Local:       local,
		LocalNodeID: "node-owner",
	})
	if err != nil {
		t.Fatalf("NewRemoteOwnerEndpoint() unexpected error: %v", err)
	}

	stream := newFakeRemoteOwnerStream()
	errCh := make(chan error, 1)
	go func() {
		errCh <- endpoint.ServeStream(ctx, stream)
	}()

	stream.pushReceive(&ynodeproto.Handshake{
		NodeID:       "node-edge",
		DocumentKey:  key,
		ConnectionID: "remote-conn",
		ClientID:     904,
		Epoch:        1,
	})
	if _, ok := readRemoteStreamMessage(t, stream).(*ynodeproto.HandshakeAck); !ok {
		t.Fatal("expected handshake ack before authority revalidation")
	}

	handoffAuthoritativeHTTPDocument(t, ctx, store, resolver, key, "lease-owner-a", "node-b", 2, "lease-owner-b")

	closeMessage := readRemoteStreamMessage(t, stream)
	closeFrame, ok := closeMessage.(*ynodeproto.Close)
	if !ok {
		t.Fatalf("close message = %T, want *ynodeproto.Close", closeMessage)
	}
	if !closeFrame.Retryable {
		t.Fatal("closeFrame.Retryable = false, want true")
	}
	if closeFrame.Reason != authorityLostCloseReason {
		t.Fatalf("closeFrame.Reason = %q, want %q", closeFrame.Reason, authorityLostCloseReason)
	}
	if closeFrame.DocumentKey != key {
		t.Fatalf("closeFrame.DocumentKey = %#v, want %#v", closeFrame.DocumentKey, key)
	}
	if closeFrame.ConnectionID != "remote-conn" {
		t.Fatalf("closeFrame.ConnectionID = %q, want %q", closeFrame.ConnectionID, "remote-conn")
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("ServeStream() unexpected error: %v", err)
		}
	case <-time.After(testIOTimeout):
		t.Fatal("ServeStream() did not return after authority loss")
	}

	waitForCondition(t, 2*time.Second, func() bool {
		snapshot := recorder.snapshot()
		return snapshot.authorityRevalidations[recordingAuthorityRevalidationKey{
			role:   authorityRevalidationRoleOwner,
			result: "error",
		}] == 1
	})
}

func TestRemoteOwnerEndpointNegotiatedV2SendsRemoteUpdatesV2(t *testing.T) {
	t.Parallel()

	local := newLocalHTTPServer(t, nil)
	endpoint, err := NewRemoteOwnerEndpoint(RemoteOwnerEndpointConfig{
		Local:       local,
		LocalNodeID: "node-owner-v2",
	})
	if err != nil {
		t.Fatalf("NewRemoteOwnerEndpoint() unexpected error: %v", err)
	}

	srv := newHTTPTestServerWithHandler(t, local)
	localPeer := dialWS(t, srv.URL+"/ws?doc=room-remote-owner-v2&client=912&conn=local")

	stream := newFakeRemoteOwnerStream()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- endpoint.ServeStream(ctx, stream)
	}()

	key := testDocumentKey("room-remote-owner-v2")
	stream.pushReceive(&ynodeproto.Handshake{
		Flags:        ynodeproto.FlagSupportsUpdateV2,
		NodeID:       "node-edge-v2",
		DocumentKey:  key,
		ConnectionID: "remote-v2",
		ClientID:     911,
		Epoch:        55,
	})

	ack := readRemoteStreamMessage(t, stream)
	handshakeAck, ok := ack.(*ynodeproto.HandshakeAck)
	if !ok {
		t.Fatalf("first stream message = %T, want *ynodeproto.HandshakeAck", ack)
	}
	if handshakeAck.Flags&ynodeproto.FlagSupportsUpdateV2 == 0 {
		t.Fatalf("handshakeAck.Flags = %#x, want V2 support", handshakeAck.Flags)
	}
	waitForCondition(t, 2*time.Second, func() bool {
		return len(local.registry.peersExcept(key, "local")) > 0
	})

	localUpdate := buildGCOnlyUpdate(912, 2)
	writeBinary(t, localPeer, yprotocol.EncodeProtocolSyncUpdate(localUpdate))

	forwarded := readRemoteStreamMessage(t, stream)
	updateV2, ok := forwarded.(*ynodeproto.DocumentUpdateV2)
	if !ok {
		t.Fatalf("forwarded = %T, want *ynodeproto.DocumentUpdateV2", forwarded)
	}
	if updateV2.DocumentKey != key || updateV2.ConnectionID != "remote-v2" || updateV2.Epoch != 55 {
		t.Fatalf("forwarded route = %#v, want key/remote-v2/55", updateV2)
	}
	assertYHTTPV2EquivalentToV1(t, "owner endpoint remote peer", updateV2.UpdateV2, localUpdate)

	remoteUpdateV1 := buildGCOnlyUpdate(913, 2)
	remoteUpdateV2, err := yjsbridge.ConvertUpdateToV2(remoteUpdateV1)
	if err != nil {
		t.Fatalf("ConvertUpdateToV2(remote update) unexpected error: %v", err)
	}
	stream.pushReceive(&ynodeproto.DocumentUpdateV2FromEdge{
		DocumentKey:  key,
		ConnectionID: "remote-v2",
		Epoch:        55,
		UpdateV2:     remoteUpdateV2,
	})

	broadcast := readBinary(t, localPeer)
	messages, err := yprotocol.DecodeProtocolMessages(broadcast)
	if err != nil {
		t.Fatalf("DecodeProtocolMessages(remote V2 broadcast) unexpected error: %v", err)
	}
	if len(messages) != 1 || messages[0].Sync == nil {
		t.Fatalf("remote V2 broadcast messages = %#v, want single sync message", messages)
	}
	if messages[0].Sync.Type != yprotocol.SyncMessageTypeUpdate {
		t.Fatalf("remote V2 broadcast sync type = %v, want %v", messages[0].Sync.Type, yprotocol.SyncMessageTypeUpdate)
	}
	convertedBroadcast, err := yjsbridge.ConvertUpdateToV1(messages[0].Sync.Payload)
	if err != nil {
		t.Fatalf("ConvertUpdateToV1(remote V2 broadcast) unexpected error: %v", err)
	}
	if !bytes.Equal(convertedBroadcast, remoteUpdateV1) {
		t.Fatalf("remote V2 broadcast payload = %x, want compatibility V1 %x", convertedBroadcast, remoteUpdateV1)
	}

	stream.pushReceive(&ynodeproto.Disconnect{
		DocumentKey:  key,
		ConnectionID: "remote-v2",
		Epoch:        55,
	})
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("ServeStream() unexpected error: %v", err)
		}
	case <-time.After(testIOTimeout):
		t.Fatal("ServeStream() did not return after disconnect")
	}
}

func TestRemoteOwnerEndpointRejectsV2DocumentUpdateWithoutNegotiation(t *testing.T) {
	t.Parallel()

	local := newLocalHTTPServer(t, nil)
	endpoint, err := NewRemoteOwnerEndpoint(RemoteOwnerEndpointConfig{
		Local:       local,
		LocalNodeID: "node-owner-v2-reject",
	})
	if err != nil {
		t.Fatalf("NewRemoteOwnerEndpoint() unexpected error: %v", err)
	}

	stream := newFakeRemoteOwnerStream()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- endpoint.ServeStream(ctx, stream)
	}()

	key := testDocumentKey("room-remote-owner-v2-reject")
	stream.pushReceive(&ynodeproto.Handshake{
		NodeID:       "node-edge-v1",
		DocumentKey:  key,
		ConnectionID: "remote-v1",
		ClientID:     914,
		Epoch:        56,
	})

	ack := readRemoteStreamMessage(t, stream)
	handshakeAck, ok := ack.(*ynodeproto.HandshakeAck)
	if !ok {
		t.Fatalf("first stream message = %T, want *ynodeproto.HandshakeAck", ack)
	}
	if handshakeAck.Flags&ynodeproto.FlagSupportsUpdateV2 != 0 {
		t.Fatalf("handshakeAck.Flags = %#x, want no V2 support", handshakeAck.Flags)
	}

	updateV2, err := yjsbridge.ConvertUpdateToV2(buildGCOnlyUpdate(914, 2))
	if err != nil {
		t.Fatalf("ConvertUpdateToV2() unexpected error: %v", err)
	}
	stream.pushReceive(&ynodeproto.DocumentUpdateV2FromEdge{
		DocumentKey:  key,
		ConnectionID: "remote-v1",
		Epoch:        56,
		UpdateV2:     updateV2,
	})

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("ServeStream() error = nil, want V2 negotiation error")
		}
	case <-time.After(testIOTimeout):
		t.Fatal("ServeStream() did not reject unnegotiated V2 update")
	}
}

func TestRemoteOwnerEndpointRejectsStaleHandshakeEpoch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := memory.New()
	local, resolver := newAuthoritativeLocalHTTPServer(t, "node-owner", store)
	key := testDocumentKey("room-owner-stale-handshake")
	seedAuthoritativeHTTPDocument(t, ctx, store, resolver, key, "node-owner", 2, "lease-owner-current")

	endpoint, err := NewRemoteOwnerEndpoint(RemoteOwnerEndpointConfig{
		Local:       local,
		LocalNodeID: "node-owner",
	})
	if err != nil {
		t.Fatalf("NewRemoteOwnerEndpoint() unexpected error: %v", err)
	}

	stream := newFakeRemoteOwnerStream()
	errCh := make(chan error, 1)
	go func() {
		errCh <- endpoint.ServeStream(ctx, stream)
	}()

	stream.pushReceive(&ynodeproto.Handshake{
		NodeID:       "node-edge",
		DocumentKey:  key,
		ConnectionID: "remote-stale",
		ClientID:     913,
		Epoch:        1,
	})

	closeMessage := readRemoteStreamMessage(t, stream)
	closeFrame, ok := closeMessage.(*ynodeproto.Close)
	if !ok {
		t.Fatalf("first stream message = %T, want *ynodeproto.Close", closeMessage)
	}
	if !closeFrame.Retryable {
		t.Fatal("closeFrame.Retryable = false, want true")
	}
	if closeFrame.Reason != authorityLostCloseReason {
		t.Fatalf("closeFrame.Reason = %q, want %q", closeFrame.Reason, authorityLostCloseReason)
	}
	if closeFrame.Epoch != 1 {
		t.Fatalf("closeFrame.Epoch = %d, want stale handshake epoch 1", closeFrame.Epoch)
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("ServeStream() unexpected error: %v", err)
		}
	case <-time.After(testIOTimeout):
		t.Fatal("ServeStream() did not return after stale epoch close")
	}
	select {
	case <-stream.closeCh:
	case <-time.After(testIOTimeout):
		t.Fatal("remote stream was not closed after stale epoch rejection")
	}
}

func readRemoteStreamMessage(t *testing.T, stream *fakeRemoteOwnerStream) ynodeproto.Message {
	t.Helper()

	select {
	case message := <-stream.sends:
		return message
	case <-time.After(testIOTimeout):
		t.Fatal("remote stream did not receive message before timeout")
		return nil
	}
}

func assertProtocolAwarenessState(t *testing.T, payload []byte, wantClientID uint32, wantClock uint32, wantState string, wantNull bool) {
	t.Helper()

	messages, err := yprotocol.DecodeProtocolMessages(payload)
	if err != nil {
		t.Fatalf("DecodeProtocolMessages() unexpected error: %v", err)
	}
	if len(messages) != 1 || messages[0].Awareness == nil {
		t.Fatalf("messages = %#v, want single awareness message", messages)
	}
	assertAwarenessClientStateDecoded(t, messages[0].Awareness, wantClientID, wantClock, wantState, wantNull)
}

func assertTypedAwarenessState(t *testing.T, payload []byte, wantClientID uint32, wantClock uint32, wantState string, wantNull bool) {
	t.Helper()

	update, err := yawareness.DecodeUpdate(payload)
	if err != nil {
		t.Fatalf("yawareness.DecodeUpdate() unexpected error: %v", err)
	}
	assertAwarenessClientStateDecoded(t, update, wantClientID, wantClock, wantState, wantNull)
}

func assertTypedAwarenessContains(t *testing.T, payload []byte, want map[uint32]string) {
	t.Helper()

	update, err := yawareness.DecodeUpdate(payload)
	if err != nil {
		t.Fatalf("yawareness.DecodeUpdate() unexpected error: %v", err)
	}
	if len(update.Clients) != len(want) {
		t.Fatalf("len(update.Clients) = %d, want %d", len(update.Clients), len(want))
	}
	for _, client := range update.Clients {
		wantState, ok := want[client.ClientID]
		if !ok {
			t.Fatalf("unexpected client in awareness snapshot: %d", client.ClientID)
		}
		if client.IsNull() {
			t.Fatalf("client %d unexpectedly tombstoned in awareness snapshot", client.ClientID)
		}
		if string(client.State) != wantState {
			t.Fatalf("client %d state = %s, want %s", client.ClientID, client.State, wantState)
		}
	}
}

func assertAwarenessClientStateDecoded(t *testing.T, update *yawareness.Update, wantClientID uint32, wantClock uint32, wantState string, wantNull bool) {
	t.Helper()

	if update == nil {
		t.Fatal("awareness update = nil")
	}
	if len(update.Clients) != 1 {
		t.Fatalf("len(update.Clients) = %d, want 1", len(update.Clients))
	}

	client := update.Clients[0]
	if client.ClientID != wantClientID {
		t.Fatalf("client.ClientID = %d, want %d", client.ClientID, wantClientID)
	}
	if client.Clock != wantClock {
		t.Fatalf("client.Clock = %d, want %d", client.Clock, wantClock)
	}
	if client.IsNull() != wantNull {
		t.Fatalf("client.IsNull() = %v, want %v", client.IsNull(), wantNull)
	}
	if wantNull {
		return
	}
	if string(client.State) != wantState {
		t.Fatalf("client.State = %s, want %s", client.State, wantState)
	}
}
