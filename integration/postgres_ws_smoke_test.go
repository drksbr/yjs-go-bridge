package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5"

	"github.com/drksbr/yjs-crdt-golang-server/internal/varint"
	"github.com/drksbr/yjs-crdt-golang-server/internal/ytypes"
	"github.com/drksbr/yjs-crdt-golang-server/internal/yupdate"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/storage"
	pgstore "github.com/drksbr/yjs-crdt-golang-server/pkg/storage/postgres"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/yawareness"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/yhttp"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/yjsbridge"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/yprotocol"
)

const smokeIOTimeout = 15 * time.Second

func TestPostgresWebSocketSmoke_FunctionalityPersistence(t *testing.T) {
	pg := startDockerPostgres(t)
	schema := newSmokeSchema(t.Name())

	server1, closeServer1 := newPostgresSmokeServer(t, pg.dsn, schema)
	left := dialSmokeWS(t, server1.URL+"/ws?doc=notes&client=101&conn=left&persist=1")
	right := dialSmokeWS(t, server1.URL+"/ws?doc=notes&client=202&conn=right&persist=1")

	update := buildGCOnlyUpdate(19, 2)
	writeSmokeBinary(t, left, yprotocol.EncodeProtocolSyncUpdate(update))

	syncBroadcast := readSmokeBinary(t, right)
	syncMessages, err := yprotocol.DecodeProtocolMessages(syncBroadcast)
	if err != nil {
		t.Fatalf("DecodeProtocolMessages(sync) unexpected error: %v", err)
	}
	if len(syncMessages) != 1 || syncMessages[0].Sync == nil {
		t.Fatalf("syncMessages = %#v, want single sync update", syncMessages)
	}
	if syncMessages[0].Sync.Type != yprotocol.SyncMessageTypeUpdate {
		t.Fatalf("sync type = %v, want %v", syncMessages[0].Sync.Type, yprotocol.SyncMessageTypeUpdate)
	}
	if !bytes.Equal(syncMessages[0].Sync.Payload, update) {
		t.Fatalf("sync payload = %v, want %v", syncMessages[0].Sync.Payload, update)
	}

	awarenessPayload, err := yprotocol.EncodeProtocolAwarenessUpdate(&yawareness.Update{
		Clients: []yawareness.ClientState{{
			ClientID: 202,
			Clock:    1,
			State:    json.RawMessage(`{"name":"right","cursor":2}`),
		}},
	})
	if err != nil {
		t.Fatalf("EncodeProtocolAwarenessUpdate() unexpected error: %v", err)
	}
	writeSmokeBinary(t, right, awarenessPayload)

	awarenessBroadcast := readSmokeBinary(t, left)
	awarenessMessages, err := yprotocol.DecodeProtocolMessages(awarenessBroadcast)
	if err != nil {
		t.Fatalf("DecodeProtocolMessages(awareness) unexpected error: %v", err)
	}
	if len(awarenessMessages) != 1 || awarenessMessages[0].Awareness == nil {
		t.Fatalf("awarenessMessages = %#v, want single awareness update", awarenessMessages)
	}
	if len(awarenessMessages[0].Awareness.Clients) != 1 {
		t.Fatalf("len(awareness clients) = %d, want 1", len(awarenessMessages[0].Awareness.Clients))
	}
	if awarenessMessages[0].Awareness.Clients[0].ClientID != 202 {
		t.Fatalf("awareness clientID = %d, want 202", awarenessMessages[0].Awareness.Clients[0].ClientID)
	}

	closeSmokeWS(t, left)
	closeSmokeWS(t, right)
	waitPersistedSnapshot(t, pg.dsn, schema, storage.DocumentKey{Namespace: "integration", DocumentID: "notes"})
	closeServer1()

	server2, closeServer2 := newPostgresSmokeServer(t, pg.dsn, schema)
	defer closeServer2()

	probe := dialSmokeWS(t, server2.URL+"/ws?doc=notes&client=303&conn=probe")
	writeSmokeBinary(t, probe, yprotocol.EncodeProtocolSyncStep1([]byte{0x00}))
	reply := readSmokeBinary(t, probe)

	replyMessages, err := yprotocol.DecodeProtocolMessages(reply)
	if err != nil {
		t.Fatalf("DecodeProtocolMessages(step2) unexpected error: %v", err)
	}
	if len(replyMessages) != 1 || replyMessages[0].Sync == nil {
		t.Fatalf("replyMessages = %#v, want single sync step2", replyMessages)
	}
	if replyMessages[0].Sync.Type != yprotocol.SyncMessageTypeStep2 {
		t.Fatalf("reply sync type = %v, want %v", replyMessages[0].Sync.Type, yprotocol.SyncMessageTypeStep2)
	}

	expectedStep2, err := yjsbridge.DiffUpdate(update, []byte{0x00})
	if err != nil {
		t.Fatalf("DiffUpdate() unexpected error: %v", err)
	}
	if !bytes.Equal(replyMessages[0].Sync.Payload, expectedStep2) {
		t.Fatalf("reply sync payload = %v, want %v", replyMessages[0].Sync.Payload, expectedStep2)
	}
}

func TestPostgresWebSocketSmoke_RecoveryFromSnapshotAndUpdateLog(t *testing.T) {
	pg := startDockerPostgres(t)
	schema := newSmokeSchema(t.Name())
	key := storage.DocumentKey{
		Namespace:  "integration",
		DocumentID: "recovery-tail",
	}

	server1, closeServer1 := newPostgresSmokeServer(t, pg.dsn, schema)
	writer := dialSmokeWS(t, server1.URL+"/ws?doc=recovery-tail&client=601&conn=writer&persist=1")

	baseUpdate := buildGCOnlyUpdate(71, 2)
	writeSmokeBinary(t, writer, yprotocol.EncodeProtocolSyncUpdate(baseUpdate))
	closeSmokeWS(t, writer)
	waitPersistedSnapshot(t, pg.dsn, schema, key)
	closeServer1()

	store, err := pgstore.New(context.Background(), pgstore.Config{
		ConnectionString: pg.dsn,
		Schema:           schema,
	})
	if err != nil {
		t.Fatalf("pgstore.New(recovery) unexpected error: %v", err)
	}

	tail := [][]byte{
		buildGCOnlyUpdate(72, 1),
		buildGCOnlyUpdate(73, 3),
	}
	expected := mustMergeUpdates(t, append([][]byte{baseUpdate}, tail...)...)

	offsets := appendUpdates(t, context.Background(), store, key, tail...)
	recovered, err := storage.RecoverSnapshot(context.Background(), store, store, key, 0, 1)
	if err != nil {
		store.Close()
		t.Fatalf("RecoverSnapshot() unexpected error: %v", err)
	}
	if recovered.LastOffset != offsets[len(offsets)-1] {
		store.Close()
		t.Fatalf("recovered.LastOffset = %d, want %d", recovered.LastOffset, offsets[len(offsets)-1])
	}
	if !bytes.Equal(recovered.Snapshot.UpdateV1, expected) {
		store.Close()
		t.Fatalf("recovered.Snapshot.UpdateV1 = %x, want %x", recovered.Snapshot.UpdateV1, expected)
	}

	if _, err := store.SaveSnapshot(context.Background(), key, recovered.Snapshot); err != nil {
		store.Close()
		t.Fatalf("SaveSnapshot(recovered) unexpected error: %v", err)
	}
	if err := store.TrimUpdates(context.Background(), key, recovered.LastOffset); err != nil {
		store.Close()
		t.Fatalf("TrimUpdates(recovered.LastOffset) unexpected error: %v", err)
	}
	store.Close()

	server2, closeServer2 := newPostgresSmokeServer(t, pg.dsn, schema)
	defer closeServer2()

	probe := dialSmokeWS(t, server2.URL+"/ws?doc=recovery-tail&client=602&conn=probe")
	writeSmokeBinary(t, probe, yprotocol.EncodeProtocolSyncStep1([]byte{0x00}))
	reply := readSmokeBinary(t, probe)
	assertSyncStep2MatchesUpdate(t, reply, expected)
}

func TestPostgresWebSocketSmoke_LargeYTextPersistence(t *testing.T) {
	dsn := postgresSmokeDSN(t)
	schema := newSmokeSchema(t.Name())
	defer dropSmokeSchema(t, dsn, schema)
	key := storage.DocumentKey{Namespace: "integration", DocumentID: "large-ytext"}

	server1, closeServer1 := newPostgresSmokeServer(t, dsn, schema)
	writer := dialSmokeWS(t, server1.URL+"/ws?doc=large-ytext&client=701&conn=writer&persist=1")

	updates := buildChunkedYTextUpdates(1701, "content", 20000, 1000)
	expected := mustMergeUpdates(t, updates...)
	for idx, update := range updates {
		writeSmokeBinary(t, writer, yprotocol.EncodeProtocolSyncUpdate(update))
		if idx%5 == 4 {
			t.Logf("large ytext postgres smoke: enviados %d/%d chunks", idx+1, len(updates))
		}
	}

	closeSmokeWS(t, writer)
	waitPersistedSnapshot(t, dsn, schema, key)
	closeServer1()

	store, err := pgstore.New(context.Background(), pgstore.Config{
		ConnectionString: dsn,
		Schema:           schema,
	})
	if err != nil {
		t.Fatalf("pgstore.New(large-ytext) unexpected error: %v", err)
	}
	record, err := store.LoadSnapshot(context.Background(), key)
	if err != nil {
		store.Close()
		t.Fatalf("LoadSnapshot(large-ytext) unexpected error: %v", err)
	}
	gotState, err := yjsbridge.StateVectorFromUpdate(record.Snapshot.UpdateV1)
	if err != nil {
		store.Close()
		t.Fatalf("StateVectorFromUpdate(snapshot) unexpected error: %v", err)
	}
	wantState, err := yjsbridge.StateVectorFromUpdate(expected)
	if err != nil {
		store.Close()
		t.Fatalf("StateVectorFromUpdate(expected) unexpected error: %v", err)
	}
	if gotState[1701] != wantState[1701] {
		store.Close()
		t.Fatalf("persisted snapshot clock = %d, want %d", gotState[1701], wantState[1701])
	}
	store.Close()

	server2, closeServer2 := newPostgresSmokeServer(t, dsn, schema)
	defer closeServer2()

	probe := dialSmokeWS(t, server2.URL+"/ws?doc=large-ytext&client=702&conn=probe")
	writeSmokeBinary(t, probe, yprotocol.EncodeProtocolSyncStep1([]byte{0x00}))
	reply := readSmokeBinary(t, probe)
	assertSyncStep2MatchesUpdate(t, reply, expected)
}

func postgresSmokeDSN(t *testing.T) string {
	t.Helper()

	if dsn := strings.TrimSpace(os.Getenv("POSTGRES_TEST_DSN")); dsn != "" {
		return dsn
	}
	return startDockerPostgres(t).dsn
}

func dropSmokeSchema(t *testing.T, dsn, schema string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Logf("cleanup postgres schema skipped: connect failed: %v", err)
		return
	}
	defer func() {
		_ = conn.Close(context.Background())
	}()

	quoted := `"` + strings.ReplaceAll(schema, `"`, `""`) + `"`
	if _, err := conn.Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", quoted)); err != nil {
		t.Logf("cleanup postgres schema %s failed: %v", schema, err)
	}
}

func TestPostgresWebSocketSmoke_Performance(t *testing.T) {
	pg := startDockerPostgres(t)
	schema := newSmokeSchema(t.Name())

	server, closeServer := newPostgresSmokeServer(t, pg.dsn, schema)
	defer closeServer()

	left := dialSmokeWS(t, server.URL+"/ws?doc=bench&client=401&conn=left")
	right := dialSmokeWS(t, server.URL+"/ws?doc=bench&client=402&conn=right")

	const iterations = 200
	start := time.Now()
	for i := 0; i < iterations; i++ {
		update := buildGCOnlyUpdate(uint32(1000+i), 1)
		payload := yprotocol.EncodeProtocolSyncUpdate(update)

		sender := left
		receiver := right
		if i%2 == 1 {
			sender = right
			receiver = left
		}

		writeSmokeBinary(t, sender, payload)
		broadcast := readSmokeBinary(t, receiver)
		messages, err := yprotocol.DecodeProtocolMessages(broadcast)
		if err != nil {
			t.Fatalf("DecodeProtocolMessages(iteration=%d) unexpected error: %v", i, err)
		}
		if len(messages) != 1 || messages[0].Sync == nil {
			t.Fatalf("messages(iteration=%d) = %#v, want single sync update", i, messages)
		}
	}

	elapsed := time.Since(start)
	throughput := float64(iterations) / elapsed.Seconds()
	t.Logf("performance smoke: %d updates em %s (%.1f updates/s)", iterations, elapsed, throughput)
	if elapsed > 10*time.Second {
		t.Fatalf("performance smoke levou %s, want <= 10s", elapsed)
	}
}

func newPostgresSmokeServer(t *testing.T, dsn, schema string) (*httptest.Server, func()) {
	t.Helper()

	store, err := pgstore.New(context.Background(), pgstore.Config{
		ConnectionString: dsn,
		Schema:           schema,
	})
	if err != nil {
		t.Fatalf("pgstore.New() unexpected error: %v", err)
	}

	handler, err := yhttp.NewServer(yhttp.ServerConfig{
		Provider:       yprotocol.NewProvider(yprotocol.ProviderConfig{Store: store}),
		ResolveRequest: resolveSmokeRequest,
	})
	if err != nil {
		store.Close()
		t.Fatalf("yhttp.NewServer() unexpected error: %v", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/ws", handler)
	server := httptest.NewServer(mux)

	return server, func() {
		server.Close()
		store.Close()
	}
}

func resolveSmokeRequest(r *http.Request) (yhttp.Request, error) {
	query := r.URL.Query()
	documentID := strings.TrimSpace(query.Get("doc"))
	if documentID == "" {
		return yhttp.Request{}, errors.New("doc obrigatorio")
	}

	clientRaw := strings.TrimSpace(query.Get("client"))
	if clientRaw == "" {
		return yhttp.Request{}, errors.New("client obrigatorio")
	}
	clientValue, err := strconv.ParseUint(clientRaw, 10, 32)
	if err != nil {
		return yhttp.Request{}, err
	}

	return yhttp.Request{
		DocumentKey: storage.DocumentKey{
			Namespace:  "integration",
			DocumentID: documentID,
		},
		ConnectionID:   strings.TrimSpace(query.Get("conn")),
		ClientID:       uint32(clientValue),
		PersistOnClose: query.Get("persist") == "1",
	}, nil
}

func dialSmokeWS(t *testing.T, rawURL string) *websocket.Conn {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), smokeIOTimeout)
	defer cancel()
	wsURL := "ws" + strings.TrimPrefix(rawURL, "http")
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("websocket.Dial(%q) unexpected error: %v", wsURL, err)
	}
	conn.SetReadLimit(16 << 20)
	t.Cleanup(func() { _ = conn.CloseNow() })
	return conn
}

func writeSmokeBinary(t *testing.T, conn *websocket.Conn, payload []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), smokeIOTimeout)
	defer cancel()
	if err := conn.Write(ctx, websocket.MessageBinary, payload); err != nil {
		t.Fatalf("conn.Write() unexpected error: %v", err)
	}
}

func readSmokeBinary(t *testing.T, conn *websocket.Conn) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), smokeIOTimeout)
	defer cancel()
	msgType, payload, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("conn.Read() unexpected error: %v", err)
	}
	if msgType != websocket.MessageBinary {
		t.Fatalf("msgType = %v, want %v", msgType, websocket.MessageBinary)
	}
	return payload
}

func closeSmokeWS(t *testing.T, conn *websocket.Conn) {
	t.Helper()
	if err := conn.Close(websocket.StatusNormalClosure, "done"); err != nil {
		t.Fatalf("conn.Close() unexpected error: %v", err)
	}
}

func waitPersistedSnapshot(t *testing.T, dsn, schema string, key storage.DocumentKey) {
	t.Helper()

	store, err := pgstore.New(context.Background(), pgstore.Config{
		ConnectionString: dsn,
		Schema:           schema,
	})
	if err != nil {
		t.Fatalf("pgstore.New(wait) unexpected error: %v", err)
	}
	defer store.Close()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		record, err := store.LoadSnapshot(context.Background(), key)
		if err == nil && record != nil && record.Snapshot != nil && !record.Snapshot.IsEmpty() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf("snapshot persistido nao apareceu para %s/%s", key.Namespace, key.DocumentID)
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

func buildChunkedYTextUpdates(client uint32, parent string, words int, wordsPerUpdate int) [][]byte {
	updates := make([][]byte, 0, (words+wordsPerUpdate-1)/wordsPerUpdate)
	clock := uint32(0)
	for start := 0; start < words; start += wordsPerUpdate {
		end := start + wordsPerUpdate
		if end > words {
			end = words
		}
		text := buildLargeTextFragment(start, end, start > 0)
		updates = append(updates, buildYTextInsertUpdate(client, clock, parent, text))
		clock += uint32(len(text))
	}
	return updates
}

func buildLargeTextFragment(start, end int, leadingSpace bool) string {
	var b strings.Builder
	b.Grow((end - start) * 12)
	if leadingSpace {
		b.WriteByte(' ')
	}
	for i := start; i < end; i++ {
		if i > start {
			b.WriteByte(' ')
		}
		b.WriteString("palavra")
		b.WriteString(strconv.Itoa(i))
	}
	return b.String()
}

func buildYTextInsertUpdate(client, clock uint32, parent string, text string) []byte {
	update := varint.Append(nil, 1)
	update = varint.Append(update, 1)
	update = varint.Append(update, client)
	update = varint.Append(update, clock)
	if clock == 0 {
		update = append(update, 4)
		update = varint.Append(update, 1)
		update = appendVarString(update, parent)
	} else {
		update = append(update, 0x84)
		update = varint.Append(update, client)
		update = varint.Append(update, clock-1)
	}
	update = appendVarString(update, text)
	return varint.Append(update, 0)
}

func appendVarString(dst []byte, value string) []byte {
	bytes := []byte(value)
	dst = varint.Append(dst, uint32(len(bytes)))
	return append(dst, bytes...)
}
