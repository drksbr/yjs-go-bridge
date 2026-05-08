package yhttp

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/coder/websocket"

	"github.com/drksbr/yjs-crdt-golang-server/pkg/ynodeproto"
)

// RemoteOwnerURLResolver resolve a URL do endpoint owner-side para um owner
// remoto já resolvido.
type RemoteOwnerURLResolver func(ctx context.Context, req RemoteOwnerDialRequest) (string, error)

// WebSocketRemoteOwnerDialerConfig define o dialer websocket tipado entre edge
// e owner remoto.
type WebSocketRemoteOwnerDialerConfig struct {
	ResolveURL     RemoteOwnerURLResolver
	DialOptions    *websocket.DialOptions
	AuthHeaders    RemoteOwnerAuthHeadersFunc
	ReadLimitBytes int64
}

type webSocketRemoteOwnerDialer struct {
	resolveURL     RemoteOwnerURLResolver
	dialOptions    *websocket.DialOptions
	authHeaders    RemoteOwnerAuthHeadersFunc
	readLimitBytes int64
}

// NewWebSocketRemoteOwnerDialer constrói um `RemoteOwnerDialer` sobre
// WebSocket binário, reaproveitando `NodeMessageStream` como wire tipado.
func NewWebSocketRemoteOwnerDialer(cfg WebSocketRemoteOwnerDialerConfig) (RemoteOwnerDialer, error) {
	if cfg.ResolveURL == nil {
		return nil, ErrNilRemoteOwnerURLResolver
	}

	readLimit := cfg.ReadLimitBytes
	if readLimit <= 0 {
		readLimit = defaultReadLimitBytes
	}

	return &webSocketRemoteOwnerDialer{
		resolveURL:     cfg.ResolveURL,
		dialOptions:    cloneDialOptions(cfg.DialOptions),
		authHeaders:    cfg.AuthHeaders,
		readLimitBytes: readLimit,
	}, nil
}

func (d *webSocketRemoteOwnerDialer) DialRemoteOwner(ctx context.Context, req RemoteOwnerDialRequest) (NodeMessageStream, error) {
	targetURL, err := d.resolveURL(ctx, req)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(targetURL) == "" {
		return nil, fmt.Errorf("yhttp: remote owner url vazia para %s", req.Resolution.Placement.NodeID)
	}

	options := cloneDialOptions(d.dialOptions)
	if options == nil {
		options = &websocket.DialOptions{}
	}
	headers := cloneHeader(options.HTTPHeader)
	if req.Header != nil {
		headers = mergeHTTPHeaders(headers, req.Header, "Authorization")
	}
	if d.authHeaders != nil {
		authHeaders, err := d.authHeaders(ctx, req)
		if err != nil {
			return nil, err
		}
		headers = mergeHTTPHeaders(headers, authHeaders)
	}
	if len(headers) > 0 {
		options.HTTPHeader = headers
	}

	socket, _, err := websocket.Dial(ctx, targetURL, options)
	if err != nil {
		return nil, err
	}
	socket.SetReadLimit(d.readLimitBytes)
	return newWebSocketNodeMessageStream(socket), nil
}

func mergeHTTPHeaders(dst http.Header, src http.Header, skipKeys ...string) http.Header {
	if len(src) == 0 {
		return dst
	}
	if dst == nil {
		dst = make(http.Header, len(src))
	}
	skip := make(map[string]struct{}, len(skipKeys))
	for _, key := range skipKeys {
		skip[http.CanonicalHeaderKey(key)] = struct{}{}
	}
	for key, values := range src {
		canonical := http.CanonicalHeaderKey(key)
		if _, ok := skip[canonical]; ok {
			continue
		}
		dst.Del(canonical)
		for _, value := range values {
			dst.Add(canonical, value)
		}
	}
	return dst
}

type webSocketNodeMessageStream struct {
	conn    *websocket.Conn
	writeMu sync.Mutex
}

func newWebSocketNodeMessageStream(conn *websocket.Conn) NodeMessageStream {
	return &webSocketNodeMessageStream{conn: conn}
}

func (s *webSocketNodeMessageStream) Send(ctx context.Context, message ynodeproto.Message) error {
	payload, err := ynodeproto.EncodeMessageFrame(message)
	if err != nil {
		return err
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.conn.Write(ctx, websocket.MessageBinary, payload)
}

func (s *webSocketNodeMessageStream) Receive(ctx context.Context) (ynodeproto.Message, error) {
	msgType, payload, err := s.conn.Read(ctx)
	if err != nil {
		if isIgnorableTransportError(err) {
			return nil, io.EOF
		}
		return nil, err
	}
	if msgType != websocket.MessageBinary {
		_ = s.conn.Close(websocket.StatusUnsupportedData, "yjs-crdt-golang-server aceita apenas frames binarios")
		return nil, fmt.Errorf("yhttp: node stream recebeu frame nao-binario: %v", msgType)
	}
	return ynodeproto.DecodeMessageFrameView(payload)
}

func (s *webSocketNodeMessageStream) Close() error {
	if s == nil || s.conn == nil {
		return nil
	}
	return s.conn.CloseNow()
}

func cloneDialOptions(src *websocket.DialOptions) *websocket.DialOptions {
	if src == nil {
		return nil
	}

	cloned := *src
	if src.HTTPHeader != nil {
		cloned.HTTPHeader = cloneHeader(src.HTTPHeader)
	}
	if len(src.Subprotocols) > 0 {
		cloned.Subprotocols = append([]string(nil), src.Subprotocols...)
	}
	return &cloned
}
