package yhttp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/drksbr/yjs-crdt-golang-server/pkg/storage"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/ycluster"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/yjsbridge"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/ynodeproto"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/yprotocol"
)

// Server implementa `http.Handler` para acoplar o provider local a qualquer
// stack HTTP compatível com `net/http`.
type Server struct {
	provider                      *yprotocol.Provider
	ownershipRuntime              *ycluster.DocumentOwnershipRuntime
	resolveRequest                ResolveRequestFunc
	acceptOptions                 *websocket.AcceptOptions
	readLimitBytes                int64
	writeTimeout                  time.Duration
	persistTimeout                time.Duration
	fanoutConcurrency             int
	bootstrapOnConnect            bool
	bootstrapSyncOnConnect        bool
	authorityRevalidationInterval time.Duration
	authenticator                 Authenticator
	authorizer                    Authorizer
	rateLimiter                   RateLimiter
	quotaLimiter                  QuotaLimiter
	originPolicy                  OriginPolicy
	redactor                      RequestRedactor
	metrics                       Metrics
	onError                       ErrorHandler

	registry               roomRegistry
	authorityRevalidations authorityRevalidationRegistry
	nextConnID             atomic.Uint64
}

const (
	authorityLostCloseReason        = "authority_lost"
	retryableRemoteOwnerCloseReason = "retryable_close"
)

type remoteOwnerCloseSignal struct {
	reason    string
	retryable bool
}

type serverSocketSessionOptions struct {
	observeConnectionLifecycle bool
	bootstrap                  bool
	bootstrapSync              bool
	authorityLossHandler       AuthorityLossHandler
	ownership                  *ycluster.DocumentOwnershipHandle
	quota                      QuotaLease
}

type authorityLossHandoffContextKey struct{}

type authorityLossHandoffState struct {
	clientErrCh  <-chan error
	cancelClient context.CancelFunc
	upstreamPeer *switchableRemoteStreamPeer
	quota        QuotaLease
}

func (s remoteOwnerCloseSignal) metricReason(fallback string) string {
	if strings.TrimSpace(s.reason) != "" {
		return s.reason
	}
	return normalizeRemoteOwnerCloseReason("", fallback)
}

func (s remoteOwnerCloseSignal) websocketReason(fallback string) string {
	return s.metricReason(fallback)
}

func (s remoteOwnerCloseSignal) websocketStatus() websocket.StatusCode {
	if s.retryable {
		return websocket.StatusTryAgainLater
	}
	return websocket.StatusGoingAway
}

// NewServer valida a configuração e constrói o handler HTTP/WebSocket.
func NewServer(cfg ServerConfig) (*Server, error) {
	if cfg.Provider == nil {
		return nil, ErrNilProvider
	}
	if cfg.ResolveRequest == nil {
		return nil, ErrNilResolveRequest
	}

	readLimit := cfg.ReadLimitBytes
	if readLimit <= 0 {
		readLimit = defaultReadLimitBytes
	}

	writeTimeout := cfg.WriteTimeout
	if writeTimeout <= 0 {
		writeTimeout = defaultWriteTimeout
	}

	persistTimeout := cfg.PersistTimeout
	if persistTimeout <= 0 {
		persistTimeout = defaultPersistTimeout
	}
	fanoutConcurrency := cfg.FanoutConcurrency
	if fanoutConcurrency <= 0 {
		fanoutConcurrency = defaultFanoutConcurrency
	}

	metrics := normalizeMetrics(cfg.Metrics)
	if cfg.Redactor != nil {
		metrics = redactingMetrics{base: metrics, redactor: cfg.Redactor}
	}

	return &Server{
		provider:                      cfg.Provider,
		ownershipRuntime:              cfg.OwnershipRuntime,
		resolveRequest:                cfg.ResolveRequest,
		acceptOptions:                 cloneAcceptOptionsForOriginPolicy(cfg.AcceptOptions, cfg.OriginPolicy),
		readLimitBytes:                readLimit,
		writeTimeout:                  writeTimeout,
		persistTimeout:                persistTimeout,
		fanoutConcurrency:             fanoutConcurrency,
		bootstrapOnConnect:            cfg.BootstrapOnConnect,
		bootstrapSyncOnConnect:        cfg.BootstrapSyncOnConnect,
		authorityRevalidationInterval: cfg.AuthorityRevalidationInterval,
		authenticator:                 cfg.Authenticator,
		authorizer:                    cfg.Authorizer,
		rateLimiter:                   cfg.RateLimiter,
		quotaLimiter:                  cfg.QuotaLimiter,
		originPolicy:                  cfg.OriginPolicy,
		redactor:                      cfg.Redactor,
		metrics:                       metrics,
		onError:                       cfg.OnError,
		registry:                      newRoomRegistry(),
		authorityRevalidations:        newAuthorityRevalidationRegistry(),
	}, nil
}

// ServeHTTP executa upgrade WebSocket, abre a conexão no provider e faz o
// fanout local dos frames retornados pelo runtime de protocolo.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.handleCORSPreflight(w, r) {
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	req, err := s.resolveRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.checkOrigin(w, r, req); err != nil {
		s.writeAuthError(w, req, err)
		return
	}
	req, err = s.authenticateAndAuthorize(r, req)
	if err != nil {
		s.writeAuthError(w, req, err)
		return
	}

	s.serveResolvedHTTP(w, r, req)
}

func (s *Server) serveResolvedHTTP(w http.ResponseWriter, r *http.Request, req Request) {
	s.serveResolvedHTTPWithOptions(w, r, req, serverSocketSessionOptions{
		observeConnectionLifecycle: true,
		bootstrap:                  s.bootstrapOnConnect,
		bootstrapSync:              s.bootstrapSyncOnConnect,
	})
}

func (s *Server) serveResolvedHTTPWithOptions(
	w http.ResponseWriter,
	r *http.Request,
	req Request,
	options serverSocketSessionOptions,
) {
	if err := req.DocumentKey.Validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := validateRequestSyncOutputFormat(req.SyncOutputFormat); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.ConnectionID) == "" {
		req.ConnectionID = s.newConnectionID()
	}

	quota, err := s.openQuota(r, req)
	if err != nil {
		s.writeAuthError(w, req, err)
		return
	}
	options.quota = quota

	ownership, err := s.acquireRequestOwnership(r.Context(), req)
	if err != nil {
		s.closeQuota(r, req, quota)
		s.metrics.Error(req, "acquire_ownership", err)
		status := statusFromOwnershipError(err)
		if status == http.StatusServiceUnavailable {
			w.Header().Set("Retry-After", "1")
		}
		http.Error(w, err.Error(), status)
		return
	}
	options.ownership = ownership

	connection, err := s.provider.Open(r.Context(), req.DocumentKey, req.ConnectionID, req.ClientID)
	if err != nil {
		s.closeQuota(r, req, quota)
		s.releaseRequestOwnership(r, req, ownership)
		s.metrics.Error(req, "open", err)
		status := statusFromOpenError(err)
		if status == http.StatusServiceUnavailable {
			w.Header().Set("Retry-After", "1")
		}
		http.Error(w, err.Error(), status)
		return
	}

	accepted := false
	defer func() {
		if accepted {
			return
		}
		if _, closeErr := connection.Close(); closeErr != nil {
			s.report(r, req, closeErr)
		}
		s.releaseRequestOwnership(r, req, ownership)
		s.closeQuota(r, req, quota)
	}()

	socket, err := websocket.Accept(w, r, s.acceptOptions)
	if err != nil {
		return
	}
	accepted = true
	s.serveConnectedSocket(r, req, connection, socket, options)
}

func (s *Server) authenticateAndAuthorize(r *http.Request, req Request) (Request, error) {
	if s == nil {
		return req, ErrUnauthorized
	}
	ctx := context.Background()
	if r != nil {
		ctx = r.Context()
	}

	var principal *Principal
	var err error
	if s.authenticator != nil {
		principal, err = s.authenticator.AuthenticateHTTP(ctx, r)
		if err != nil {
			s.metrics.Error(req, "authenticate", err)
			s.report(r, req, err)
			return req, err
		}
		principal = clonePrincipal(principal)
		req.Principal = principal
	}

	if s.authorizer != nil {
		if err := s.authorizer.AuthorizeHTTP(ctx, principal, req); err != nil {
			s.metrics.Error(req, "authorize", err)
			s.report(r, req, err)
			return req, err
		}
	}
	if s.rateLimiter != nil {
		if err := s.rateLimiter.AllowHTTP(ctx, r, principal, req); err != nil {
			s.metrics.Error(req, "rate_limit", err)
			s.report(r, req, err)
			return req, err
		}
	}
	return req, nil
}

func (s *Server) handleCORSPreflight(w http.ResponseWriter, r *http.Request) bool {
	if s == nil || s.originPolicy == nil {
		return false
	}
	preflight, ok := s.originPolicy.(CORSPreflightPolicy)
	if !ok {
		return false
	}
	ctx := context.Background()
	if r != nil {
		ctx = r.Context()
	}
	return preflight.HandleCORSPreflight(ctx, w, r)
}

func (s *Server) checkOrigin(w http.ResponseWriter, r *http.Request, req Request) error {
	if s == nil || s.originPolicy == nil {
		return nil
	}
	ctx := context.Background()
	if r != nil {
		ctx = r.Context()
	}
	if err := s.originPolicy.CheckHTTPOrigin(ctx, r, req); err != nil {
		s.metrics.Error(req, "origin", err)
		s.report(r, req, err)
		return err
	}
	if cors, ok := s.originPolicy.(CORSHeaderPolicy); ok {
		cors.WriteCORSHeaders(w, r)
	}
	return nil
}

func (s *Server) writeAuthError(w http.ResponseWriter, req Request, err error) {
	status := statusFromAuthError(err)
	if s != nil {
		s.metrics.Error(req, "auth", err)
	}
	http.Error(w, http.StatusText(status), status)
}

func (s *Server) openQuota(r *http.Request, req Request) (QuotaLease, error) {
	if s == nil || s.quotaLimiter == nil {
		return nil, nil
	}
	ctx := context.Background()
	if r != nil {
		ctx = r.Context()
	}
	quota, err := s.quotaLimiter.OpenQuota(ctx, r, req)
	if err != nil {
		s.metrics.Error(req, "quota_open", err)
		s.report(r, req, err)
		return nil, err
	}
	return quota, nil
}

func (s *Server) closeQuota(r *http.Request, req Request, quota QuotaLease) {
	if quota == nil {
		return
	}
	ctx := context.Background()
	if r != nil {
		ctx = r.Context()
	}
	if err := quota.Close(ctx); err != nil {
		s.metrics.Error(req, "quota_close", err)
		s.report(r, req, err)
	}
}

func (s *Server) allowQuotaFrame(r *http.Request, req Request, quota QuotaLease, direction QuotaDirection, bytes int) error {
	if quota == nil {
		return nil
	}
	ctx := context.Background()
	if r != nil {
		ctx = r.Context()
	}
	if err := quota.AllowFrame(ctx, direction, bytes); err != nil {
		s.metrics.Error(req, "quota_frame", err)
		s.report(r, req, err)
		return err
	}
	return nil
}

func quotaCloseStatus(err error) websocket.StatusCode {
	if errors.Is(err, ErrQuotaUnavailable) {
		return websocket.StatusTryAgainLater
	}
	return websocket.StatusPolicyViolation
}

func quotaCloseReason(err error) string {
	if errors.Is(err, ErrQuotaUnavailable) {
		return "quota indisponivel"
	}
	return "quota excedida"
}

func quotaWrapPeer(peer roomPeer, quota QuotaLease) roomPeer {
	if peer == nil || quota == nil {
		return peer
	}
	return quotaPeer{base: peer, quota: quota}
}

func (s *Server) serveConnectedSocket(
	r *http.Request,
	req Request,
	connection *yprotocol.Connection,
	socket *websocket.Conn,
	options serverSocketSessionOptions,
) {
	defer s.closeQuota(r, req, options.quota)

	if options.authorityLossHandler != nil {
		s.serveSwitchableConnectedSocket(r, req, connection, socket, options)
		return
	}

	socket.SetReadLimit(s.readLimitBytes)

	sessionCtx, cancelSession := context.WithCancel(r.Context())
	defer cancelSession()
	if options.observeConnectionLifecycle {
		s.metrics.ConnectionOpened(req)
		defer s.metrics.ConnectionClosed(req)
	}

	peer := s.registry.add(req.DocumentKey, req.ConnectionID, s.socketPeer(socket, req, options.quota))
	var onAuthorityLoss func(remoteOwnerCloseSignal)
	if options.authorityLossHandler == nil {
		onAuthorityLoss = func(signal remoteOwnerCloseSignal) {
			if closeErr := socket.Close(signal.websocketStatus(), signal.websocketReason(authorityLostCloseReason)); closeErr != nil && !isIgnorableTransportError(closeErr) {
				s.metrics.Error(req, "close_revalidate_authority", closeErr)
				s.report(nil, req, closeErr)
			}
		}
	}
	revalidateCh := s.startAuthorityRevalidator(sessionCtx, req, connection, cancelSession, onAuthorityLoss)
	defer drainRemoteOwnerCloseSignal(revalidateCh)

	if options.bootstrap {
		if err := s.bootstrapConnection(r, req, connection, peer, options.bootstrapSync); err != nil {
			if isAuthorityLostRetryableError(err) {
				s.cleanupConnectionWithOwnership(r, req, connection, options.ownership)
				s.handleAuthorityLoss(r, req, socket, connection.AuthorityEpoch(), options.authorityLossHandler)
				return
			}
			s.metrics.Error(req, "bootstrap", err)
			s.report(r, req, err)
			closeStatus, closeReason := websocketCloseFromRuntimeError(
				err,
				websocket.StatusGoingAway,
				"falha ao assumir owner local",
			)
			if closeErr := socket.Close(closeStatus, closeReason); closeErr != nil && !isIgnorableTransportError(closeErr) {
				s.metrics.Error(req, "close_bootstrap", closeErr)
				s.report(r, req, closeErr)
			}
			s.cleanupConnectionWithOwnership(r, req, connection, options.ownership)
			s.closeSocket(r, req, socket)
			return
		}
	}

	for {
		msgType, payload, err := socket.Read(sessionCtx)
		if err != nil {
			if signal, ok := drainRemoteOwnerCloseSignal(revalidateCh); ok {
				s.cleanupConnectionWithOwnership(r, req, connection, options.ownership)
				s.handleAuthorityLoss(r, req, socket, connection.AuthorityEpoch(), options.authorityLossHandler, signal)
				return
			}
			if status := websocket.CloseStatus(err); !isExpectedClientCloseStatus(status) && !isIgnorableTransportError(err) {
				s.metrics.Error(req, "read", err)
				s.report(r, req, err)
			}
			s.cleanupConnectionWithOwnership(r, req, connection, options.ownership)
			s.closeSocket(r, req, socket)
			return
		}
		s.metrics.FrameRead(req, len(payload))
		if msgType != websocket.MessageBinary {
			if closeErr := socket.Close(websocket.StatusUnsupportedData, "yjs-crdt-golang-server aceita apenas frames binarios"); closeErr != nil {
				s.metrics.Error(req, "close_unsupported_data", closeErr)
				s.report(r, req, closeErr)
			}
			s.cleanupConnectionWithOwnership(r, req, connection, options.ownership)
			s.closeSocket(r, req, socket)
			return
		}
		if err := s.allowQuotaFrame(r, req, options.quota, QuotaDirectionInbound, len(payload)); err != nil {
			if closeErr := socket.Close(quotaCloseStatus(err), quotaCloseReason(err)); closeErr != nil && !isIgnorableTransportError(closeErr) {
				s.metrics.Error(req, "close_quota", closeErr)
				s.report(r, req, closeErr)
			}
			s.cleanupConnectionWithOwnership(r, req, connection, options.ownership)
			s.closeSocket(r, req, socket)
			return
		}

		handleStart := time.Now()
		result, err := connection.HandleEncodedMessagesContext(sessionCtx, payload)
		s.metrics.Handle(req, time.Since(handleStart), err)
		if err != nil {
			if isAuthorityLostRetryableError(err) {
				s.cleanupConnectionWithOwnership(r, req, connection, options.ownership)
				s.handleAuthorityLoss(
					r,
					req,
					socket,
					connection.AuthorityEpoch(),
					options.authorityLossHandler,
					remoteOwnerCloseSignalFromError(err, "handle"),
				)
				return
			}
			s.metrics.Error(req, "handle", err)
			s.report(r, req, err)
			closeStatus, closeReason := websocketCloseFromRuntimeError(
				err,
				websocket.StatusPolicyViolation,
				"payload do y-protocol invalido",
			)
			if closeErr := socket.Close(closeStatus, closeReason); closeErr != nil {
				s.metrics.Error(req, "close_policy_violation", closeErr)
				s.report(r, req, closeErr)
			}
			s.cleanupConnectionWithOwnership(r, req, connection, options.ownership)
			s.closeSocket(r, req, socket)
			return
		}

		if len(result.Direct) > 0 {
			if err := s.writeBinary(peer, result.Direct); err != nil {
				if !isIgnorableTransportError(err) {
					s.metrics.Error(req, "write_direct", err)
					s.report(r, req, err)
				}
				s.cleanupConnectionWithOwnership(r, req, connection, options.ownership)
				s.closeSocket(r, req, socket)
				return
			}
			s.metrics.FrameWritten(req, "direct", len(result.Direct))
		}
		if len(result.Broadcast) > 0 {
			s.fanout(r, req, result.Broadcast)
		}
	}
}

func (s *Server) serveSwitchableConnectedSocket(
	r *http.Request,
	req Request,
	connection *yprotocol.Connection,
	socket *websocket.Conn,
	options serverSocketSessionOptions,
) {
	socket.SetReadLimit(s.readLimitBytes)

	sessionCtx, cancelSession := context.WithCancel(r.Context())
	defer cancelSession()
	if options.observeConnectionLifecycle {
		s.metrics.ConnectionOpened(req)
		defer s.metrics.ConnectionClosed(req)
	}

	peer := s.socketPeer(socket, req, options.quota)
	s.registry.add(req.DocumentKey, req.ConnectionID, peer)
	revalidateCh := s.startAuthorityRevalidator(sessionCtx, req, connection, cancelSession, nil)
	defer drainRemoteOwnerCloseSignal(revalidateCh)
	notifyAuthorityLoss := func(signal remoteOwnerCloseSignal) {
		signalRemoteOwnerClose(revalidateCh, signal)
		cancelSession()
	}

	if options.bootstrap {
		if err := s.bootstrapConnection(r, req, connection, peer, options.bootstrapSync); err != nil {
			if isAuthorityLostRetryableError(err) {
				s.cleanupConnectionWithOwnership(r, req, connection, options.ownership)
				s.handleAuthorityLoss(r, req, socket, connection.AuthorityEpoch(), options.authorityLossHandler)
				return
			}
			s.metrics.Error(req, "bootstrap", err)
			s.report(r, req, err)
			closeStatus, closeReason := websocketCloseFromRuntimeError(
				err,
				websocket.StatusGoingAway,
				"falha ao assumir owner local",
			)
			if closeErr := socket.Close(closeStatus, closeReason); closeErr != nil && !isIgnorableTransportError(closeErr) {
				s.metrics.Error(req, "close_bootstrap", closeErr)
				s.report(r, req, closeErr)
			}
			s.cleanupConnectionWithOwnership(r, req, connection, options.ownership)
			return
		}
	}

	upstreamPeer := newSwitchableRemoteStreamPeer(req.DocumentKey, req.ConnectionID)
	upstreamPeer.switchTarget(&localConnectionPeer{
		server:          s,
		req:             req,
		connection:      connection,
		peer:            peer,
		quota:           options.quota,
		onAuthorityLoss: notifyAuthorityLoss,
	})

	clientCtx, cancelClient := context.WithCancel(r.Context())
	defer cancelClient()
	clientErrCh := make(chan error, 1)
	go func() {
		clientErrCh <- s.pipeClientToLocal(clientCtx, req, socket, upstreamPeer, options.quota)
	}()

	handoffReq := r.WithContext(context.WithValue(r.Context(), authorityLossHandoffContextKey{}, &authorityLossHandoffState{
		clientErrCh:  clientErrCh,
		cancelClient: cancelClient,
		upstreamPeer: upstreamPeer,
		quota:        options.quota,
	}))

	for {
		select {
		case err := <-clientErrCh:
			if signal, ok := drainRemoteOwnerCloseSignal(revalidateCh); ok {
				previousEpoch := connection.AuthorityEpoch()
				upstreamPeer.clearSession()
				s.cleanupConnectionWithOwnership(r, req, connection, options.ownership)
				s.handleAuthorityLoss(handoffReq, req, socket, previousEpoch, options.authorityLossHandler, signal)
				return
			}
			s.cleanupConnectionWithOwnership(r, req, connection, options.ownership)
			if err != nil && !isIgnorableTransportError(err) {
				s.metrics.Error(req, "read", err)
				s.report(r, req, err)
			}
			s.closeSocket(r, req, socket)
			return
		case <-sessionCtx.Done():
			if signal, ok := drainRemoteOwnerCloseSignal(revalidateCh); ok {
				previousEpoch := connection.AuthorityEpoch()
				upstreamPeer.clearSession()
				s.cleanupConnectionWithOwnership(r, req, connection, options.ownership)
				s.handleAuthorityLoss(handoffReq, req, socket, previousEpoch, options.authorityLossHandler, signal)
				return
			}
			s.cleanupConnectionWithOwnership(r, req, connection, options.ownership)
			cancelClient()
			s.closeSocket(r, req, socket)
			return
		}
	}
}

func (s *Server) bootstrapConnection(r *http.Request, req Request, connection *yprotocol.Connection, peer roomPeer, includeSync bool) error {
	if includeSync {
		if err := s.bootstrapConnectionPayload(r, req, connection, peer, yprotocol.EncodeProtocolSyncStep1([]byte{0x00})); err != nil {
			return err
		}
	}
	return s.bootstrapConnectionPayload(r, req, connection, peer, yprotocol.EncodeProtocolQueryAwareness())
}

func (s *Server) bootstrapConnectionPayload(r *http.Request, req Request, connection *yprotocol.Connection, peer roomPeer, payload []byte) error {
	handleStart := time.Now()
	result, err := connection.HandleEncodedMessagesContext(r.Context(), payload)
	s.metrics.Handle(req, time.Since(handleStart), err)
	if err != nil {
		return err
	}
	if len(result.Direct) > 0 {
		if err := s.writeBinary(peer, result.Direct); err != nil {
			return err
		}
		s.metrics.FrameWritten(req, "direct", len(result.Direct))
	}
	if len(result.Broadcast) > 0 {
		s.fanout(r, req, result.Broadcast)
	}
	return nil
}

func (s *Server) acquireRequestOwnership(ctx context.Context, req Request) (*ycluster.DocumentOwnershipHandle, error) {
	if s.ownershipRuntime == nil {
		return nil, nil
	}
	return s.ownershipRuntime.AcquireDocumentOwnership(ctx, ycluster.ClaimDocumentRequest{
		DocumentKey: req.DocumentKey,
	})
}

func (s *Server) cleanupConnectionWithOwnership(
	r *http.Request,
	req Request,
	connection *yprotocol.Connection,
	ownership *ycluster.DocumentOwnershipHandle,
) {
	s.registry.remove(req.DocumentKey, req.ConnectionID)

	if req.PersistOnClose && !connection.AuthorityLost() {
		ctx, cancel := context.WithTimeout(context.Background(), s.persistTimeout)
		persistStart := time.Now()
		_, err := connection.Persist(ctx)
		s.metrics.Persist(req, time.Since(persistStart), err)
		if err != nil {
			s.metrics.Error(req, "persist", err)
			s.report(r, req, err)
		}
		cancel()
	}

	result, err := connection.Close()
	if err != nil {
		s.metrics.Error(req, "close_connection", err)
		s.report(r, req, err)
	} else if len(result.Broadcast) > 0 {
		s.fanout(r, req, result.Broadcast)
	}

	s.releaseRequestOwnership(r, req, ownership)
}

func (s *Server) releaseRequestOwnership(r *http.Request, req Request, ownership *ycluster.DocumentOwnershipHandle) {
	if ownership == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.persistTimeout)
	err := ownership.Release(ctx)
	cancel()
	if err == nil {
		return
	}
	s.metrics.Error(req, "release_ownership", err)
	s.report(r, req, err)
}

func (s *Server) closeSocket(r *http.Request, req Request, socket *websocket.Conn) {
	if err := socket.CloseNow(); err != nil {
		if isIgnorableTransportError(err) {
			return
		}
		s.metrics.Error(req, "close_socket", err)
		s.report(r, req, err)
	}
}

func (s *Server) pipeClientToLocal(
	ctx context.Context,
	req Request,
	socket *websocket.Conn,
	peer *switchableRemoteStreamPeer,
	quota QuotaLease,
) error {
	for {
		msgType, payload, err := socket.Read(ctx)
		if err != nil {
			if status := websocket.CloseStatus(err); !isExpectedClientCloseStatus(status) && !isIgnorableTransportError(err) {
				return err
			}
			return nil
		}

		s.metrics.FrameRead(req, len(payload))
		if msgType != websocket.MessageBinary {
			if closeErr := socket.Close(websocket.StatusUnsupportedData, "yjs-crdt-golang-server aceita apenas frames binarios"); closeErr != nil && !isIgnorableTransportError(closeErr) {
				s.metrics.Error(req, "close_unsupported_data", closeErr)
				s.report(nil, req, closeErr)
			}
			return nil
		}

		if err := s.allowQuotaFrame(nil, req, quota, QuotaDirectionInbound, len(payload)); err != nil {
			if closeErr := socket.Close(quotaCloseStatus(err), quotaCloseReason(err)); closeErr != nil && !isIgnorableTransportError(closeErr) {
				s.metrics.Error(req, "close_quota", closeErr)
				s.report(nil, req, closeErr)
			}
			return err
		}

		if err := peer.deliver(ctx, payload); err != nil {
			if isIgnorableTransportError(err) {
				return nil
			}
			s.metrics.Error(req, "write_direct", err)
			s.report(nil, req, err)
			return err
		}
	}
}

func (s *Server) fanout(r *http.Request, req Request, payload []byte) {
	peers := s.registry.peersExcept(req.DocumentKey, req.ConnectionID)
	if len(peers) == 0 {
		return
	}
	payloads := newBroadcastPayloads(payload)
	if err := payloads.prepare(peers); err != nil {
		s.metrics.Error(req, "prepare_broadcast", err)
		s.report(r, req, err)
	}
	results := s.writeBroadcastPeers(peers, payloads)
	for idx, result := range results {
		if result != nil {
			if !isIgnorableTransportError(result) {
				s.metrics.Error(req, "write_broadcast", result)
				s.report(r, req, result)
			}
			if closeErr := peers[idx].close("falha ao entregar broadcast local"); closeErr != nil {
				if isIgnorableTransportError(closeErr) {
					continue
				}
				s.metrics.Error(req, "close_broadcast_peer", closeErr)
				s.report(r, req, closeErr)
			}
			continue
		}
		s.metrics.FrameWritten(req, "broadcast", len(payload))
	}
}

type broadcastPayloads struct {
	v1    []byte
	v2    []byte
	v2Err error
}

func newBroadcastPayloads(payload []byte) *broadcastPayloads {
	return &broadcastPayloads{v1: payload}
}

func (p *broadcastPayloads) prepare(peers []roomPeer) error {
	for _, peer := range peers {
		if prepared, ok := peer.(preparedSyncOutputPeer); ok && prepared.syncOutputFormat() == yjsbridge.UpdateFormatV2 {
			p.v2, p.v2Err = protocolPayloadForSyncOutputFormat(p.v1, yjsbridge.UpdateFormatV2)
			return p.v2Err
		}
	}
	return nil
}

func (p *broadcastPayloads) payloadFor(peer roomPeer) ([]byte, error) {
	if prepared, ok := peer.(preparedSyncOutputPeer); ok && prepared.syncOutputFormat() == yjsbridge.UpdateFormatV2 {
		return p.v2, p.v2Err
	}
	return p.v1, nil
}

func (s *Server) writeBroadcastPeers(peers []roomPeer, payloads *broadcastPayloads) []error {
	errs := make([]error, len(peers))
	concurrency := s.fanoutConcurrency
	if concurrency <= 1 || len(peers) <= fanoutParallelThreshold {
		for idx, peer := range peers {
			errs[idx] = s.writeBroadcastPeer(peer, payloads)
		}
		return errs
	}
	if concurrency > len(peers) {
		concurrency = len(peers)
	}

	jobs := make(chan int)
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for worker := 0; worker < concurrency; worker++ {
		go func() {
			defer wg.Done()
			for idx := range jobs {
				errs[idx] = s.writeBroadcastPeer(peers[idx], payloads)
			}
		}()
	}
	for idx := range peers {
		jobs <- idx
	}
	close(jobs)
	wg.Wait()
	return errs
}

func (s *Server) writeBroadcastPeer(peer roomPeer, payloads *broadcastPayloads) error {
	payload, err := payloads.payloadFor(peer)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.writeTimeout)
	defer cancel()
	if prepared, ok := peer.(preparedSyncOutputPeer); ok {
		return prepared.deliverPrepared(ctx, payload)
	}
	return peer.deliver(ctx, payload)
}

func (s *Server) writeBinary(peer roomPeer, payload []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.writeTimeout)
	defer cancel()
	return peer.deliver(ctx, payload)
}

func (s *Server) socketPeer(socket *websocket.Conn, req Request, quota QuotaLease) roomPeer {
	return syncOutputFormatWrapPeer(quotaWrapPeer(&websocketPeer{conn: socket}, quota), req.SyncOutputFormat)
}

func (s *Server) report(r *http.Request, req Request, err error) {
	if s.onError != nil && err != nil {
		s.onError(redactHTTPRequest(s.redactor, r), redactRequest(s.redactor, req), err)
	}
}

func (s *Server) newConnectionID() string {
	return fmt.Sprintf("conn-%d", s.nextConnID.Add(1))
}

func statusFromOpenError(err error) int {
	switch {
	case errors.Is(err, storage.ErrInvalidDocumentKey), errors.Is(err, yprotocol.ErrInvalidConnectionID):
		return http.StatusBadRequest
	case errors.Is(err, yprotocol.ErrConnectionExists):
		return http.StatusConflict
	case isAuthorityLostRetryableError(err):
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

func statusFromOwnershipError(err error) int {
	switch {
	case errors.Is(err, storage.ErrInvalidDocumentKey), errors.Is(err, ycluster.ErrInvalidLeaseRequest):
		return http.StatusBadRequest
	case errors.Is(err, ycluster.ErrLeaseHeld),
		errors.Is(err, ycluster.ErrLeaseExpired),
		errors.Is(err, ycluster.ErrOwnershipRuntimeClosed),
		errors.Is(err, context.Canceled),
		errors.Is(err, context.DeadlineExceeded):
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

func isAuthorityLostRetryableError(err error) bool {
	return errors.Is(err, yprotocol.ErrAuthorityLost) || errors.Is(err, storage.ErrAuthorityLost)
}

func websocketCloseFromRuntimeError(
	err error,
	fallbackStatus websocket.StatusCode,
	fallbackReason string,
) (websocket.StatusCode, string) {
	if isAuthorityLostRetryableError(err) {
		return websocket.StatusTryAgainLater, authorityLostCloseReason
	}
	return fallbackStatus, fallbackReason
}

func remoteOwnerCloseSignalFromError(err error, fallback string) remoteOwnerCloseSignal {
	if isAuthorityLostRetryableError(err) {
		return remoteOwnerCloseSignal{
			reason:    authorityLostCloseReason,
			retryable: true,
		}
	}
	return remoteOwnerCloseSignal{
		reason: normalizeRemoteOwnerCloseReason(fallback, "close"),
	}
}

func remoteOwnerCloseSignalFromMessage(message *ynodeproto.Close) remoteOwnerCloseSignal {
	if message == nil {
		return remoteOwnerCloseSignal{}
	}

	fallback := "remote_close"
	if message.Retryable {
		fallback = retryableRemoteOwnerCloseReason
	}
	return remoteOwnerCloseSignal{
		reason:    normalizeRemoteOwnerCloseReason(message.Reason, fallback),
		retryable: message.Retryable,
	}
}

func (s *Server) handleAuthorityLoss(
	r *http.Request,
	req Request,
	socket *websocket.Conn,
	previousEpoch uint64,
	handler AuthorityLossHandler,
	signals ...remoteOwnerCloseSignal,
) {
	signal := remoteOwnerCloseSignal{
		reason:    authorityLostCloseReason,
		retryable: true,
	}
	if len(signals) > 0 {
		signal = signals[0]
	}

	if handler != nil {
		if err := handler(r, req, socket, previousEpoch); err == nil {
			return
		} else {
			s.metrics.Error(req, "authority_loss_handoff", err)
			s.report(r, req, err)
		}
	}

	if closeErr := socket.Close(signal.websocketStatus(), signal.websocketReason(authorityLostCloseReason)); closeErr != nil && !isIgnorableTransportError(closeErr) {
		s.metrics.Error(req, "close_authority_loss", closeErr)
		s.report(r, req, closeErr)
	}
}

func (s *Server) startAuthorityRevalidator(
	ctx context.Context,
	req Request,
	connection *yprotocol.Connection,
	cancelSession context.CancelFunc,
	onAuthorityLoss func(remoteOwnerCloseSignal),
) chan remoteOwnerCloseSignal {
	signals := make(chan remoteOwnerCloseSignal, 1)
	if s == nil || connection == nil {
		return signals
	}

	session := &authorityRevalidationSession{
		req:             req,
		connection:      connection,
		cancelSession:   cancelSession,
		signals:         signals,
		onAuthorityLoss: onAuthorityLoss,
	}
	connectionID := connection.ID()
	s.authorityRevalidations.add(req.DocumentKey, connectionID, session)
	if s.authorityRevalidationInterval <= 0 {
		context.AfterFunc(ctx, func() {
			s.authorityRevalidations.remove(req.DocumentKey, connectionID)
		})
		return signals
	}

	go func() {
		ticker := time.NewTicker(s.authorityRevalidationInterval)
		defer func() {
			ticker.Stop()
			s.authorityRevalidations.remove(req.DocumentKey, connectionID)
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				checkCtx, cancel := context.WithTimeout(context.Background(), s.writeTimeout)
				err := s.revalidateAuthoritySession(checkCtx, session)
				cancel()
				switch {
				case err == nil, errors.Is(err, yprotocol.ErrConnectionClosed):
					continue
				case isAuthorityLostRetryableError(err):
					return
				default:
				}
			}
		}
	}()

	return signals
}

func signalRemoteOwnerClose(ch chan<- remoteOwnerCloseSignal, signal remoteOwnerCloseSignal) {
	select {
	case ch <- signal:
	default:
	}
}

func drainRemoteOwnerCloseSignal(ch <-chan remoteOwnerCloseSignal) (remoteOwnerCloseSignal, bool) {
	if ch == nil {
		return remoteOwnerCloseSignal{}, false
	}

	select {
	case signal := <-ch:
		return signal, true
	default:
		return remoteOwnerCloseSignal{}, false
	}
}

func cloneAcceptOptions(src *websocket.AcceptOptions) *websocket.AcceptOptions {
	if src == nil {
		return nil
	}

	cloned := *src
	if len(src.Subprotocols) > 0 {
		cloned.Subprotocols = append([]string(nil), src.Subprotocols...)
	}
	if len(src.OriginPatterns) > 0 {
		cloned.OriginPatterns = append([]string(nil), src.OriginPatterns...)
	}
	return &cloned
}

func cloneAcceptOptionsForOriginPolicy(src *websocket.AcceptOptions, policy OriginPolicy) *websocket.AcceptOptions {
	cloned := cloneAcceptOptions(src)
	websocketPolicy, ok := policy.(websocketOriginPolicy)
	if !ok {
		return cloned
	}
	patterns, insecureSkipVerify := websocketPolicy.websocketOriginPatterns()
	if insecureSkipVerify {
		if cloned == nil {
			cloned = &websocket.AcceptOptions{}
		}
		cloned.InsecureSkipVerify = true
		cloned.OriginPatterns = nil
		return cloned
	}
	if len(patterns) == 0 {
		return cloned
	}
	if cloned == nil {
		cloned = &websocket.AcceptOptions{}
	}
	for _, pattern := range patterns {
		if !containsOriginPattern(cloned.OriginPatterns, pattern) {
			cloned.OriginPatterns = append(cloned.OriginPatterns, pattern)
		}
	}
	return cloned
}

func containsOriginPattern(patterns []string, pattern string) bool {
	for _, current := range patterns {
		if strings.EqualFold(strings.TrimSpace(current), strings.TrimSpace(pattern)) {
			return true
		}
	}
	return false
}

func isIgnorableTransportError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, net.ErrClosed) || errors.Is(err, context.Canceled) {
		return true
	}
	return strings.Contains(err.Error(), "use of closed network connection")
}

func isExpectedClientCloseStatus(status websocket.StatusCode) bool {
	return status == websocket.StatusNormalClosure ||
		status == websocket.StatusGoingAway ||
		status == websocket.StatusTryAgainLater
}
