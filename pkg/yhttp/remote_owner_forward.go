package yhttp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/drksbr/yjs-crdt-golang-server/pkg/storage"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/ycluster"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/yjsbridge"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/ynodeproto"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/yprotocol"
)

const remoteOwnerDialErrorStatusCode = http.StatusBadGateway

const (
	defaultRemoteOwnerRebindTimeout  = 2 * time.Second
	defaultRemoteOwnerRebindInterval = 50 * time.Millisecond
)

type remoteOwnerClosedError struct {
	signal remoteOwnerCloseSignal
}

func (e *remoteOwnerClosedError) Error() string {
	if e == nil {
		return "yhttp: owner remoto encerrou a conexao"
	}
	return fmt.Sprintf("yhttp: owner remoto encerrou a conexao (%s)", e.signal.metricReason("remote_close"))
}

// RemoteOwnerDialRequest descreve o contexto necessário para abrir um stream
// com o owner remoto resolvido.
type RemoteOwnerDialRequest struct {
	Request    Request
	Resolution ycluster.OwnerResolution
	Header     http.Header
}

// RemoteOwnerDialer abre um stream bidirecional para o owner remoto.
type RemoteOwnerDialer interface {
	DialRemoteOwner(ctx context.Context, req RemoteOwnerDialRequest) (NodeMessageStream, error)
}

// NodeMessageStream representa um stream bidirecional de mensagens tipadas
// entre edge e owner remoto, independente do transporte subjacente.
type NodeMessageStream interface {
	Send(ctx context.Context, message ynodeproto.Message) error
	Receive(ctx context.Context) (ynodeproto.Message, error)
	Close() error
}

// RemoteOwnerForwardConfig define o wiring do handler de forwarding remoto.
//
// O handler resultante deve ser usado em `OwnerAwareServerConfig.OnRemoteOwner`.
// Quando a request não for um upgrade WebSocket, o handler retorna `false` para
// permitir que `OwnerAwareServer` preserve o fallback HTTP com metadados do
// owner remoto.
type RemoteOwnerForwardConfig struct {
	LocalNodeID    ycluster.NodeID
	Local          *Server
	Dialer         RemoteOwnerDialer
	OwnerLookup    ycluster.OwnerLookup
	AcceptOptions  *websocket.AcceptOptions
	ReadLimitBytes int64
	WriteTimeout   time.Duration
	RebindTimeout  time.Duration
	RebindInterval time.Duration
	OriginPolicy   OriginPolicy
	Redactor       RequestRedactor
	Metrics        Metrics
	OnError        ErrorHandler
}

type remoteOwnerForwarder struct {
	localNodeID    ycluster.NodeID
	local          *Server
	dialer         RemoteOwnerDialer
	ownerLookup    ycluster.OwnerLookup
	acceptOptions  *websocket.AcceptOptions
	readLimitBytes int64
	writeTimeout   time.Duration
	rebindTimeout  time.Duration
	rebindInterval time.Duration
	redactor       RequestRedactor
	metrics        Metrics
	onError        ErrorHandler
}

type remoteOwnerSession struct {
	stream            NodeMessageStream
	resolution        ycluster.OwnerResolution
	epoch             uint64
	ownerToEdgeFormat yjsbridge.UpdateFormat
}

type remoteOwnerBridgeResult struct {
	closeReason     string
	retryableSignal *remoteOwnerCloseSignal
}

type remoteOwnerRebindTarget struct {
	session         remoteOwnerSession
	localResolution *ycluster.OwnerResolution
}

type forwardDeliveryTarget interface {
	deliver(ctx context.Context, payload []byte) error
}

type switchableRemoteStreamPeer struct {
	documentKey  storage.DocumentKey
	connectionID string

	mu      sync.RWMutex
	target  forwardDeliveryTarget
	readyCh chan struct{}
}

// NewRemoteOwnerForwardHandlers constroi o par de handlers owner-aware para:
// - encaminhamento inicial quando o owner resolvido ja e remoto; e
// - handoff transparente quando uma sessao local perde autoridade.
func NewRemoteOwnerForwardHandlers(
	cfg RemoteOwnerForwardConfig,
) (RemoteOwnerHandler, AuthorityLossHandler, error) {
	forwarder, err := newRemoteOwnerForwarder(cfg)
	if err != nil {
		return nil, nil, err
	}
	return forwarder.handle, forwarder.handleLocalAuthorityLoss, nil
}

// NewRemoteOwnerForwardHandler constrói um `RemoteOwnerHandler` que aceita a
// conexão WebSocket do cliente e faz bridge de frames binários com um owner
// remoto via `RemoteOwnerDialer`.
func NewRemoteOwnerForwardHandler(cfg RemoteOwnerForwardConfig) (RemoteOwnerHandler, error) {
	forwardRemoteOwner, _, err := NewRemoteOwnerForwardHandlers(cfg)
	return forwardRemoteOwner, err
}

func newRemoteOwnerForwarder(cfg RemoteOwnerForwardConfig) (*remoteOwnerForwarder, error) {
	if cfg.Dialer == nil {
		return nil, ErrNilRemoteOwnerDialer
	}
	if err := cfg.LocalNodeID.Validate(); err != nil {
		return nil, err
	}

	readLimit := cfg.ReadLimitBytes
	if readLimit <= 0 {
		readLimit = defaultReadLimitBytes
	}

	writeTimeout := cfg.WriteTimeout
	if writeTimeout <= 0 {
		writeTimeout = defaultWriteTimeout
	}

	rebindTimeout := cfg.RebindTimeout
	if rebindTimeout <= 0 {
		rebindTimeout = defaultRemoteOwnerRebindTimeout
	}

	rebindInterval := cfg.RebindInterval
	if rebindInterval <= 0 {
		rebindInterval = defaultRemoteOwnerRebindInterval
	}

	redactor := cfg.Redactor
	if redactor == nil && cfg.Local != nil {
		redactor = cfg.Local.redactor
	}

	metrics := normalizeMetrics(cfg.Metrics)
	if redactor != nil {
		metrics = redactingMetrics{base: metrics, redactor: redactor}
	}

	originPolicy := cfg.OriginPolicy
	if originPolicy == nil && cfg.Local != nil {
		originPolicy = cfg.Local.originPolicy
	}

	forwarder := &remoteOwnerForwarder{
		localNodeID:    cfg.LocalNodeID,
		local:          cfg.Local,
		dialer:         cfg.Dialer,
		ownerLookup:    cfg.OwnerLookup,
		acceptOptions:  cloneAcceptOptionsForOriginPolicy(cfg.AcceptOptions, originPolicy),
		readLimitBytes: readLimit,
		writeTimeout:   writeTimeout,
		rebindTimeout:  rebindTimeout,
		rebindInterval: rebindInterval,
		redactor:       redactor,
		metrics:        metrics,
		onError:        cfg.OnError,
	}
	return forwarder, nil
}

func (f *remoteOwnerForwarder) handle(w http.ResponseWriter, r *http.Request, req Request, resolution ycluster.OwnerResolution) bool {
	if !isWebSocketUpgrade(r) {
		return false
	}

	epoch, err := remoteOwnerEpoch(resolution)
	if err != nil {
		f.metrics.Error(req, "remote_owner_epoch", err)
		f.report(r, req, err)
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return true
	}

	header := cloneHeader(r.Header)
	quota, err := f.openQuota(r, req)
	if err != nil {
		status := statusFromAuthError(err)
		http.Error(w, http.StatusText(status), status)
		return true
	}
	session, err := f.openRemoteOwnerSession(r.Context(), req, resolution, epoch, header, false)
	if err != nil {
		f.closeQuota(r, req, quota)
		f.metrics.Error(req, "remote_owner_open", err)
		f.report(r, req, err)
		status := statusFromRemoteOwnerOpenError(err)
		if status == http.StatusServiceUnavailable {
			w.Header().Set("Retry-After", "1")
		}
		http.Error(w, err.Error(), status)
		return true
	}

	socket, err := websocket.Accept(w, r, f.acceptOptions)
	if err != nil {
		f.closeRemoteOwnerSession(r, req, session, "accept_failed", false)
		f.closeQuota(r, req, quota)
		return true
	}
	socket.SetReadLimit(f.readLimitBytes)

	if err := f.serveRemoteForwardedSocket(r, req, socket, session, header, quota, true); err != nil {
		f.metrics.Error(req, "remote_owner_forward", err)
		f.report(r, req, err)
	}
	return true
}

func (f *remoteOwnerForwarder) handleLocalAuthorityLoss(
	r *http.Request,
	req Request,
	socket *websocket.Conn,
	previousEpoch uint64,
) error {
	transitionStart := time.Now()
	target, err := f.rebindRemoteOwnerSession(
		r.Context(),
		req,
		remoteOwnerSession{epoch: previousEpoch},
		cloneHeader(r.Header),
		false,
	)
	if err != nil {
		observeOwnershipTransition(f.metrics, req, ownershipStateLocal, ownershipStateClosed, time.Since(transitionStart), err)
		return err
	}
	observeOwnershipTransition(f.metrics, req, ownershipStateLocal, ownershipStateRemote, time.Since(transitionStart), nil)
	if handoffState := authorityLossHandoffStateFromRequest(r); handoffState != nil {
		session := target.session
		handoffState.upstreamPeer.switchSession(session.stream, session.epoch, session.ownerToEdgeFormat)
		closeReason := "client_closed"
		defer f.cleanup(r, req, socket, session, closeReason, false)
		return f.bridgeRemoteOwnerSessions(
			r,
			req,
			socket,
			&session,
			cloneHeader(r.Header),
			handoffState.clientErrCh,
			handoffState.upstreamPeer,
			handoffState.cancelClient,
			handoffState.quota,
			&closeReason,
		)
	}
	return f.serveRemoteForwardedSocket(r, req, socket, target.session, cloneHeader(r.Header), nil, false)
}

func (f *remoteOwnerForwarder) openQuota(r *http.Request, req Request) (QuotaLease, error) {
	if f == nil || f.local == nil {
		return nil, nil
	}
	return f.local.openQuota(r, req)
}

func (f *remoteOwnerForwarder) closeQuota(r *http.Request, req Request, quota QuotaLease) {
	if f == nil || f.local == nil {
		return
	}
	f.local.closeQuota(r, req, quota)
}

func (f *remoteOwnerForwarder) allowQuotaFrame(r *http.Request, req Request, quota QuotaLease, direction QuotaDirection, bytes int) error {
	if f == nil || f.local == nil {
		return nil
	}
	return f.local.allowQuotaFrame(r, req, quota, direction, bytes)
}

func (f *remoteOwnerForwarder) serveRemoteForwardedSocket(
	r *http.Request,
	req Request,
	socket *websocket.Conn,
	session remoteOwnerSession,
	header http.Header,
	quota QuotaLease,
	observeConnectionLifecycle bool,
) error {
	defer f.closeQuota(r, req, quota)
	if observeConnectionLifecycle {
		f.metrics.ConnectionOpened(req)
		defer f.metrics.ConnectionClosed(req)
	}

	clientCtx, cancelClient := context.WithCancel(r.Context())
	defer cancelClient()
	upstreamPeer := newSwitchableRemoteStreamPeer(req.DocumentKey, req.ConnectionID)
	upstreamPeer.switchSession(session.stream, session.epoch, session.ownerToEdgeFormat)
	clientErrCh := make(chan error, 1)
	go func() {
		clientErrCh <- f.pipeClientToRemote(clientCtx, req, socket, upstreamPeer, quota)
	}()

	closeReason := "client_closed"
	defer func() {
		f.cleanup(r, req, socket, session, closeReason, observeConnectionLifecycle)
	}()

	if err := f.bridgeRemoteOwnerSessions(
		r,
		req,
		socket,
		&session,
		header,
		clientErrCh,
		upstreamPeer,
		cancelClient,
		quota,
		&closeReason,
	); err != nil {
		return err
	}
	return nil
}

func (f *remoteOwnerForwarder) bridgeRemoteOwnerSessions(
	r *http.Request,
	req Request,
	socket *websocket.Conn,
	session *remoteOwnerSession,
	header http.Header,
	clientErrCh <-chan error,
	upstreamPeer *switchableRemoteStreamPeer,
	cancelClient context.CancelFunc,
	quota QuotaLease,
	closeReason *string,
) error {
	if session == nil {
		return errors.New("yhttp: sessao remota obrigatoria para bridge")
	}
	if closeReason == nil {
		return errors.New("yhttp: ponteiro de closeReason obrigatorio")
	}

	for {
		result := f.bridge(r, req, socket, *session, clientErrCh, quota)
		if result.retryableSignal == nil {
			*closeReason = result.closeReason
			return nil
		}

		retryableReason := result.retryableSignal.metricReason(retryableRemoteOwnerCloseReason)
		previous := *session
		upstreamPeer.clearSession()
		f.closeRemoteOwnerSession(r, req, previous, retryableReason, false)
		*session = remoteOwnerSession{}

		transitionStart := time.Now()
		target, rebindErr := f.rebindRemoteOwnerSession(r.Context(), req, previous, header, true)
		if rebindErr != nil {
			observeOwnershipTransition(f.metrics, req, ownershipStateRemote, ownershipStateClosed, time.Since(transitionStart), rebindErr)
			if closeErr := socket.Close(result.retryableSignal.websocketStatus(), result.retryableSignal.websocketReason(authorityLostCloseReason)); closeErr != nil && !isIgnorableTransportError(closeErr) {
				f.metrics.Error(req, "remote_owner_close_client", closeErr)
				f.report(r, req, closeErr)
			}
			cancelClient()
			*closeReason = retryableReason
			return nil
		}

		if target.localResolution != nil {
			observeOwnershipTransition(f.metrics, req, ownershipStateRemote, ownershipStateLocal, time.Since(transitionStart), nil)
			*closeReason = "client_closed"
			return f.takeoverLocalOwner(r, req, socket, header, clientErrCh, upstreamPeer, cancelClient, quota, closeReason)
		}

		observeOwnershipTransition(f.metrics, req, ownershipStateRemote, ownershipStateRemote, time.Since(transitionStart), nil)
		*session = target.session
		upstreamPeer.switchSession(session.stream, session.epoch, session.ownerToEdgeFormat)
	}
}

func (f *remoteOwnerForwarder) openRemoteOwnerSession(
	ctx context.Context,
	req Request,
	resolution ycluster.OwnerResolution,
	epoch uint64,
	header http.Header,
	bootstrap bool,
) (remoteOwnerSession, error) {
	stream, err := f.dialer.DialRemoteOwner(ctx, RemoteOwnerDialRequest{
		Request:    req,
		Resolution: resolution,
		Header:     cloneHeader(header),
	})
	if err != nil {
		return remoteOwnerSession{}, err
	}

	handshakeStart := time.Now()
	if err := f.sendHandshake(ctx, req, stream, epoch); err != nil {
		observeRemoteOwnerHandshake(f.metrics, req, remoteOwnerMetricsRoleEdge, time.Since(handshakeStart), err)
		f.closeStream(nil, req, stream)
		return remoteOwnerSession{}, err
	}
	observeRemoteOwnerMessage(f.metrics, req, remoteOwnerMetricsRoleEdge, remoteOwnerMetricsDirectionOut, "handshake")

	ownerToEdgeFormat, err := f.receiveHandshakeAck(ctx, req, resolution, stream, epoch)
	if err != nil {
		observeRemoteOwnerHandshake(f.metrics, req, remoteOwnerMetricsRoleEdge, time.Since(handshakeStart), err)
		f.closeStream(nil, req, stream)
		return remoteOwnerSession{}, err
	}
	observeRemoteOwnerHandshake(f.metrics, req, remoteOwnerMetricsRoleEdge, time.Since(handshakeStart), nil)
	observeRemoteOwnerConnectionOpened(f.metrics, req, remoteOwnerMetricsRoleEdge)

	session := remoteOwnerSession{
		stream:            stream,
		resolution:        resolution,
		epoch:             epoch,
		ownerToEdgeFormat: ownerToEdgeFormat,
	}
	if !bootstrap {
		return session, nil
	}

	if err := f.sendBootstrapRequests(ctx, req, stream, epoch, ownerToEdgeFormat); err != nil {
		f.closeRemoteOwnerSession(nil, req, session, "rebind_bootstrap_error", false)
		return remoteOwnerSession{}, err
	}
	return session, nil
}

func (f *remoteOwnerForwarder) rebindRemoteOwnerSession(
	ctx context.Context,
	req Request,
	previous remoteOwnerSession,
	header http.Header,
	allowLocal bool,
) (*remoteOwnerRebindTarget, error) {
	if f.ownerLookup == nil {
		return nil, errors.New("yhttp: owner lookup obrigatorio para rebind remoto")
	}

	lookupCtx, cancel := context.WithTimeout(ctx, f.rebindTimeout)
	defer cancel()

	var lastErr error
	for {
		resolution, err := f.lookupNextRemoteOwner(lookupCtx, req, previous, allowLocal)
		if err == nil {
			if resolution.Local {
				return &remoteOwnerRebindTarget{
					localResolution: resolution,
				}, nil
			}
			epoch, epochErr := remoteOwnerEpoch(*resolution)
			if epochErr != nil {
				lastErr = epochErr
			} else {
				session, openErr := f.openRemoteOwnerSession(lookupCtx, req, *resolution, epoch, header, true)
				if openErr == nil {
					return &remoteOwnerRebindTarget{
						session: session,
					}, nil
				}
				lastErr = openErr
			}
		} else {
			lastErr = err
		}

		select {
		case <-lookupCtx.Done():
			if lastErr == nil {
				return nil, lookupCtx.Err()
			}
			return nil, lastErr
		case <-time.After(f.rebindInterval):
		}
	}
}

func (f *remoteOwnerForwarder) lookupNextRemoteOwner(
	ctx context.Context,
	req Request,
	previous remoteOwnerSession,
	allowLocal bool,
) (*ycluster.OwnerResolution, error) {
	lookupStart := time.Now()
	resolution, err := f.ownerLookup.LookupOwner(ctx, ycluster.OwnerLookupRequest{
		DocumentKey: req.DocumentKey,
	})
	lookupDuration := time.Since(lookupStart)
	if err != nil {
		observeOwnerLookup(f.metrics, req, lookupDuration, ownerLookupResultFromLookupError(err))
		return nil, err
	}
	if resolution == nil {
		observeOwnerLookup(f.metrics, req, lookupDuration, ownerLookupResultNotFound)
		return nil, ycluster.ErrOwnerNotFound
	}
	observeOwnerLookup(f.metrics, req, lookupDuration, ownerLookupResultFromResolution(*resolution))
	currentEpoch := uint64(0)
	if resolution.Placement.Lease != nil {
		currentEpoch = resolution.Placement.Lease.Epoch
	}
	if currentEpoch <= previous.epoch {
		return nil, fmt.Errorf("%w: epoch remoto ainda nao avancou (got=%d want>%d)", ycluster.ErrLeaseExpired, currentEpoch, previous.epoch)
	}
	if resolution.Local && !allowLocal {
		return nil, fmt.Errorf("%w: owner ainda local durante handoff (epoch=%d)", ycluster.ErrLeaseExpired, currentEpoch)
	}
	return resolution, nil
}

func (f *remoteOwnerForwarder) takeoverLocalOwner(
	r *http.Request,
	req Request,
	socket *websocket.Conn,
	header http.Header,
	clientErrCh <-chan error,
	upstreamPeer *switchableRemoteStreamPeer,
	cancelClient context.CancelFunc,
	quota QuotaLease,
	closeReason *string,
) error {
	if f.local == nil {
		return errors.New("yhttp: server local obrigatorio para takeover")
	}

	ownership, err := f.local.acquireRequestOwnership(r.Context(), req)
	if err != nil {
		return err
	}
	connection, err := f.local.provider.Open(r.Context(), req.DocumentKey, req.ConnectionID, req.ClientID)
	if err != nil {
		f.local.releaseRequestOwnership(r, req, ownership)
		return err
	}

	peer := f.local.socketPeer(socket, req, quota)
	f.local.registry.add(req.DocumentKey, req.ConnectionID, peer)

	sessionCtx, cancelSession := context.WithCancel(r.Context())
	defer cancelSession()
	revalidateCh := f.local.startAuthorityRevalidator(sessionCtx, req, connection, cancelSession, nil)
	defer drainRemoteOwnerCloseSignal(revalidateCh)
	notifyAuthorityLoss := func(signal remoteOwnerCloseSignal) {
		signalRemoteOwnerClose(revalidateCh, signal)
		cancelSession()
	}

	if err := f.local.bootstrapConnection(r, req, connection, peer, true); err != nil {
		f.local.cleanupConnectionWithOwnership(r, req, connection, ownership)
		return err
	}
	upstreamPeer.switchTarget(&localConnectionPeer{
		server:          f.local,
		req:             req,
		connection:      connection,
		peer:            peer,
		quota:           quota,
		onAuthorityLoss: notifyAuthorityLoss,
	})

	for {
		select {
		case err := <-clientErrCh:
			f.local.cleanupConnectionWithOwnership(r, req, connection, ownership)
			if signal, ok := drainRemoteOwnerCloseSignal(revalidateCh); ok {
				previous := remoteOwnerSession{epoch: connection.AuthorityEpoch()}
				upstreamPeer.clearSession()
				transitionStart := time.Now()
				target, rebindErr := f.rebindRemoteOwnerSession(r.Context(), req, previous, header, false)
				if rebindErr != nil {
					observeOwnershipTransition(f.metrics, req, ownershipStateLocal, ownershipStateClosed, time.Since(transitionStart), rebindErr)
					if closeReason != nil {
						*closeReason = signal.metricReason(authorityLostCloseReason)
					}
					if closeErr := socket.Close(signal.websocketStatus(), signal.websocketReason(authorityLostCloseReason)); closeErr != nil && !isIgnorableTransportError(closeErr) {
						f.metrics.Error(req, "remote_owner_close_client", closeErr)
						f.report(r, req, closeErr)
					}
					cancelClient()
					return nil
				}

				observeOwnershipTransition(f.metrics, req, ownershipStateLocal, ownershipStateRemote, time.Since(transitionStart), nil)
				session := target.session
				upstreamPeer.switchSession(session.stream, session.epoch, session.ownerToEdgeFormat)
				return f.bridgeRemoteOwnerSessions(
					r,
					req,
					socket,
					&session,
					header,
					clientErrCh,
					upstreamPeer,
					cancelClient,
					quota,
					closeReason,
				)
			}
			return err
		case <-sessionCtx.Done():
			f.local.cleanupConnectionWithOwnership(r, req, connection, ownership)
			if signal, ok := drainRemoteOwnerCloseSignal(revalidateCh); ok {
				previous := remoteOwnerSession{epoch: connection.AuthorityEpoch()}
				upstreamPeer.clearSession()
				transitionStart := time.Now()
				target, rebindErr := f.rebindRemoteOwnerSession(r.Context(), req, previous, header, false)
				if rebindErr != nil {
					observeOwnershipTransition(f.metrics, req, ownershipStateLocal, ownershipStateClosed, time.Since(transitionStart), rebindErr)
					if closeReason != nil {
						*closeReason = signal.metricReason(authorityLostCloseReason)
					}
					if closeErr := socket.Close(signal.websocketStatus(), signal.websocketReason(authorityLostCloseReason)); closeErr != nil && !isIgnorableTransportError(closeErr) {
						f.metrics.Error(req, "remote_owner_close_client", closeErr)
						f.report(r, req, closeErr)
					}
					cancelClient()
					return nil
				}

				observeOwnershipTransition(f.metrics, req, ownershipStateLocal, ownershipStateRemote, time.Since(transitionStart), nil)
				session := target.session
				upstreamPeer.switchSession(session.stream, session.epoch, session.ownerToEdgeFormat)
				return f.bridgeRemoteOwnerSessions(
					r,
					req,
					socket,
					&session,
					header,
					clientErrCh,
					upstreamPeer,
					cancelClient,
					quota,
					closeReason,
				)
			}
			return nil
		}
	}
}

func (f *remoteOwnerForwarder) bridge(
	r *http.Request,
	req Request,
	socket *websocket.Conn,
	session remoteOwnerSession,
	clientErrCh <-chan error,
	quota QuotaLease,
) remoteOwnerBridgeResult {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	remoteErrCh := make(chan error, 1)
	go func() {
		remoteErrCh <- f.pipeRemoteToClient(ctx, req, session.resolution, socket, session.stream, session.epoch, session.ownerToEdgeFormat, quota)
	}()

	for {
		select {
		case err := <-clientErrCh:
			cancel()
			if err == nil {
				return remoteOwnerBridgeResult{closeReason: "client_closed"}
			}
			return remoteOwnerBridgeResult{closeReason: "bridge_error"}
		case err := <-remoteErrCh:
			cancel()
			if err == nil {
				return remoteOwnerBridgeResult{closeReason: "bridge_error"}
			}

			closeReason := "bridge_error"
			closeStatus := websocket.StatusGoingAway
			closeMessage := "falha ao encaminhar para owner remoto"
			var remoteCloseErr *remoteOwnerClosedError
			if errors.As(err, &remoteCloseErr) {
				closeReason = remoteCloseErr.signal.metricReason("remote_close")
				if remoteCloseErr.signal.retryable {
					return remoteOwnerBridgeResult{
						closeReason:     closeReason,
						retryableSignal: &remoteCloseErr.signal,
					}
				}
				closeStatus = remoteCloseErr.signal.websocketStatus()
				closeMessage = remoteCloseErr.signal.websocketReason("owner remoto encerrou a conexao")
			}
			if closeErr := socket.Close(closeStatus, closeMessage); closeErr != nil && !isIgnorableTransportError(closeErr) {
				f.metrics.Error(req, "remote_owner_close_client", closeErr)
				f.report(r, req, closeErr)
			}
			return remoteOwnerBridgeResult{closeReason: closeReason}
		}
	}
}

func (f *remoteOwnerForwarder) pipeClientToRemote(ctx context.Context, req Request, socket *websocket.Conn, peer *switchableRemoteStreamPeer, quota QuotaLease) error {
	for {
		msgType, payload, err := socket.Read(ctx)
		if err != nil {
			if status := websocket.CloseStatus(err); !isExpectedClientCloseStatus(status) && !isIgnorableTransportError(err) {
				f.metrics.Error(req, "remote_owner_read_client", err)
				f.report(nil, req, err)
				return err
			}
			return nil
		}

		f.metrics.FrameRead(req, len(payload))
		if msgType != websocket.MessageBinary {
			if closeErr := socket.Close(websocket.StatusUnsupportedData, "yjs-crdt-golang-server aceita apenas frames binarios"); closeErr != nil && !isIgnorableTransportError(closeErr) {
				f.metrics.Error(req, "remote_owner_reject_non_binary", closeErr)
				f.report(nil, req, closeErr)
			}
			return nil
		}

		if err := f.allowQuotaFrame(nil, req, quota, QuotaDirectionInbound, len(payload)); err != nil {
			if closeErr := socket.Close(quotaCloseStatus(err), quotaCloseReason(err)); closeErr != nil && !isIgnorableTransportError(closeErr) {
				f.metrics.Error(req, "remote_owner_close_quota", closeErr)
				f.report(nil, req, closeErr)
			}
			return err
		}

		writeCtx, cancel := context.WithTimeout(ctx, f.writeTimeout)
		err = peer.deliver(writeCtx, payload)
		cancel()
		if err != nil {
			if isIgnorableRemoteOwnerStreamError(err) {
				return nil
			}
			f.metrics.Error(req, "remote_owner_write_upstream", err)
			f.report(nil, req, err)
			return err
		}
		if kinds, metricErr := protocolPayloadMetricKindsForOwner(payload); metricErr == nil {
			for _, kind := range kinds {
				observeRemoteOwnerMessage(f.metrics, req, remoteOwnerMetricsRoleEdge, remoteOwnerMetricsDirectionOut, kind)
			}
		}
		f.metrics.FrameWritten(req, "remote_owner_upstream", len(payload))
	}
}

func (f *remoteOwnerForwarder) pipeRemoteToClient(
	ctx context.Context,
	req Request,
	resolution ycluster.OwnerResolution,
	socket *websocket.Conn,
	stream NodeMessageStream,
	epoch uint64,
	ownerToEdgeFormat yjsbridge.UpdateFormat,
	quota QuotaLease,
) error {
	for {
		message, err := stream.Receive(ctx)
		if err != nil {
			if isIgnorableRemoteOwnerStreamError(err) {
				return nil
			}
			f.metrics.Error(req, "remote_owner_read_upstream", err)
			f.report(nil, req, err)
			return err
		}
		if err := validateRemoteOwnerUpstreamMessage(req, resolution, epoch, ownerToEdgeFormat, message); err != nil {
			f.metrics.Error(req, "remote_owner_validate_upstream", err)
			f.report(nil, req, err)
			return err
		}
		observeRemoteOwnerMessage(f.metrics, req, remoteOwnerMetricsRoleEdge, remoteOwnerMetricsDirectionIn, nodeMessageMetricKind(message))

		payload, closeMessage, err := remoteMessageToProtocolPayload(message)
		if err != nil {
			f.metrics.Error(req, "remote_owner_decode_upstream", err)
			f.report(nil, req, err)
			return err
		}
		if closeMessage != nil {
			return &remoteOwnerClosedError{signal: remoteOwnerCloseSignalFromMessage(closeMessage)}
		}
		if len(payload) == 0 {
			continue
		}
		if ownerToEdgeFormat != yjsbridge.UpdateFormatV2 {
			payload, err = protocolPayloadForSyncOutputFormat(payload, req.SyncOutputFormat)
			if err != nil {
				f.metrics.Error(req, "remote_owner_encode_downstream", err)
				f.report(nil, req, err)
				return err
			}
		}
		if err := f.allowQuotaFrame(nil, req, quota, QuotaDirectionOutbound, len(payload)); err != nil {
			if closeErr := socket.Close(quotaCloseStatus(err), quotaCloseReason(err)); closeErr != nil && !isIgnorableTransportError(closeErr) {
				f.metrics.Error(req, "remote_owner_close_quota", closeErr)
				f.report(nil, req, closeErr)
			}
			return err
		}

		writeCtx, cancel := context.WithTimeout(ctx, f.writeTimeout)
		err = socket.Write(writeCtx, websocket.MessageBinary, payload)
		cancel()
		if err != nil {
			if isIgnorableTransportError(err) {
				return nil
			}
			f.metrics.Error(req, "remote_owner_write_client", err)
			f.report(nil, req, err)
			return err
		}
		f.metrics.FrameWritten(req, "remote_owner_downstream", len(payload))
	}
}

func (f *remoteOwnerForwarder) cleanup(
	r *http.Request,
	req Request,
	socket *websocket.Conn,
	session remoteOwnerSession,
	closeReason string,
	observeConnectionLifecycle bool,
) {
	f.closeRemoteOwnerSession(r, req, session, closeReason, closeReason == "client_closed")
	if observeConnectionLifecycle {
		defer f.metrics.ConnectionClosed(req)
	}
	if closeReason != "client_closed" {
		return
	}
	if err := socket.CloseNow(); err != nil && !isIgnorableTransportError(err) {
		f.metrics.Error(req, "remote_owner_close_socket", err)
		f.report(r, req, err)
	}
}

func (f *remoteOwnerForwarder) sendHandshake(ctx context.Context, req Request, stream NodeMessageStream, epoch uint64) error {
	flags := ynodeproto.FlagNone
	if req.PersistOnClose {
		flags |= ynodeproto.FlagPersistOnClose
	}
	if requestWantsInterNodeV2(req) {
		flags |= ynodeproto.FlagSupportsUpdateV2
	}
	return stream.Send(ctx, &ynodeproto.Handshake{
		Flags:        flags,
		NodeID:       f.localNodeID.String(),
		DocumentKey:  req.DocumentKey,
		ConnectionID: req.ConnectionID,
		ClientID:     req.ClientID,
		Epoch:        epoch,
	})
}

func (f *remoteOwnerForwarder) receiveHandshakeAck(
	ctx context.Context,
	req Request,
	resolution ycluster.OwnerResolution,
	stream NodeMessageStream,
	epoch uint64,
) (yjsbridge.UpdateFormat, error) {
	readCtx, cancel := context.WithTimeout(ctx, f.writeTimeout)
	defer cancel()

	message, err := stream.Receive(readCtx)
	if err != nil {
		return yjsbridge.UpdateFormatV1, err
	}

	handshakeAck, ok := message.(*ynodeproto.HandshakeAck)
	if !ok {
		if closeMessage, closeOK := message.(*ynodeproto.Close); closeOK {
			if err := validateRemoteOwnerRouteFields(req, epoch, closeMessage.DocumentKey, closeMessage.ConnectionID, closeMessage.Epoch); err != nil {
				return yjsbridge.UpdateFormatV1, err
			}
			observeRemoteOwnerMessage(f.metrics, req, remoteOwnerMetricsRoleEdge, remoteOwnerMetricsDirectionIn, nodeMessageMetricKind(closeMessage))
			return yjsbridge.UpdateFormatV1, &remoteOwnerClosedError{signal: remoteOwnerCloseSignalFromMessage(closeMessage)}
		}
		return yjsbridge.UpdateFormatV1, fmt.Errorf("yhttp: handshake ack inicial obrigatorio, recebido %T", message)
	}
	if err := handshakeAck.Validate(); err != nil {
		return yjsbridge.UpdateFormatV1, err
	}
	if err := validateRemoteOwnerHandshakeAck(req, resolution, epoch, handshakeAck); err != nil {
		return yjsbridge.UpdateFormatV1, err
	}
	observeRemoteOwnerMessage(f.metrics, req, remoteOwnerMetricsRoleEdge, remoteOwnerMetricsDirectionIn, nodeMessageMetricKind(handshakeAck))
	return ownerToEdgeFormatFromAck(req, handshakeAck), nil
}

func statusFromRemoteOwnerOpenError(err error) int {
	var closeErr *remoteOwnerClosedError
	if errors.As(err, &closeErr) && closeErr.signal.retryable {
		return http.StatusServiceUnavailable
	}
	return remoteOwnerDialErrorStatusCode
}

func (f *remoteOwnerForwarder) sendBootstrapRequests(ctx context.Context, req Request, stream NodeMessageStream, epoch uint64, format yjsbridge.UpdateFormat) error {
	stateVector, err := yjsbridge.EncodeStateVectorFromUpdates(nil)
	if err != nil {
		return err
	}

	var syncRequest ynodeproto.Message
	if format == yjsbridge.UpdateFormatV2 {
		syncRequest = &ynodeproto.DocumentSyncRequestV2{
			DocumentKey:  req.DocumentKey,
			ConnectionID: req.ConnectionID,
			Epoch:        epoch,
			StateVector:  stateVector,
		}
	} else {
		syncRequest = &ynodeproto.DocumentSyncRequest{
			DocumentKey:  req.DocumentKey,
			ConnectionID: req.ConnectionID,
			Epoch:        epoch,
			StateVector:  stateVector,
		}
	}

	messages := []ynodeproto.Message{
		syncRequest,
		&ynodeproto.QueryAwarenessRequest{
			DocumentKey:  req.DocumentKey,
			ConnectionID: req.ConnectionID,
			Epoch:        epoch,
		},
	}
	for _, message := range messages {
		writeCtx, cancel := context.WithTimeout(ctx, f.writeTimeout)
		sendErr := stream.Send(writeCtx, message)
		cancel()
		if sendErr != nil {
			return sendErr
		}
		observeRemoteOwnerMessage(f.metrics, req, remoteOwnerMetricsRoleEdge, remoteOwnerMetricsDirectionOut, nodeMessageMetricKind(message))
	}
	return nil
}

func (f *remoteOwnerForwarder) sendDisconnect(req Request, stream NodeMessageStream, epoch uint64) {
	if stream == nil || epoch == 0 || req.DocumentKey.DocumentID == "" || strings.TrimSpace(req.ConnectionID) == "" {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), f.writeTimeout)
	defer cancel()
	if err := stream.Send(ctx, &ynodeproto.Disconnect{
		DocumentKey:  req.DocumentKey,
		ConnectionID: req.ConnectionID,
		Epoch:        epoch,
	}); err != nil && !isIgnorableRemoteOwnerStreamError(err) {
		f.metrics.Error(req, "remote_owner_disconnect", err)
		f.report(nil, req, err)
	} else if err == nil {
		observeRemoteOwnerMessage(f.metrics, req, remoteOwnerMetricsRoleEdge, remoteOwnerMetricsDirectionOut, "disconnect")
	}
}

func (f *remoteOwnerForwarder) closeStream(r *http.Request, req Request, stream NodeMessageStream) {
	if stream == nil {
		return
	}
	if err := stream.Close(); err != nil && !isIgnorableRemoteOwnerStreamError(err) {
		f.metrics.Error(req, "remote_owner_close_stream", err)
		f.report(r, req, err)
	}
}

func (f *remoteOwnerForwarder) closeRemoteOwnerSession(
	r *http.Request,
	req Request,
	session remoteOwnerSession,
	closeReason string,
	sendDisconnect bool,
) {
	if session.stream == nil {
		return
	}
	if sendDisconnect {
		f.sendDisconnect(req, session.stream, session.epoch)
	}
	f.closeStream(r, req, session.stream)
	observeRemoteOwnerConnectionClosed(f.metrics, req, remoteOwnerMetricsRoleEdge)
	observeRemoteOwnerClose(f.metrics, req, remoteOwnerMetricsRoleEdge, closeReason)
}

func newSwitchableRemoteStreamPeer(key storage.DocumentKey, connectionID string) *switchableRemoteStreamPeer {
	return &switchableRemoteStreamPeer{
		documentKey:  key,
		connectionID: connectionID,
		readyCh:      make(chan struct{}),
	}
}

func (p *switchableRemoteStreamPeer) switchSession(stream NodeMessageStream, epoch uint64, format yjsbridge.UpdateFormat) {
	p.switchTarget(&remoteOwnerUpstreamPeer{
		stream:           stream,
		documentKey:      p.documentKey,
		connectionID:     p.connectionID,
		epoch:            epoch,
		syncOutputFormat: format,
	})
}

func (p *switchableRemoteStreamPeer) switchTarget(target forwardDeliveryTarget) {
	if p == nil {
		return
	}

	p.mu.Lock()
	readyCh := p.readyCh
	p.target = target
	p.readyCh = nil
	p.mu.Unlock()

	if readyCh != nil {
		close(readyCh)
	}
}

func (p *switchableRemoteStreamPeer) clearSession() {
	if p == nil {
		return
	}

	p.mu.Lock()
	p.target = nil
	p.readyCh = make(chan struct{})
	p.mu.Unlock()
}

func (p *switchableRemoteStreamPeer) deliver(ctx context.Context, payload []byte) error {
	if p == nil {
		return errors.New("yhttp: peer remoto ausente")
	}

	for {
		p.mu.RLock()
		target := p.target
		readyCh := p.readyCh
		p.mu.RUnlock()

		if target != nil {
			return target.deliver(ctx, payload)
		}

		select {
		case <-readyCh:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

type localConnectionPeer struct {
	server          *Server
	req             Request
	connection      *yprotocol.Connection
	peer            roomPeer
	quota           QuotaLease
	onAuthorityLoss func(remoteOwnerCloseSignal)
}

func (p *localConnectionPeer) deliver(ctx context.Context, payload []byte) error {
	if p == nil || p.server == nil || p.connection == nil || p.peer == nil {
		return errors.New("yhttp: peer local ausente")
	}

	handleStart := time.Now()
	result, err := p.connection.HandleEncodedMessagesContext(ctx, payload)
	p.server.metrics.Handle(p.req, time.Since(handleStart), err)
	if err != nil {
		if isAuthorityLostRetryableError(err) && p.onAuthorityLoss != nil {
			p.onAuthorityLoss(remoteOwnerCloseSignalFromError(err, "handle"))
		}
		p.server.metrics.Error(p.req, "handle", err)
		p.server.report(nil, p.req, err)
		return err
	}

	if len(result.Direct) > 0 {
		if err := p.server.writeBinary(p.peer, result.Direct); err != nil {
			if !isIgnorableTransportError(err) {
				p.server.metrics.Error(p.req, "write_direct", err)
				p.server.report(nil, p.req, err)
			}
			return err
		}
		p.server.metrics.FrameWritten(p.req, "direct", len(result.Direct))
	}
	if len(result.Broadcast) > 0 {
		p.server.fanout(nil, p.req, result.Broadcast)
	}
	return nil
}

func (f *remoteOwnerForwarder) report(r *http.Request, req Request, err error) {
	if f.onError != nil && err != nil {
		f.onError(redactHTTPRequest(f.redactor, r), redactRequest(f.redactor, req), err)
	}
}

func authorityLossHandoffStateFromRequest(r *http.Request) *authorityLossHandoffState {
	if r == nil {
		return nil
	}
	state, _ := r.Context().Value(authorityLossHandoffContextKey{}).(*authorityLossHandoffState)
	return state
}

func cloneHeader(src http.Header) http.Header {
	if src == nil {
		return nil
	}

	cloned := make(http.Header, len(src))
	for key, values := range src {
		cloned[key] = append([]string(nil), values...)
	}
	return cloned
}

func isWebSocketUpgrade(r *http.Request) bool {
	if r == nil {
		return false
	}
	if !headerContainsToken(r.Header.Values("Connection"), "upgrade") {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(r.Header.Get("Upgrade")), "websocket")
}

func headerContainsToken(values []string, want string) bool {
	for _, value := range values {
		for _, token := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(token), want) {
				return true
			}
		}
	}
	return false
}

func isIgnorableRemoteOwnerStreamError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, io.EOF)
}

func validateRemoteOwnerUpstreamMessage(
	req Request,
	resolution ycluster.OwnerResolution,
	epoch uint64,
	ownerToEdgeFormat yjsbridge.UpdateFormat,
	message ynodeproto.Message,
) error {
	switch message := message.(type) {
	case *ynodeproto.HandshakeAck:
		return validateRemoteOwnerHandshakeAck(req, resolution, epoch, message)
	case *ynodeproto.DocumentSyncResponse:
		return validateRemoteOwnerRouteFields(req, epoch, message.DocumentKey, message.ConnectionID, message.Epoch)
	case *ynodeproto.DocumentUpdate:
		return validateRemoteOwnerRouteFields(req, epoch, message.DocumentKey, message.ConnectionID, message.Epoch)
	case *ynodeproto.DocumentSyncResponseV2:
		if ownerToEdgeFormat != yjsbridge.UpdateFormatV2 {
			return fmt.Errorf("yhttp: owner remoto enviou V2 sem negociacao inter-node")
		}
		return validateRemoteOwnerRouteFields(req, epoch, message.DocumentKey, message.ConnectionID, message.Epoch)
	case *ynodeproto.DocumentUpdateV2:
		if ownerToEdgeFormat != yjsbridge.UpdateFormatV2 {
			return fmt.Errorf("yhttp: owner remoto enviou V2 sem negociacao inter-node")
		}
		return validateRemoteOwnerRouteFields(req, epoch, message.DocumentKey, message.ConnectionID, message.Epoch)
	case *ynodeproto.AwarenessUpdate:
		return validateRemoteOwnerRouteFields(req, epoch, message.DocumentKey, message.ConnectionID, message.Epoch)
	case *ynodeproto.QueryAwarenessResponse:
		return validateRemoteOwnerRouteFields(req, epoch, message.DocumentKey, message.ConnectionID, message.Epoch)
	case *ynodeproto.Close:
		return validateRemoteOwnerRouteFields(req, epoch, message.DocumentKey, message.ConnectionID, message.Epoch)
	case *ynodeproto.Ping, *ynodeproto.Pong:
		return nil
	default:
		return fmt.Errorf("yhttp: message type remoto nao suportado pelo edge relay: %T", message)
	}
}

func validateRemoteOwnerHandshakeAck(
	req Request,
	resolution ycluster.OwnerResolution,
	epoch uint64,
	message *ynodeproto.HandshakeAck,
) error {
	if message == nil {
		return errors.New("yhttp: handshake ack ausente")
	}
	if err := validateRemoteOwnerRouteFields(req, epoch, message.DocumentKey, message.ConnectionID, message.Epoch); err != nil {
		return err
	}
	if strings.TrimSpace(message.NodeID) != resolution.Placement.NodeID.String() {
		return fmt.Errorf("yhttp: handshake ack route mismatch: node id got %q want %q", message.NodeID, resolution.Placement.NodeID)
	}
	if message.ClientID != req.ClientID {
		return fmt.Errorf("yhttp: handshake ack route mismatch: client id got %d want %d", message.ClientID, req.ClientID)
	}
	return nil
}

func remoteOwnerEpoch(resolution ycluster.OwnerResolution) (uint64, error) {
	if resolution.Placement.Lease == nil || resolution.Placement.Lease.Epoch == 0 {
		return 0, fmt.Errorf("%w: owner remoto sem epoch ativo", ycluster.ErrInvalidLease)
	}
	return resolution.Placement.Lease.Epoch, nil
}
