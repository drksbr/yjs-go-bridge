package yprotocol

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/drksbr/yjs-crdt-golang-server/pkg/storage"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/yawareness"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/yjsbridge"
)

var (
	// ErrInvalidConnectionID sinaliza tentativa de abrir conexão sem identificador.
	ErrInvalidConnectionID = errors.New("yprotocol: connection id invalido")
	// ErrConnectionClosed sinaliza uso de conexão já encerrada.
	ErrConnectionClosed = errors.New("yprotocol: connection fechada")
	// ErrConnectionExists sinaliza duplicidade de connectionID dentro do mesmo documento.
	ErrConnectionExists = errors.New("yprotocol: connection ja existe para o documento")
	// ErrClientIDExists sinaliza duplicidade de localClientID dentro do mesmo documento.
	ErrClientIDExists = errors.New("yprotocol: client id ja existe para o documento")
	// ErrPersistenceDisabled sinaliza ausência de SnapshotStore no provider.
	ErrPersistenceDisabled = errors.New("yprotocol: persistencia desabilitada")
	// ErrAuthorityLost sinaliza que o owner local perdeu a autoridade sobre o documento.
	ErrAuthorityLost = errors.New("yprotocol: autoridade perdida para o documento")
	// ErrAuthorityFenceUnsupported sinaliza wiring inconsistente entre resolver e store.
	ErrAuthorityFenceUnsupported = errors.New("yprotocol: store nao suporta fencing autoritativo")
)

// ResolveAuthorityFenceFunc resolve o fence autoritativo atual do owner local
// para um documento antes das operações de escrita/persistência.
type ResolveAuthorityFenceFunc func(ctx context.Context, key storage.DocumentKey) (*storage.AuthorityFence, error)

// ProviderConfig define dependências opcionais do provider local.
type ProviderConfig struct {
	// Store permite hidratação e persistência explícita de snapshots do documento.
	//
	// Quando o store também implementa `storage.UpdateLogStore`, o provider
	// recupera `snapshot + tail` em `Open`, registra updates incrementais no log
	// e compacta esse estado em `Persist`.
	Store storage.SnapshotStore

	// ResolveAuthorityFence ativa fencing autoritativo opcional para runtimes
	// distribuídos, exigindo que o store suporte os contratos autoritativos.
	ResolveAuthorityFence ResolveAuthorityFenceFunc

	// Metrics adiciona observabilidade opcional ao lifecycle local do provider.
	Metrics Metrics

	// StorageMetrics propaga observabilidade opcional aos helpers agregados de
	// replay/recovery/compaction usados pelo provider.
	StorageMetrics storage.Metrics
}

// DispatchResult representa a saída local de uma operação no provider.
//
// `Direct` é enviado apenas para a conexão chamadora.
// `Broadcast` pode ser reenviado para os demais peers do mesmo documento.
type DispatchResult struct {
	Direct    []byte
	Broadcast []byte
}

// ConnectionHandleOptions controla opções explícitas de saída para uma conexão
// do provider sem alterar o estado interno V2-canônico do room.
type ConnectionHandleOptions struct {
	// DirectSyncOutputFormat define o formato de respostas diretas SyncStep2.
	//
	// Zero value e UpdateFormatV1 preservam o contrato V1-first atual. Use
	// UpdateFormatV2 apenas quando o caller já negociou suporte V2 com o peer.
	DirectSyncOutputFormat yjsbridge.UpdateFormat

	// BroadcastSyncOutputFormat define o formato de broadcasts SyncStep2/Update.
	//
	// Storage e replay continuam V1-compatible enquanto o room mantém V2 em memória.
	BroadcastSyncOutputFormat yjsbridge.UpdateFormat
}

// Provider compõe múltiplas `Session` em torno do mesmo documento para um
// runtime single-process mínimo.
//
// O provider:
// - carrega o snapshot inicial do documento em `Open`;
// - mantém o update V2 autoritativo do room em memória;
// - deriva V1 apenas para compatibilidade, storage e update log existentes;
// - guarda awareness local por conexão e agrega snapshots por room;
// - deixa transporte, fanout de rede e persistência automática fora de escopo.
type Provider struct {
	mu                    sync.Mutex
	store                 storage.SnapshotStore
	resolveAuthorityFence ResolveAuthorityFenceFunc
	metrics               Metrics
	storageMetrics        storage.Metrics
	rooms                 map[storage.DocumentKey]*providerRoom
	roomLoads             singleflight.Group
}

type providerRoom struct {
	mu            sync.Mutex
	key           storage.DocumentKey
	snapshot      *yjsbridge.PersistedSnapshot
	updateV2      []byte
	lastOffset    storage.UpdateOffset
	compactedAt   storage.UpdateOffset
	authority     *storage.AuthorityFence
	authorityLost bool
	connections   map[string]*Connection
}

// Connection representa uma conexão local anexada a um documento do provider.
type Connection struct {
	provider *Provider
	room     *providerRoom
	id       string
	clientID uint32
	session  *Session
	closed   bool
}

// NewProvider cria um provider local com store opcional.
func NewProvider(cfg ProviderConfig) *Provider {
	return &Provider{
		store:                 cfg.Store,
		resolveAuthorityFence: cfg.ResolveAuthorityFence,
		metrics:               normalizeMetrics(cfg.Metrics),
		storageMetrics:        cfg.StorageMetrics,
		rooms:                 make(map[storage.DocumentKey]*providerRoom),
	}
}

// Open cria ou reutiliza o room do documento e anexa uma conexão local.
func (p *Provider) Open(ctx context.Context, key storage.DocumentKey, connectionID string, localClientID uint32) (*Connection, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if strings.TrimSpace(connectionID) == "" {
		return nil, ErrInvalidConnectionID
	}
	if err := key.Validate(); err != nil {
		return nil, err
	}

	room, err := p.ensureRoom(ctx, key)
	if err != nil {
		return nil, err
	}

	room.mu.Lock()
	defer room.mu.Unlock()

	if room.authorityLost {
		return nil, ErrAuthorityLost
	}
	if _, exists := room.connections[connectionID]; exists {
		return nil, ErrConnectionExists
	}
	for _, existing := range room.connections {
		if existing.closed {
			continue
		}
		if existing.clientID == localClientID {
			return nil, ErrClientIDExists
		}
	}

	connection := &Connection{
		provider: p,
		room:     room,
		id:       connectionID,
		clientID: localClientID,
		session:  NewSession(localClientID),
	}

	room.connections[connectionID] = connection
	return connection, nil
}

// ID retorna o identificador estável da conexão no room.
func (c *Connection) ID() string {
	if c == nil {
		return ""
	}
	return c.id
}

// ClientID retorna o clientID awareness da conexão.
func (c *Connection) ClientID() uint32 {
	if c == nil {
		return 0
	}
	return c.clientID
}

// AuthorityLost informa se o room desta conexão já perdeu a autoridade local.
func (c *Connection) AuthorityLost() bool {
	if c == nil || c.room == nil {
		return false
	}

	c.room.mu.Lock()
	defer c.room.mu.Unlock()
	return c.room.authorityLost
}

// AuthorityEpoch retorna o epoch autoritativo atualmente anexado ao room.
//
// Quando o provider nao opera com fencing autoritativo, retorna zero.
func (c *Connection) AuthorityEpoch() uint64 {
	if c == nil || c.room == nil {
		return 0
	}

	c.room.mu.Lock()
	defer c.room.mu.Unlock()
	if c.room.authority == nil {
		return 0
	}
	return c.room.authority.Owner.Epoch
}

// RevalidateAuthority força uma nova checagem do fence autoritativo do room.
//
// Quando não há fencing configurado, a operação é no-op.
func (c *Connection) RevalidateAuthority(ctx context.Context) (err error) {
	if c == nil {
		return ErrConnectionClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}

	room := c.room
	if room == nil {
		return ErrConnectionClosed
	}

	room.mu.Lock()
	if c.closed {
		room.mu.Unlock()
		return ErrConnectionClosed
	}
	if room.authorityLost {
		room.mu.Unlock()
		return ErrAuthorityLost
	}
	provider := c.provider
	key := room.key
	current := room.authority.Clone()
	room.mu.Unlock()
	start := time.Now()
	defer func() {
		if provider != nil {
			observeAuthorityRevalidation(provider.metrics, key, time.Since(start), err)
		}
	}()

	if provider == nil || provider.resolveAuthorityFence == nil || current == nil {
		return nil
	}

	resolved, err := provider.resolveRoomAuthority(ctx, key)
	if err != nil {
		if errors.Is(err, ErrAuthorityLost) || errors.Is(err, storage.ErrAuthorityLost) {
			room.mu.Lock()
			room.authorityLost = true
			room.mu.Unlock()
			observeAuthorityLost(provider.metrics, key, authorityLossStageRevalidate)
			return wrapAuthorityLost(err)
		}
		return err
	}
	if !authorityFenceEqual(current, resolved) {
		room.mu.Lock()
		room.authorityLost = true
		room.mu.Unlock()
		observeAuthorityLost(provider.metrics, key, authorityLossStageRevalidate)
		return wrapAuthorityLost(storage.ErrAuthorityLost)
	}

	room.mu.Lock()
	if !room.authorityLost {
		room.authority = resolved.Clone()
	}
	room.mu.Unlock()
	return nil
}

// DocumentKey retorna a chave do documento associada à conexão.
func (c *Connection) DocumentKey() storage.DocumentKey {
	if c == nil || c.room == nil {
		return storage.DocumentKey{}
	}
	return c.room.key
}

// HandleEncodedMessages aplica um stream protocolado à conexão usando
// `context.Background()` e retorna:
// - resposta direta para a conexão chamadora;
// - stream de broadcast uniforme para os demais peers do room.
func (c *Connection) HandleEncodedMessages(src []byte) (*DispatchResult, error) {
	return c.HandleEncodedMessagesContext(context.Background(), src)
}

// HandleEncodedMessagesWithOptions aplica um stream protocolado à conexão usando
// opções explícitas de saída e `context.Background()`.
func (c *Connection) HandleEncodedMessagesWithOptions(src []byte, opts ConnectionHandleOptions) (*DispatchResult, error) {
	return c.HandleEncodedMessagesContextWithOptions(context.Background(), src, opts)
}

// HandleEncodedMessagesContext aplica um stream protocolado à conexão e
// propaga `ctx` para operações bloqueantes de storage, incluindo append
// autoritativo sob fence quando configurado.
//
// `ctx == nil` é tratado como `context.Background()`.
func (c *Connection) HandleEncodedMessagesContext(ctx context.Context, src []byte) (*DispatchResult, error) {
	return c.HandleEncodedMessagesContextWithOptions(ctx, src, ConnectionHandleOptions{})
}

// HandleEncodedMessagesContextWithOptions aplica um stream protocolado à
// conexão, propagando `ctx` para operações bloqueantes e usando opções
// explícitas de egress para mensagens sync.
func (c *Connection) HandleEncodedMessagesContextWithOptions(ctx context.Context, src []byte, opts ConnectionHandleOptions) (*DispatchResult, error) {
	if c == nil {
		return nil, ErrConnectionClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateConnectionHandleOptions(opts); err != nil {
		return nil, err
	}

	messages, err := DecodeProtocolMessages(src)
	if err != nil {
		return nil, err
	}
	if len(messages) == 1 && messages[0] != nil &&
		messages[0].Protocol == ProtocolTypeSync &&
		messages[0].Sync != nil &&
		messages[0].Sync.Type == SyncMessageTypeStep1 {
		return c.handleSyncStep1Only(ctx, messages[0], opts)
	}

	c.room.mu.Lock()
	defer c.room.mu.Unlock()

	if c.closed {
		return nil, ErrConnectionClosed
	}
	if c.room.authorityLost {
		return nil, ErrAuthorityLost
	}

	result := &DispatchResult{}
	direct := make([]*ProtocolMessage, 0)

	for idx, message := range messages {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		directMessages, broadcast, err := c.room.handleMessageLocked(ctx, c, message, opts)
		if err != nil {
			return nil, fmt.Errorf("provider handle message %d: %w", idx, err)
		}
		direct = append(direct, directMessages...)
		result.Broadcast = append(result.Broadcast, broadcast...)
	}

	encodedDirect, err := EncodeProtocolEnvelopes(direct...)
	if err != nil {
		return nil, err
	}
	result.Direct = encodedDirect
	return result, nil
}

func (c *Connection) handleSyncStep1Only(ctx context.Context, message *ProtocolMessage, opts ConnectionHandleOptions) (*DispatchResult, error) {
	if err := validateProtocolMessage(message); err != nil {
		return nil, err
	}

	room := c.room
	if room == nil {
		return nil, ErrConnectionClosed
	}
	room.mu.Lock()
	if c.closed {
		room.mu.Unlock()
		return nil, ErrConnectionClosed
	}
	if room.authorityLost {
		room.mu.Unlock()
		return nil, ErrAuthorityLost
	}
	var updateV1 []byte
	if room.snapshot != nil {
		updateV1 = room.snapshot.UpdateV1
	}
	updateV2 := room.updateV2
	room.mu.Unlock()

	diff, err := diffForSyncOutputFormatFromUpdates(ctx, updateV1, updateV2, message.Sync.Payload, opts.DirectSyncOutputFormat)
	if err != nil {
		return nil, err
	}
	encodedDirect, err := EncodeProtocolEnvelope(&ProtocolMessage{
		Protocol: ProtocolTypeSync,
		Sync: &SyncMessage{
			Type:    SyncMessageTypeStep2,
			Payload: diff,
		},
	})
	if err != nil {
		return nil, err
	}
	return &DispatchResult{Direct: encodedDirect}, nil
}

func validateConnectionHandleOptions(opts ConnectionHandleOptions) error {
	for _, format := range []yjsbridge.UpdateFormat{opts.DirectSyncOutputFormat, opts.BroadcastSyncOutputFormat} {
		switch format {
		case yjsbridge.UpdateFormatUnknown, yjsbridge.UpdateFormatV1, yjsbridge.UpdateFormatV2:
		default:
			return fmt.Errorf("%w: %s", yjsbridge.ErrUnknownUpdateFormat, format)
		}
	}
	return nil
}

// Persist grava o snapshot autoritativo atual do documento no store configurado.
func (c *Connection) Persist(ctx context.Context) (record *storage.SnapshotRecord, err error) {
	if c == nil {
		return nil, ErrConnectionClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}

	c.room.mu.Lock()
	if c.closed {
		c.room.mu.Unlock()
		return nil, ErrConnectionClosed
	}
	if c.room.authorityLost {
		c.room.mu.Unlock()
		return nil, ErrAuthorityLost
	}
	if c.provider == nil || c.provider.store == nil {
		c.room.mu.Unlock()
		return nil, ErrPersistenceDisabled
	}
	key := c.room.key
	var snapshot *yjsbridge.PersistedSnapshot
	if c.room.snapshot != nil {
		snapshot = c.room.snapshot.Clone()
	}
	updateV2 := append([]byte(nil), c.room.updateV2...)
	lastOffset := c.room.lastOffset
	shouldTrim := lastOffset > c.room.compactedAt
	compacted := storage.UpdateOffset(0)
	if shouldTrim {
		compacted = lastOffset - c.room.compactedAt
	}
	authority := c.room.authority.Clone()
	c.room.mu.Unlock()
	start := time.Now()
	defer func() {
		if c.provider != nil {
			observePersist(c.provider.metrics, key, time.Since(start), lastOffset, compacted, err)
		}
	}()

	if snapshot == nil {
		snapshot, err = yjsbridge.DecodePersistedSnapshotV2Context(ctx, updateV2)
		if err != nil {
			return nil, err
		}
	}

	record, err = c.provider.saveSnapshot(ctx, key, snapshot, lastOffset, authority)
	if err != nil {
		if errors.Is(err, storage.ErrAuthorityLost) {
			c.room.mu.Lock()
			c.room.authorityLost = true
			c.room.mu.Unlock()
			observeAuthorityLost(c.provider.metrics, key, authorityLossStagePersistSave)
			return nil, wrapAuthorityLost(err)
		}
		return nil, err
	}

	if updateStore := c.provider.updateLogStore(); updateStore != nil && shouldTrim {
		if err := c.provider.trimUpdates(ctx, key, lastOffset, authority); err != nil {
			if errors.Is(err, storage.ErrAuthorityLost) {
				c.room.mu.Lock()
				c.room.authorityLost = true
				c.room.mu.Unlock()
				observeAuthorityLost(c.provider.metrics, key, authorityLossStagePersistTrim)
				return record, wrapAuthorityLost(err)
			}
			return record, fmt.Errorf("trim compacted updates through %d: %w", lastOffset, err)
		}

		c.room.mu.Lock()
		if c.room.compactedAt < lastOffset {
			c.room.compactedAt = lastOffset
		}
		c.room.mu.Unlock()
	}
	return record, nil
}

// Close remove a conexão do room e, se existir presença local, gera um
// broadcast awareness tombstone para os peers restantes.
func (c *Connection) Close() (*DispatchResult, error) {
	if c == nil {
		return nil, ErrConnectionClosed
	}

	room := c.room
	room.mu.Lock()
	if c.closed {
		room.mu.Unlock()
		return nil, ErrConnectionClosed
	}

	result := &DispatchResult{}
	if tombstone := c.localAwarenessTombstone(); len(tombstone.Clients) > 0 {
		message := &ProtocolMessage{
			Protocol:  ProtocolTypeAwareness,
			Awareness: tombstone,
		}
		encoded, err := EncodeProtocolEnvelope(message)
		if err != nil {
			room.mu.Unlock()
			return nil, err
		}
		result.Broadcast = encoded
	}

	c.closed = true
	delete(room.connections, c.id)
	empty := len(room.connections) == 0
	room.mu.Unlock()

	if empty && c.provider != nil {
		c.provider.mu.Lock()
		removed := false
		if current := c.provider.rooms[room.key]; current == room {
			delete(c.provider.rooms, room.key)
			removed = true
		}
		c.provider.mu.Unlock()
		if removed {
			observeRoomClosed(c.provider.metrics, room.key)
		}
	}

	return result, nil
}

func (p *Provider) ensureRoom(ctx context.Context, key storage.DocumentKey) (*providerRoom, error) {
	p.mu.Lock()
	room, ok := p.rooms[key]
	if ok {
		p.mu.Unlock()
		return room, nil
	}
	p.mu.Unlock()

	loaded, err, _ := p.roomLoads.Do(providerRoomLoadKey(key), func() (any, error) {
		p.mu.Lock()
		if current, ok := p.rooms[key]; ok {
			p.mu.Unlock()
			return current, nil
		}
		p.mu.Unlock()

		return p.loadRoom(ctx, key)
	})
	if err != nil {
		return nil, err
	}
	room, ok = loaded.(*providerRoom)
	if !ok || room == nil {
		return nil, fmt.Errorf("yprotocol: room load retornou %T", loaded)
	}
	return room, nil
}

func (p *Provider) loadRoom(ctx context.Context, key storage.DocumentKey) (*providerRoom, error) {
	authority, err := p.resolveRoomAuthority(ctx, key)
	if err != nil {
		if errors.Is(err, ErrAuthorityLost) {
			observeAuthorityLost(p.metrics, key, authorityLossStageOpen)
		}
		return nil, err
	}

	snapshot, lastOffset, compactedAt, err := p.loadSnapshot(ctx, key)
	if err != nil {
		return nil, err
	}
	updateV2, err := persistedSnapshotUpdateV2(snapshot)
	if err != nil {
		return nil, err
	}

	room := &providerRoom{
		key:         key,
		updateV2:    updateV2,
		lastOffset:  lastOffset,
		compactedAt: compactedAt,
		authority:   authority,
		connections: make(map[string]*Connection),
	}
	p.mu.Lock()
	if current, ok := p.rooms[key]; ok {
		p.mu.Unlock()
		return current, nil
	}
	p.rooms[key] = room
	p.mu.Unlock()
	observeRoomOpened(p.metrics, key)
	return room, nil
}

func providerRoomLoadKey(key storage.DocumentKey) string {
	return key.Namespace + "\x00" + key.DocumentID
}

func (p *Provider) loadSnapshot(ctx context.Context, key storage.DocumentKey) (*yjsbridge.PersistedSnapshot, storage.UpdateOffset, storage.UpdateOffset, error) {
	if p == nil || p.store == nil {
		return yjsbridge.NewPersistedSnapshot(), 0, 0, nil
	}
	if p.storageMetrics != nil {
		ctx = storage.ContextWithMetrics(ctx, p.storageMetrics)
	}

	if updateStore := p.updateLogStore(); updateStore != nil {
		recovered, err := storage.RecoverSnapshotStateContext(ctx, p.store, updateStore, key, 0, 0)
		if err != nil {
			return nil, 0, 0, err
		}
		if recovered == nil || recovered.Snapshot == nil {
			return yjsbridge.NewPersistedSnapshot(), 0, 0, nil
		}
		return recovered.Snapshot.Clone(), recovered.LastOffset, recovered.CheckpointThrough, nil
	}

	record, err := p.store.LoadSnapshot(ctx, key)
	if err != nil {
		if errors.Is(err, storage.ErrSnapshotNotFound) {
			return yjsbridge.NewPersistedSnapshot(), 0, 0, nil
		}
		return nil, 0, 0, err
	}
	if record == nil || record.Snapshot == nil {
		if record == nil {
			return yjsbridge.NewPersistedSnapshot(), 0, 0, nil
		}
		return yjsbridge.NewPersistedSnapshot(), record.Through, record.Through, nil
	}
	return record.Snapshot.Clone(), record.Through, record.Through, nil
}

func (p *Provider) updateLogStore() storage.UpdateLogStore {
	if p == nil || p.store == nil {
		return nil
	}
	updateStore, ok := p.store.(storage.UpdateLogStore)
	if !ok {
		return nil
	}
	return updateStore
}

func (p *Provider) authoritativeUpdateLogStore() storage.AuthoritativeUpdateLogStore {
	if p == nil || p.store == nil {
		return nil
	}
	updateStore, ok := p.store.(storage.AuthoritativeUpdateLogStore)
	if !ok {
		return nil
	}
	return updateStore
}

func (p *Provider) authoritativeSnapshotStore() storage.AuthoritativeSnapshotStore {
	if p == nil || p.store == nil {
		return nil
	}
	snapshotStore, ok := p.store.(storage.AuthoritativeSnapshotStore)
	if !ok {
		return nil
	}
	return snapshotStore
}

func (p *Provider) snapshotCheckpointStore() storage.SnapshotCheckpointStore {
	if p == nil || p.store == nil {
		return nil
	}
	snapshotStore, ok := p.store.(storage.SnapshotCheckpointStore)
	if !ok {
		return nil
	}
	return snapshotStore
}

func (p *Provider) authoritativeSnapshotCheckpointStore() storage.AuthoritativeSnapshotCheckpointStore {
	if p == nil || p.store == nil {
		return nil
	}
	snapshotStore, ok := p.store.(storage.AuthoritativeSnapshotCheckpointStore)
	if !ok {
		return nil
	}
	return snapshotStore
}

func (p *Provider) resolveRoomAuthority(ctx context.Context, key storage.DocumentKey) (*storage.AuthorityFence, error) {
	if p == nil || p.resolveAuthorityFence == nil {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if p.authoritativeSnapshotStore() == nil || p.authoritativeUpdateLogStore() == nil {
		return nil, ErrAuthorityFenceUnsupported
	}

	fence, err := p.resolveAuthorityFence(ctx, key)
	if err != nil {
		if errors.Is(err, storage.ErrAuthorityLost) {
			return nil, wrapAuthorityLost(err)
		}
		return nil, err
	}
	if fence == nil {
		return nil, wrapAuthorityLost(storage.ErrAuthorityLost)
	}
	if err := fence.Validate(); err != nil {
		return nil, err
	}
	return fence.Clone(), nil
}

func (p *Provider) saveSnapshot(ctx context.Context, key storage.DocumentKey, snapshot *yjsbridge.PersistedSnapshot, through storage.UpdateOffset, authority *storage.AuthorityFence) (*storage.SnapshotRecord, error) {
	if authority == nil {
		if checkpointStore := p.snapshotCheckpointStore(); checkpointStore != nil {
			return checkpointStore.SaveSnapshotCheckpoint(ctx, key, snapshot, through)
		}
		return p.store.SaveSnapshot(ctx, key, snapshot)
	}
	if checkpointStore := p.authoritativeSnapshotCheckpointStore(); checkpointStore != nil {
		return checkpointStore.SaveSnapshotCheckpointAuthoritative(ctx, key, snapshot, through, *authority)
	}
	return p.authoritativeSnapshotStore().SaveSnapshotAuthoritative(ctx, key, snapshot, *authority)
}

func (p *Provider) trimUpdates(ctx context.Context, key storage.DocumentKey, through storage.UpdateOffset, authority *storage.AuthorityFence) error {
	updateStore := p.updateLogStore()
	if updateStore == nil {
		return nil
	}
	if authority == nil {
		return updateStore.TrimUpdates(ctx, key, through)
	}
	return p.authoritativeUpdateLogStore().TrimUpdatesAuthoritative(ctx, key, through, *authority)
}

func wrapAuthorityLost(err error) error {
	if err == nil {
		return ErrAuthorityLost
	}
	return fmt.Errorf("%w: %v", ErrAuthorityLost, err)
}

func authorityFenceEqual(left *storage.AuthorityFence, right *storage.AuthorityFence) bool {
	switch {
	case left == nil && right == nil:
		return true
	case left == nil || right == nil:
		return false
	default:
		return left.ShardID == right.ShardID &&
			left.Owner == right.Owner &&
			left.Token == right.Token
	}
}

func (r *providerRoom) handleMessageLocked(ctx context.Context, sender *Connection, message *ProtocolMessage, opts ConnectionHandleOptions) ([]*ProtocolMessage, []byte, error) {
	if r.authorityLost {
		return nil, nil, ErrAuthorityLost
	}
	if err := validateProtocolMessage(message); err != nil {
		return nil, nil, err
	}

	switch message.Protocol {
	case ProtocolTypeQueryAwareness:
		return []*ProtocolMessage{{
			Protocol:  ProtocolTypeAwareness,
			Awareness: r.aggregateLocalAwarenessLocked(""),
		}}, nil, nil
	case ProtocolTypeSync:
		if message.Sync.Type == SyncMessageTypeStep2 || message.Sync.Type == SyncMessageTypeUpdate {
			updateV2, err := r.applyDocumentPayloadLocked(ctx, sender.provider, r.key, message.Sync.Payload)
			if err != nil {
				return nil, nil, err
			}
			payload, err := convertForSyncOutputFormat(updateV2, opts.BroadcastSyncOutputFormat)
			if err != nil {
				return nil, nil, err
			}
			encoded, err := EncodeProtocolEnvelope(&ProtocolMessage{
				Protocol: ProtocolTypeSync,
				Sync: &SyncMessage{
					Type:    message.Sync.Type,
					Payload: payload,
				},
			})
			if err != nil {
				return nil, nil, err
			}
			return nil, encoded, nil
		}
		if message.Sync.Type != SyncMessageTypeStep1 {
			return nil, nil, fmt.Errorf("%w: %d", ErrUnknownSyncMessageType, message.Sync.Type)
		}

		var updateV1 []byte
		if r.snapshot != nil {
			updateV1 = r.snapshot.UpdateV1
		}
		diff, err := diffForSyncOutputFormatFromUpdates(ctx, updateV1, r.updateV2, message.Sync.Payload, opts.DirectSyncOutputFormat)
		if err != nil {
			return nil, nil, err
		}
		return []*ProtocolMessage{{
			Protocol: ProtocolTypeSync,
			Sync: &SyncMessage{
				Type:    SyncMessageTypeStep2,
				Payload: diff,
			},
		}}, nil, nil
	case ProtocolTypeAwareness:
		if _, err := sender.session.HandleProtocolMessage(message); err != nil {
			return nil, nil, err
		}
		encoded, err := EncodeProtocolEnvelope(message)
		if err != nil {
			return nil, nil, err
		}
		return nil, encoded, nil
	case ProtocolTypeAuth:
		_, err := sender.session.HandleProtocolMessage(message)
		return nil, nil, err
	default:
		return nil, nil, fmt.Errorf("%w: %d", ErrUnknownProtocolType, message.Protocol)
	}
}

func (r *providerRoom) applyDocumentPayloadLocked(ctx context.Context, provider *Provider, key storage.DocumentKey, payload []byte) ([]byte, error) {
	if r.authorityLost {
		return nil, ErrAuthorityLost
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	updateV2, err := yjsbridge.ConvertUpdateToV2YjsWire(payload)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var appendedOffset storage.UpdateOffset
	var updateV1 []byte
	if provider != nil {
		if updateStore := provider.updateLogStore(); updateStore != nil {
			var (
				record *storage.UpdateLogRecord
				err    error
			)
			if r.authority != nil {
				if updateStoreV2, ok := provider.store.(storage.AuthoritativeUpdateLogStoreV2); ok {
					record, err = updateStoreV2.AppendUpdateV2Authoritative(ctx, key, updateV2, *r.authority)
				} else {
					updateV1, err = yjsbridge.ConvertUpdateToV1YjsWire(updateV2)
					if err != nil {
						return nil, err
					}
					record, err = provider.authoritativeUpdateLogStore().AppendUpdateAuthoritative(ctx, key, updateV1, *r.authority)
				}
			} else if updateStoreV2, ok := updateStore.(storage.UpdateLogStoreV2); ok {
				record, err = updateStoreV2.AppendUpdateV2(ctx, key, updateV2)
			} else {
				updateV1, err = yjsbridge.ConvertUpdateToV1YjsWire(updateV2)
				if err != nil {
					return nil, err
				}
				record, err = updateStore.AppendUpdate(ctx, key, updateV1)
			}
			if err != nil {
				if errors.Is(err, storage.ErrAuthorityLost) {
					r.authorityLost = true
					if provider != nil {
						observeAuthorityLost(provider.metrics, key, authorityLossStageAppend)
					}
					return nil, wrapAuthorityLost(err)
				}
				return nil, err
			}
			if record != nil {
				appendedOffset = record.Offset
			}
		}
	}

	merged, err := yjsbridge.MergeUpdatesV2Context(ctx, r.updateV2, updateV2)
	if err != nil {
		if appendedOffset == 0 || provider == nil {
			return nil, err
		}

		recovered, lastOffset, compactedAt, recoverErr := provider.loadSnapshot(context.Background(), key)
		if recoverErr != nil {
			return nil, fmt.Errorf("rebuild room snapshot: %w (recover: %v)", err, recoverErr)
		}
		recoveredUpdateV2, updateErr := persistedSnapshotUpdateV2(recovered)
		if updateErr != nil {
			return nil, fmt.Errorf("rebuild room snapshot v2: %w", updateErr)
		}
		r.snapshot = nil
		r.updateV2 = recoveredUpdateV2
		if lastOffset < appendedOffset {
			lastOffset = appendedOffset
		}
		r.lastOffset = lastOffset
		r.compactedAt = compactedAt
		return r.updateV2, nil
	}

	r.snapshot = nil
	r.updateV2 = merged
	if appendedOffset > 0 {
		r.lastOffset = appendedOffset
	}
	return updateV2, nil
}

func persistedSnapshotUpdateV2(snapshot *yjsbridge.PersistedSnapshot) ([]byte, error) {
	if snapshot != nil && len(snapshot.UpdateV2) != 0 {
		return snapshot.UpdateV2, nil
	}
	updateV2, err := yjsbridge.EncodePersistedSnapshotV2(snapshot)
	if err != nil {
		return nil, err
	}
	return updateV2, nil
}

func (r *providerRoom) aggregateLocalAwarenessLocked(excludeConnectionID string) *yawareness.Update {
	clients := make([]yawareness.ClientState, 0, len(r.connections))
	for _, connection := range r.connections {
		if connection.id == excludeConnectionID || connection.closed {
			continue
		}
		clientIDs := [1]uint32{connection.clientID}
		update := connection.session.Awareness().UpdateForClients(clientIDs[:])
		if update == nil || len(update.Clients) == 0 {
			continue
		}
		for _, client := range update.Clients {
			clients = append(clients, yawareness.ClientState{
				ClientID: client.ClientID,
				Clock:    client.Clock,
				State:    append([]byte(nil), client.State...),
			})
		}
	}
	return &yawareness.Update{Clients: clients}
}

func (c *Connection) localAwarenessTombstone() *yawareness.Update {
	if c == nil || c.session == nil || c.session.Awareness() == nil {
		return &yawareness.Update{}
	}

	if _, ok := c.session.Awareness().Meta(c.clientID); !ok {
		return &yawareness.Update{}
	}
	if err := c.session.Awareness().SetLocalState(nil); err != nil {
		return &yawareness.Update{}
	}
	update := c.session.Awareness().UpdateForClients([]uint32{c.clientID})
	if update == nil {
		return &yawareness.Update{}
	}
	return update
}
