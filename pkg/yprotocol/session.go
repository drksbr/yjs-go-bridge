package yprotocol

import (
	"context"
	"fmt"
	"sync"

	"github.com/drksbr/yjs-crdt-golang-server/pkg/yawareness"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/yjsbridge"
)

// Session mantém um estado mínimo em-processo para handshake e ingestão de
// envelopes y-protocols com documento canônico em Update V2.
//
// O runtime cobre apenas:
// - update V2 canônico do documento;
// - compatibilidade V1 derivada nas APIs antigas;
// - awareness local/remoto via `StateManager`;
// - resposta a `SyncStep1` com `SyncStep2`;
// - resposta a `query-awareness` com snapshot de awareness.
//
// O tipo não implementa websocket server, auth policy distribuída nem provider
// completo.
type Session struct {
	mu        sync.RWMutex
	updateV2  []byte
	awareness *yawareness.StateManager
}

// SessionHandleOptions controla opções explícitas de saída do runtime local.
type SessionHandleOptions struct {
	// SyncOutputFormat define o formato usado em respostas SyncStep2.
	//
	// Zero value e UpdateFormatV1 preservam compatibilidade V1. Use
	// UpdateFormatV2 quando o caller já negociou suporte V2 com o peer.
	SyncOutputFormat yjsbridge.UpdateFormat
}

// NewSession cria um runtime in-process vazio para um client local.
func NewSession(localClientID uint32) *Session {
	return &Session{
		updateV2:  newEmptyUpdateV2(),
		awareness: yawareness.NewStateManager(localClientID),
	}
}

// Awareness expõe o runtime thread-safe de awareness associado à sessão.
func (s *Session) Awareness() *yawareness.StateManager {
	if s == nil {
		return nil
	}
	return s.awareness
}

// UpdateV1 retorna uma cópia compatível V1 derivada do estado V2 atual.
func (s *Session) UpdateV1() []byte {
	if s == nil {
		return nil
	}

	updateV2 := s.UpdateV2()
	if len(updateV2) == 0 {
		return nil
	}
	updateV1, err := yjsbridge.ConvertUpdateToV1YjsWire(updateV2)
	if err != nil {
		return nil
	}
	return updateV1
}

// UpdateV2 retorna uma cópia do update V2 canônico atual.
func (s *Session) UpdateV2() []byte {
	if s == nil {
		return nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]byte(nil), s.updateV2...)
}

// LoadUpdate substitui o estado atual pelo update suportado informado,
// normalizado para V2 canônico.
func (s *Session) LoadUpdate(update []byte) error {
	if s == nil {
		return ErrNilSession
	}

	updateV2, err := yjsbridge.ConvertUpdateToV2YjsWire(update)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.updateV2 = append([]byte(nil), updateV2...)
	return nil
}

// LoadPersistedSnapshot substitui o documento atual usando um snapshot já
// persistido. Snapshot `nil` reinicia a sessão para o estado vazio.
func (s *Session) LoadPersistedSnapshot(snapshot *yjsbridge.PersistedSnapshot) error {
	if s == nil {
		return ErrNilSession
	}
	if snapshot == nil || snapshot.IsEmpty() {
		s.mu.Lock()
		s.updateV2 = newEmptyUpdateV2()
		s.mu.Unlock()
		return nil
	}
	updateV2, err := yjsbridge.EncodePersistedSnapshotV2(snapshot)
	if err != nil {
		return err
	}
	return s.LoadUpdate(updateV2)
}

// PersistedSnapshot materializa um snapshot persistível V1-compatible a partir
// do estado V2 atual.
func (s *Session) PersistedSnapshot() (*yjsbridge.PersistedSnapshot, error) {
	if s == nil {
		return nil, ErrNilSession
	}
	return yjsbridge.PersistedSnapshotFromUpdate(s.UpdateV2())
}

// HandleProtocolMessage aplica uma mensagem inbound e retorna as respostas
// necessárias do runtime mínimo.
func (s *Session) HandleProtocolMessage(message *ProtocolMessage) ([]*ProtocolMessage, error) {
	return s.HandleProtocolMessageWithOptions(message, SessionHandleOptions{})
}

// HandleProtocolMessageWithOptions aplica uma mensagem inbound usando opções
// explícitas de saída, preservando o estado interno V2-canônico.
func (s *Session) HandleProtocolMessageWithOptions(message *ProtocolMessage, opts SessionHandleOptions) ([]*ProtocolMessage, error) {
	if s == nil {
		return nil, ErrNilSession
	}
	if err := validateProtocolMessage(message); err != nil {
		return nil, err
	}

	switch message.Protocol {
	case ProtocolTypeSync:
		return s.handleSyncMessage(message.Sync, opts)
	case ProtocolTypeAwareness:
		s.awareness.Apply(message.Awareness)
		return nil, nil
	case ProtocolTypeAuth:
		// Auth policy fica fora do escopo do runtime mínimo.
		return nil, nil
	case ProtocolTypeQueryAwareness:
		return []*ProtocolMessage{{
			Protocol:  ProtocolTypeAwareness,
			Awareness: s.awareness.Snapshot(),
		}}, nil
	default:
		return nil, fmt.Errorf("%w: %d", ErrUnknownProtocolType, message.Protocol)
	}
}

// HandleProtocolMessages processa uma sequência de mensagens inbound e agrega as
// respostas no mesmo ordenamento.
func (s *Session) HandleProtocolMessages(messages ...*ProtocolMessage) ([]*ProtocolMessage, error) {
	return s.HandleProtocolMessagesWithOptions(SessionHandleOptions{}, messages...)
}

// HandleProtocolMessagesWithOptions processa uma sequência de mensagens inbound
// usando opções explícitas de saída e agrega as respostas no mesmo ordenamento.
func (s *Session) HandleProtocolMessagesWithOptions(opts SessionHandleOptions, messages ...*ProtocolMessage) ([]*ProtocolMessage, error) {
	if s == nil {
		return nil, ErrNilSession
	}

	out := make([]*ProtocolMessage, 0, len(messages))
	for idx, message := range messages {
		responses, err := s.HandleProtocolMessageWithOptions(message, opts)
		if err != nil {
			return nil, fmt.Errorf("handle protocol message %d: %w", idx, err)
		}
		out = append(out, responses...)
	}
	return out, nil
}

// HandleEncodedMessages decodifica um stream inbound, aplica as mensagens à
// sessão e retorna o stream outbound concatenado.
func (s *Session) HandleEncodedMessages(src []byte) ([]byte, error) {
	return s.HandleEncodedMessagesWithOptions(src, SessionHandleOptions{})
}

// HandleEncodedMessagesWithOptions decodifica um stream inbound, aplica as
// mensagens à sessão e retorna o stream outbound usando opções explícitas.
func (s *Session) HandleEncodedMessagesWithOptions(src []byte, opts SessionHandleOptions) ([]byte, error) {
	if s == nil {
		return nil, ErrNilSession
	}

	messages, err := DecodeProtocolMessages(src)
	if err != nil {
		return nil, err
	}
	responses, err := s.HandleProtocolMessagesWithOptions(opts, messages...)
	if err != nil {
		return nil, err
	}
	return EncodeProtocolEnvelopes(responses...)
}

func (s *Session) handleSyncMessage(message *SyncMessage, opts SessionHandleOptions) ([]*ProtocolMessage, error) {
	switch message.Type {
	case SyncMessageTypeStep1:
		diff, err := s.diffForSyncStep1(message.Payload, opts)
		if err != nil {
			return nil, err
		}
		return []*ProtocolMessage{{
			Protocol: ProtocolTypeSync,
			Sync: &SyncMessage{
				Type:    SyncMessageTypeStep2,
				Payload: diff,
			},
		}}, nil
	case SyncMessageTypeStep2, SyncMessageTypeUpdate:
		current := s.UpdateV2()
		updateV2, err := yjsbridge.ConvertUpdateToV2YjsWire(message.Payload)
		if err != nil {
			return nil, err
		}
		merged, err := yjsbridge.MergeUpdatesV2(current, updateV2)
		if err != nil {
			return nil, err
		}

		s.mu.Lock()
		s.updateV2 = append([]byte(nil), merged...)
		s.mu.Unlock()
		return nil, nil
	default:
		return nil, fmt.Errorf("%w: %d", ErrUnknownSyncMessageType, message.Type)
	}
}

func (s *Session) diffForSyncStep1(stateVector []byte, opts SessionHandleOptions) ([]byte, error) {
	return diffForSyncOutputFormat(context.Background(), s.UpdateV2(), stateVector, opts.SyncOutputFormat)
}

func diffForSyncOutputFormat(ctx context.Context, updateV2, stateVector []byte, format yjsbridge.UpdateFormat) ([]byte, error) {
	return diffForSyncOutputFormatFromUpdates(ctx, nil, updateV2, stateVector, format)
}

func diffForSyncOutputFormatFromUpdates(ctx context.Context, updateV1, updateV2, stateVector []byte, format yjsbridge.UpdateFormat) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	switch format {
	case yjsbridge.UpdateFormatUnknown, yjsbridge.UpdateFormatV1:
		if len(updateV1) == 0 {
			var err error
			updateV1, err = yjsbridge.ConvertUpdateToV1YjsWire(updateV2)
			if err != nil {
				return nil, err
			}
			return yjsbridge.DiffUpdateContext(ctx, updateV1, stateVector)
		}
		diffV1, err := yjsbridge.DiffUpdateContext(ctx, updateV1, stateVector)
		if err != nil {
			return nil, err
		}
		return yjsbridge.ConvertUpdateToV1YjsWire(diffV1)
	case yjsbridge.UpdateFormatV2:
		diffV1, err := yjsbridge.DiffUpdateContext(ctx, updateV2, stateVector)
		if err != nil {
			return nil, err
		}
		return yjsbridge.ConvertUpdateToV2YjsWire(diffV1)
	default:
		return nil, fmt.Errorf("%w: %s", yjsbridge.ErrUnknownUpdateFormat, format)
	}
}

func convertForSyncOutputFormat(updateV2 []byte, format yjsbridge.UpdateFormat) ([]byte, error) {
	switch format {
	case yjsbridge.UpdateFormatUnknown, yjsbridge.UpdateFormatV1:
		return yjsbridge.ConvertUpdateToV1YjsWire(updateV2)
	case yjsbridge.UpdateFormatV2:
		return yjsbridge.ConvertUpdateToV2YjsWire(updateV2)
	default:
		return nil, fmt.Errorf("%w: %s", yjsbridge.ErrUnknownUpdateFormat, format)
	}
}

func newEmptyUpdateV2() []byte {
	snapshot := yjsbridge.NewPersistedSnapshot()
	if snapshot == nil || len(snapshot.UpdateV2) == 0 {
		return nil
	}
	return append([]byte(nil), snapshot.UpdateV2...)
}
