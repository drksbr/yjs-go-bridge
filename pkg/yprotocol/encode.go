package yprotocol

import (
	"errors"
	"fmt"

	internal "github.com/drksbr/yjs-crdt-golang-server/internal/yprotocol"
)

var (
	// ErrNilProtocolMessage sinaliza tentativa de encode/handle de mensagem nula.
	ErrNilProtocolMessage = errors.New("yprotocol: protocol message nao pode ser nil")
	// ErrInvalidProtocolMessage sinaliza shape inconsistente entre envelope e payload.
	ErrInvalidProtocolMessage = errors.New("yprotocol: protocol message invalida")
	// ErrNilSession sinaliza uso de Session nula.
	ErrNilSession = errors.New("yprotocol: session nao pode ser nil")
)

// EncodeProtocolEnvelope serializa uma mensagem tipada do envelope y-protocols.
func EncodeProtocolEnvelope(message *ProtocolMessage) ([]byte, error) {
	return appendProtocolEnvelope(nil, message)
}

// EncodeProtocolEnvelopes serializa um stream concatenado de mensagens tipadas.
func EncodeProtocolEnvelopes(messages ...*ProtocolMessage) ([]byte, error) {
	dst := make([]byte, 0, protocolEnvelopesSizeHint(messages))
	for idx, message := range messages {
		var err error
		dst, err = appendProtocolEnvelope(dst, message)
		if err != nil {
			return nil, fmt.Errorf("encode protocol envelope %d: %w", idx, err)
		}
	}
	return dst, nil
}

func appendProtocolEnvelope(dst []byte, message *ProtocolMessage) ([]byte, error) {
	if err := validateProtocolMessage(message); err != nil {
		return nil, err
	}

	switch message.Protocol {
	case ProtocolTypeSync:
		dst = internal.AppendProtocolType(dst, ProtocolTypeSync)
		return internal.AppendSyncMessage(dst, message.Sync.Type, message.Sync.Payload)
	case ProtocolTypeAwareness:
		encoded, err := EncodeProtocolAwarenessUpdate(message.Awareness)
		if err != nil {
			return nil, err
		}
		return append(dst, encoded...), nil
	case ProtocolTypeAuth:
		dst = internal.AppendProtocolType(dst, ProtocolTypeAuth)
		return internal.AppendAuthMessage(dst, message.Auth.Type, message.Auth.Reason)
	case ProtocolTypeQueryAwareness:
		return internal.AppendProtocolType(dst, ProtocolTypeQueryAwareness), nil
	default:
		return nil, fmt.Errorf("%w: %d", ErrUnknownProtocolType, message.Protocol)
	}
}

func protocolEnvelopesSizeHint(messages []*ProtocolMessage) int {
	size := 0
	for _, message := range messages {
		if message == nil {
			continue
		}
		switch {
		case message.Sync != nil:
			size += len(message.Sync.Payload) + 8
		case message.Awareness != nil:
			for _, client := range message.Awareness.Clients {
				size += len(client.State) + 16
			}
		case message.Auth != nil:
			size += len(message.Auth.Reason) + 8
		default:
			size += 2
		}
	}
	return size
}

func validateProtocolMessage(message *ProtocolMessage) error {
	if message == nil {
		return ErrNilProtocolMessage
	}

	populated := 0
	if message.Sync != nil {
		populated++
	}
	if message.Awareness != nil {
		populated++
	}
	if message.Auth != nil {
		populated++
	}
	if message.QueryAwareness != nil {
		populated++
	}

	switch message.Protocol {
	case ProtocolTypeSync:
		if message.Sync == nil || populated != 1 {
			return fmt.Errorf("%w: protocolo sync exige exatamente um payload sync", ErrInvalidProtocolMessage)
		}
	case ProtocolTypeAwareness:
		if message.Awareness == nil || populated != 1 {
			return fmt.Errorf("%w: protocolo awareness exige exatamente um payload awareness", ErrInvalidProtocolMessage)
		}
	case ProtocolTypeAuth:
		if message.Auth == nil || populated != 1 {
			return fmt.Errorf("%w: protocolo auth exige exatamente um payload auth", ErrInvalidProtocolMessage)
		}
	case ProtocolTypeQueryAwareness:
		if populated > 1 || message.Sync != nil || message.Awareness != nil || message.Auth != nil {
			return fmt.Errorf("%w: query-awareness nao aceita payload extra", ErrInvalidProtocolMessage)
		}
	default:
		return fmt.Errorf("%w: %d", ErrUnknownProtocolType, message.Protocol)
	}

	return nil
}
