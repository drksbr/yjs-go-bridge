package ynodeproto

import (
	"encoding/binary"
	"fmt"
	"strings"

	ybinary "github.com/drksbr/yjs-crdt-golang-server/internal/binary"
	"github.com/drksbr/yjs-crdt-golang-server/internal/varint"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/storage"
)

const fixedUint64Size = 8
const fixedUint32Size = 4

type routedRoute struct {
	DocumentKey  storage.DocumentKey
	ConnectionID string
	Epoch        uint64
}

type routedPayload struct {
	routedRoute
	Body []byte
}

// Message representa um payload tipado do protocolo inter-node.
//
// As implementações públicas desse contrato são as structs exportadas deste
// pacote. O método não exportado fecha o conjunto de implementações aceitas
// pelo codec.
type Message interface {
	Type() MessageType
	FrameFlags() Flags
	Validate() error
	appendPayload(dst []byte) ([]byte, error)
}

// ParseError adiciona contexto e offset às falhas de parsing de payloads tipados.
type ParseError struct {
	Op     string
	Offset int
	Err    error
}

// Error retorna a mensagem formatada do erro com contexto e offset.
func (e *ParseError) Error() string {
	return fmt.Sprintf("ynodeproto: %s falhou no offset %d: %v", e.Op, e.Offset, e.Err)
}

// Unwrap expõe o erro interno para `errors.Is` e `errors.As`.
func (e *ParseError) Unwrap() error {
	return e.Err
}

// Handshake inicia a negociação de identidade e roteamento de uma conexão de
// documento entre nós.
type Handshake struct {
	Flags        Flags
	NodeID       string
	DocumentKey  storage.DocumentKey
	ConnectionID string
	ClientID     uint32
	Epoch        uint64
}

// Type retorna o message type associado ao payload.
func (m *Handshake) Type() MessageType {
	return MessageTypeHandshake
}

// FrameFlags retorna os bits auxiliares do header preservados por esta mensagem.
func (m *Handshake) FrameFlags() Flags {
	if m == nil {
		return FlagNone
	}
	return m.Flags
}

// Validate confirma se o payload contém os campos mínimos exigidos pelo wire.
func (m *Handshake) Validate() error {
	if m == nil {
		return ErrNilMessage
	}
	return validateNodeRoute(m.NodeID, m.DocumentKey, m.ConnectionID, m.Epoch)
}

func (m *Handshake) appendPayload(dst []byte) ([]byte, error) {
	var err error

	dst, err = appendString(dst, m.NodeID)
	if err != nil {
		return nil, err
	}
	dst, err = appendDocumentKey(dst, m.DocumentKey)
	if err != nil {
		return nil, err
	}
	dst, err = appendString(dst, m.ConnectionID)
	if err != nil {
		return nil, err
	}
	dst = appendUint64(dst, m.Epoch)
	return appendUint32(dst, m.ClientID), nil
}

// HandshakeAck confirma o handshake e espelha o contexto roteável aceito.
type HandshakeAck struct {
	Flags        Flags
	NodeID       string
	DocumentKey  storage.DocumentKey
	ConnectionID string
	ClientID     uint32
	Epoch        uint64
}

// Type retorna o message type associado ao payload.
func (m *HandshakeAck) Type() MessageType {
	return MessageTypeHandshakeAck
}

// FrameFlags retorna os bits auxiliares do header preservados por esta mensagem.
func (m *HandshakeAck) FrameFlags() Flags {
	if m == nil {
		return FlagNone
	}
	return m.Flags
}

// Validate confirma se o payload contém os campos mínimos exigidos pelo wire.
func (m *HandshakeAck) Validate() error {
	if m == nil {
		return ErrNilMessage
	}
	return validateNodeRoute(m.NodeID, m.DocumentKey, m.ConnectionID, m.Epoch)
}

func (m *HandshakeAck) appendPayload(dst []byte) ([]byte, error) {
	var err error

	dst, err = appendString(dst, m.NodeID)
	if err != nil {
		return nil, err
	}
	dst, err = appendDocumentKey(dst, m.DocumentKey)
	if err != nil {
		return nil, err
	}
	dst, err = appendString(dst, m.ConnectionID)
	if err != nil {
		return nil, err
	}
	dst = appendUint64(dst, m.Epoch)
	return appendUint32(dst, m.ClientID), nil
}

// DocumentSyncRequest solicita catch-up de um documento e carrega o state
// vector bruto do solicitante.
type DocumentSyncRequest struct {
	Flags        Flags
	DocumentKey  storage.DocumentKey
	ConnectionID string
	Epoch        uint64
	StateVector  []byte
}

// Type retorna o message type associado ao payload.
func (m *DocumentSyncRequest) Type() MessageType {
	return MessageTypeDocumentSyncRequest
}

// FrameFlags retorna os bits auxiliares do header preservados por esta mensagem.
func (m *DocumentSyncRequest) FrameFlags() Flags {
	if m == nil {
		return FlagNone
	}
	return m.Flags
}

// Validate confirma se o payload contém os campos mínimos exigidos pelo wire.
func (m *DocumentSyncRequest) Validate() error {
	if m == nil {
		return ErrNilMessage
	}
	return validateRoutedBody(m.DocumentKey, m.ConnectionID, m.Epoch, m.StateVector)
}

func (m *DocumentSyncRequest) appendPayload(dst []byte) ([]byte, error) {
	return appendRoutedPayload(dst, m.DocumentKey, m.ConnectionID, m.Epoch, m.StateVector)
}

// DocumentSyncRequestV2 solicita catch-up de um documento para uma resposta em
// Update V2 e carrega o state vector bruto do solicitante.
type DocumentSyncRequestV2 struct {
	Flags        Flags
	DocumentKey  storage.DocumentKey
	ConnectionID string
	Epoch        uint64
	StateVector  []byte
}

// Type retorna o message type associado ao payload.
func (m *DocumentSyncRequestV2) Type() MessageType {
	return MessageTypeDocumentSyncRequestV2
}

// FrameFlags retorna os bits auxiliares do header preservados por esta mensagem.
func (m *DocumentSyncRequestV2) FrameFlags() Flags {
	if m == nil {
		return FlagNone
	}
	return m.Flags
}

// Validate confirma se o payload contém os campos mínimos exigidos pelo wire.
func (m *DocumentSyncRequestV2) Validate() error {
	if m == nil {
		return ErrNilMessage
	}
	return validateRoutedBody(m.DocumentKey, m.ConnectionID, m.Epoch, m.StateVector)
}

func (m *DocumentSyncRequestV2) appendPayload(dst []byte) ([]byte, error) {
	return appendRoutedPayload(dst, m.DocumentKey, m.ConnectionID, m.Epoch, m.StateVector)
}

// DocumentSyncResponse entrega o update V1 necessário para sincronizar um
// documento.
type DocumentSyncResponse struct {
	Flags        Flags
	DocumentKey  storage.DocumentKey
	ConnectionID string
	Epoch        uint64
	UpdateV1     []byte
}

// Type retorna o message type associado ao payload.
func (m *DocumentSyncResponse) Type() MessageType {
	return MessageTypeDocumentSyncResponse
}

// FrameFlags retorna os bits auxiliares do header preservados por esta mensagem.
func (m *DocumentSyncResponse) FrameFlags() Flags {
	if m == nil {
		return FlagNone
	}
	return m.Flags
}

// Validate confirma se o payload contém os campos mínimos exigidos pelo wire.
func (m *DocumentSyncResponse) Validate() error {
	if m == nil {
		return ErrNilMessage
	}
	return validateRoutedBody(m.DocumentKey, m.ConnectionID, m.Epoch, m.UpdateV1)
}

func (m *DocumentSyncResponse) appendPayload(dst []byte) ([]byte, error) {
	return appendRoutedPayload(dst, m.DocumentKey, m.ConnectionID, m.Epoch, m.UpdateV1)
}

// DocumentSyncResponseV2 entrega o update V2 necessário para sincronizar um
// documento quando o handshake inter-node negociou suporte explícito.
type DocumentSyncResponseV2 struct {
	Flags        Flags
	DocumentKey  storage.DocumentKey
	ConnectionID string
	Epoch        uint64
	UpdateV2     []byte
}

// Type retorna o message type associado ao payload.
func (m *DocumentSyncResponseV2) Type() MessageType {
	return MessageTypeDocumentSyncResponseV2
}

// FrameFlags retorna os bits auxiliares do header preservados por esta mensagem.
func (m *DocumentSyncResponseV2) FrameFlags() Flags {
	if m == nil {
		return FlagNone
	}
	return m.Flags
}

// Validate confirma se o payload contém os campos mínimos exigidos pelo wire.
func (m *DocumentSyncResponseV2) Validate() error {
	if m == nil {
		return ErrNilMessage
	}
	return validateRoutedBody(m.DocumentKey, m.ConnectionID, m.Epoch, m.UpdateV2)
}

func (m *DocumentSyncResponseV2) appendPayload(dst []byte) ([]byte, error) {
	return appendRoutedPayload(dst, m.DocumentKey, m.ConnectionID, m.Epoch, m.UpdateV2)
}

// DocumentUpdate carrega um update V1 incremental de documento.
type DocumentUpdate struct {
	Flags        Flags
	DocumentKey  storage.DocumentKey
	ConnectionID string
	Epoch        uint64
	UpdateV1     []byte
}

// Type retorna o message type associado ao payload.
func (m *DocumentUpdate) Type() MessageType {
	return MessageTypeDocumentUpdate
}

// FrameFlags retorna os bits auxiliares do header preservados por esta mensagem.
func (m *DocumentUpdate) FrameFlags() Flags {
	if m == nil {
		return FlagNone
	}
	return m.Flags
}

// Validate confirma se o payload contém os campos mínimos exigidos pelo wire.
func (m *DocumentUpdate) Validate() error {
	if m == nil {
		return ErrNilMessage
	}
	return validateRoutedBody(m.DocumentKey, m.ConnectionID, m.Epoch, m.UpdateV1)
}

func (m *DocumentUpdate) appendPayload(dst []byte) ([]byte, error) {
	return appendRoutedPayload(dst, m.DocumentKey, m.ConnectionID, m.Epoch, m.UpdateV1)
}

// DocumentUpdateV2 carrega um update V2 incremental de documento quando o
// handshake inter-node negociou suporte explícito.
type DocumentUpdateV2 struct {
	Flags        Flags
	DocumentKey  storage.DocumentKey
	ConnectionID string
	Epoch        uint64
	UpdateV2     []byte
}

// Type retorna o message type associado ao payload.
func (m *DocumentUpdateV2) Type() MessageType {
	return MessageTypeDocumentUpdateV2
}

// FrameFlags retorna os bits auxiliares do header preservados por esta mensagem.
func (m *DocumentUpdateV2) FrameFlags() Flags {
	if m == nil {
		return FlagNone
	}
	return m.Flags
}

// Validate confirma se o payload contém os campos mínimos exigidos pelo wire.
func (m *DocumentUpdateV2) Validate() error {
	if m == nil {
		return ErrNilMessage
	}
	return validateRoutedBody(m.DocumentKey, m.ConnectionID, m.Epoch, m.UpdateV2)
}

func (m *DocumentUpdateV2) appendPayload(dst []byte) ([]byte, error) {
	return appendRoutedPayload(dst, m.DocumentKey, m.ConnectionID, m.Epoch, m.UpdateV2)
}

// DocumentUpdateV2FromEdge carrega um update V2 incremental da borda para o
// owner sem sobrecarregar o campo UpdateV1 de DocumentUpdate.
type DocumentUpdateV2FromEdge struct {
	Flags        Flags
	DocumentKey  storage.DocumentKey
	ConnectionID string
	Epoch        uint64
	UpdateV2     []byte
}

// Type retorna o message type associado ao payload.
func (m *DocumentUpdateV2FromEdge) Type() MessageType {
	return MessageTypeDocumentUpdateV2FromEdge
}

// FrameFlags retorna os bits auxiliares do header preservados por esta mensagem.
func (m *DocumentUpdateV2FromEdge) FrameFlags() Flags {
	if m == nil {
		return FlagNone
	}
	return m.Flags
}

// Validate confirma se o payload contém os campos mínimos exigidos pelo wire.
func (m *DocumentUpdateV2FromEdge) Validate() error {
	if m == nil {
		return ErrNilMessage
	}
	return validateRoutedBody(m.DocumentKey, m.ConnectionID, m.Epoch, m.UpdateV2)
}

func (m *DocumentUpdateV2FromEdge) appendPayload(dst []byte) ([]byte, error) {
	return appendRoutedPayload(dst, m.DocumentKey, m.ConnectionID, m.Epoch, m.UpdateV2)
}

// AwarenessUpdate carrega um delta bruto de awareness associado a uma conexão
// já roteada.
type AwarenessUpdate struct {
	Flags        Flags
	DocumentKey  storage.DocumentKey
	ConnectionID string
	Epoch        uint64
	Payload      []byte
}

// Type retorna o message type associado ao payload.
func (m *AwarenessUpdate) Type() MessageType {
	return MessageTypeAwarenessUpdate
}

// FrameFlags retorna os bits auxiliares do header preservados por esta mensagem.
func (m *AwarenessUpdate) FrameFlags() Flags {
	if m == nil {
		return FlagNone
	}
	return m.Flags
}

// Validate confirma se o payload contém os campos mínimos exigidos pelo wire.
func (m *AwarenessUpdate) Validate() error {
	if m == nil {
		return ErrNilMessage
	}
	return validateRoutedBody(m.DocumentKey, m.ConnectionID, m.Epoch, m.Payload)
}

func (m *AwarenessUpdate) appendPayload(dst []byte) ([]byte, error) {
	return appendRoutedPayload(dst, m.DocumentKey, m.ConnectionID, m.Epoch, m.Payload)
}

// QueryAwarenessRequest solicita ao owner o snapshot agregado de awareness
// para a conexão roteada.
type QueryAwarenessRequest struct {
	Flags        Flags
	DocumentKey  storage.DocumentKey
	ConnectionID string
	Epoch        uint64
}

// Type retorna o message type associado ao payload.
func (m *QueryAwarenessRequest) Type() MessageType {
	return MessageTypeQueryAwarenessRequest
}

// FrameFlags retorna os bits auxiliares do header preservados por esta mensagem.
func (m *QueryAwarenessRequest) FrameFlags() Flags {
	if m == nil {
		return FlagNone
	}
	return m.Flags
}

// Validate confirma se o payload contém os campos mínimos exigidos pelo wire.
func (m *QueryAwarenessRequest) Validate() error {
	if m == nil {
		return ErrNilMessage
	}
	return validateRoute(m.DocumentKey, m.ConnectionID, m.Epoch)
}

func (m *QueryAwarenessRequest) appendPayload(dst []byte) ([]byte, error) {
	return appendRoutedRoute(dst, m.DocumentKey, m.ConnectionID, m.Epoch)
}

// QueryAwarenessResponse entrega o snapshot awareness solicitado pelo peer.
type QueryAwarenessResponse struct {
	Flags        Flags
	DocumentKey  storage.DocumentKey
	ConnectionID string
	Epoch        uint64
	Payload      []byte
}

// Type retorna o message type associado ao payload.
func (m *QueryAwarenessResponse) Type() MessageType {
	return MessageTypeQueryAwarenessResponse
}

// FrameFlags retorna os bits auxiliares do header preservados por esta mensagem.
func (m *QueryAwarenessResponse) FrameFlags() Flags {
	if m == nil {
		return FlagNone
	}
	return m.Flags
}

// Validate confirma se o payload contém os campos mínimos exigidos pelo wire.
func (m *QueryAwarenessResponse) Validate() error {
	if m == nil {
		return ErrNilMessage
	}
	return validateRoutedBody(m.DocumentKey, m.ConnectionID, m.Epoch, m.Payload)
}

func (m *QueryAwarenessResponse) appendPayload(dst []byte) ([]byte, error) {
	return appendRoutedPayload(dst, m.DocumentKey, m.ConnectionID, m.Epoch, m.Payload)
}

// Disconnect notifica o owner que a borda perdeu a conexão do cliente e o
// contexto roteado precisa ser encerrado.
type Disconnect struct {
	Flags        Flags
	DocumentKey  storage.DocumentKey
	ConnectionID string
	Epoch        uint64
}

// Type retorna o message type associado ao payload.
func (m *Disconnect) Type() MessageType {
	return MessageTypeDisconnect
}

// FrameFlags retorna os bits auxiliares do header preservados por esta mensagem.
func (m *Disconnect) FrameFlags() Flags {
	if m == nil {
		return FlagNone
	}
	return m.Flags
}

// Validate confirma se o payload contém os campos mínimos exigidos pelo wire.
func (m *Disconnect) Validate() error {
	if m == nil {
		return ErrNilMessage
	}
	return validateRoute(m.DocumentKey, m.ConnectionID, m.Epoch)
}

func (m *Disconnect) appendPayload(dst []byte) ([]byte, error) {
	return appendRoutedRoute(dst, m.DocumentKey, m.ConnectionID, m.Epoch)
}

// Close instrui a borda a fechar explicitamente a conexão encaminhada.
type Close struct {
	Flags        Flags
	DocumentKey  storage.DocumentKey
	ConnectionID string
	Epoch        uint64
	Retryable    bool
	Reason       string
}

// Type retorna o message type associado ao payload.
func (m *Close) Type() MessageType {
	return MessageTypeClose
}

// FrameFlags retorna os bits auxiliares do header preservados por esta mensagem.
func (m *Close) FrameFlags() Flags {
	if m == nil {
		return FlagNone
	}
	flags := m.Flags &^ FlagCloseRetryable
	if m.Retryable {
		flags |= FlagCloseRetryable
	}
	return flags
}

// Validate confirma se o payload contém os campos mínimos exigidos pelo wire.
func (m *Close) Validate() error {
	if m == nil {
		return ErrNilMessage
	}
	return validateRoute(m.DocumentKey, m.ConnectionID, m.Epoch)
}

func (m *Close) appendPayload(dst []byte) ([]byte, error) {
	dst, err := appendRoutedRoute(dst, m.DocumentKey, m.ConnectionID, m.Epoch)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(m.Reason) == "" {
		return dst, nil
	}
	return appendString(dst, m.Reason)
}

// Ping carrega um nonce correlacionável para keepalive/latência.
type Ping struct {
	Flags Flags
	Nonce uint64
}

// Type retorna o message type associado ao payload.
func (m *Ping) Type() MessageType {
	return MessageTypePing
}

// FrameFlags retorna os bits auxiliares do header preservados por esta mensagem.
func (m *Ping) FrameFlags() Flags {
	if m == nil {
		return FlagNone
	}
	return m.Flags
}

// Validate confirma se o payload contém os campos mínimos exigidos pelo wire.
func (m *Ping) Validate() error {
	if m == nil {
		return ErrNilMessage
	}
	if m.Nonce == 0 {
		return ErrInvalidNonce
	}
	return nil
}

func (m *Ping) appendPayload(dst []byte) ([]byte, error) {
	return appendUint64(dst, m.Nonce), nil
}

// Pong responde a um ping previamente emitido e repete o nonce recebido.
type Pong struct {
	Flags Flags
	Nonce uint64
}

// Type retorna o message type associado ao payload.
func (m *Pong) Type() MessageType {
	return MessageTypePong
}

// FrameFlags retorna os bits auxiliares do header preservados por esta mensagem.
func (m *Pong) FrameFlags() Flags {
	if m == nil {
		return FlagNone
	}
	return m.Flags
}

// Validate confirma se o payload contém os campos mínimos exigidos pelo wire.
func (m *Pong) Validate() error {
	if m == nil {
		return ErrNilMessage
	}
	if m.Nonce == 0 {
		return ErrInvalidNonce
	}
	return nil
}

func (m *Pong) appendPayload(dst []byte) ([]byte, error) {
	return appendUint64(dst, m.Nonce), nil
}

// EncodeMessagePayload serializa apenas o payload tipado, sem o frame externo.
func EncodeMessagePayload(message Message) ([]byte, error) {
	if message == nil {
		return nil, ErrNilMessage
	}
	if err := message.Validate(); err != nil {
		return nil, err
	}
	return message.appendPayload(nil)
}

// NewMessageFrame constrói um frame validado a partir de uma mensagem tipada.
func NewMessageFrame(message Message) (*Frame, error) {
	payload, err := EncodeMessagePayload(message)
	if err != nil {
		return nil, err
	}
	return NewFrame(message.Type(), message.FrameFlags(), payload)
}

// EncodeMessageFrame serializa uma mensagem tipada como frame completo.
func EncodeMessageFrame(message Message) ([]byte, error) {
	payload, err := EncodeMessagePayload(message)
	if err != nil {
		return nil, err
	}
	header, err := NewHeader(message.Type(), message.FrameFlags(), len(payload))
	if err != nil {
		return nil, err
	}
	dst, err := AppendHeader(make([]byte, 0, HeaderSize+len(payload)), header)
	if err != nil {
		return nil, err
	}
	return append(dst, payload...), nil
}

// DecodeMessagePayload decodifica um payload tipado isolado para a mensagem
// concreta correspondente ao `typ` informado.
func DecodeMessagePayload(typ MessageType, flags Flags, payload []byte) (Message, error) {
	if !typ.Valid() {
		return nil, fmt.Errorf("%w: %d", ErrUnknownMessageType, uint8(typ))
	}

	readerValue := ybinary.NewReaderValue(payload)
	reader := &readerValue
	var (
		message Message
		err     error
	)

	switch typ {
	case MessageTypeHandshake:
		message, err = decodeHandshake(reader, flags)
	case MessageTypeHandshakeAck:
		message, err = decodeHandshakeAck(reader, flags)
	case MessageTypeDocumentSyncRequest:
		message, err = decodeDocumentSyncRequest(reader, flags)
	case MessageTypeDocumentSyncResponse:
		message, err = decodeDocumentSyncResponse(reader, flags)
	case MessageTypeDocumentUpdate:
		message, err = decodeDocumentUpdate(reader, flags)
	case MessageTypeDocumentSyncResponseV2:
		message, err = decodeDocumentSyncResponseV2(reader, flags)
	case MessageTypeDocumentUpdateV2:
		message, err = decodeDocumentUpdateV2(reader, flags)
	case MessageTypeDocumentSyncRequestV2:
		message, err = decodeDocumentSyncRequestV2(reader, flags)
	case MessageTypeDocumentUpdateV2FromEdge:
		message, err = decodeDocumentUpdateV2FromEdge(reader, flags)
	case MessageTypeAwarenessUpdate:
		message, err = decodeAwarenessUpdate(reader, flags)
	case MessageTypeQueryAwarenessRequest:
		message, err = decodeQueryAwarenessRequest(reader, flags)
	case MessageTypeQueryAwarenessResponse:
		message, err = decodeQueryAwarenessResponse(reader, flags)
	case MessageTypeDisconnect:
		message, err = decodeDisconnect(reader, flags)
	case MessageTypeClose:
		message, err = decodeClose(reader, flags)
	case MessageTypePing:
		message, err = decodePing(reader, flags)
	case MessageTypePong:
		message, err = decodePong(reader, flags)
	default:
		return nil, fmt.Errorf("%w: %d", ErrUnknownMessageType, uint8(typ))
	}
	if err != nil {
		return nil, err
	}
	if reader.Remaining() != 0 {
		return nil, wrapParseError("DecodeMessagePayload.trailing", reader.Offset(), ErrTrailingPayloadBytes)
	}
	return message, nil
}

// DecodeFrameMessage decodifica um frame já lido para a mensagem tipada contida
// em seu payload.
func DecodeFrameMessage(frame *Frame) (Message, error) {
	if err := frame.Validate(); err != nil {
		return nil, err
	}
	return DecodeMessagePayload(frame.Header.Type, frame.Header.Flags, frame.Payload)
}

// DecodeMessageFrame decodifica exatamente um frame e materializa a mensagem
// tipada contida nele.
func DecodeMessageFrame(src []byte) (Message, error) {
	frame, err := DecodeFrame(src)
	if err != nil {
		return nil, err
	}
	return DecodeFrameMessage(frame)
}

// DecodeMessageFrameView decodifica um frame completo sem copiar o payload do
// frame antes de materializar a mensagem tipada.
func DecodeMessageFrameView(src []byte) (Message, error) {
	frame, err := DecodeFrameView(src)
	if err != nil {
		return nil, err
	}
	return DecodeFrameMessage(frame)
}

func decodeHandshake(r *ybinary.Reader, flags Flags) (*Handshake, error) {
	nodeID, err := readNodeID(r, "ReadHandshake.nodeID")
	if err != nil {
		return nil, err
	}
	key, err := readDocumentKey(r, "ReadHandshake.documentKey")
	if err != nil {
		return nil, err
	}
	connectionID, err := readString(r, "ReadHandshake.connectionID")
	if err != nil {
		return nil, err
	}
	epoch, err := readUint64(r, "ReadHandshake.epoch")
	if err != nil {
		return nil, err
	}
	clientID, err := readOptionalUint32(r, "ReadHandshake.clientID")
	if err != nil {
		return nil, err
	}

	message := &Handshake{
		Flags:        flags,
		NodeID:       nodeID,
		DocumentKey:  key,
		ConnectionID: connectionID,
		ClientID:     clientID,
		Epoch:        epoch,
	}
	if err := message.Validate(); err != nil {
		return nil, wrapParseError("ReadHandshake.validate", r.Offset(), err)
	}
	return message, nil
}

func decodeHandshakeAck(r *ybinary.Reader, flags Flags) (*HandshakeAck, error) {
	nodeID, err := readNodeID(r, "ReadHandshakeAck.nodeID")
	if err != nil {
		return nil, err
	}
	key, err := readDocumentKey(r, "ReadHandshakeAck.documentKey")
	if err != nil {
		return nil, err
	}
	connectionID, err := readString(r, "ReadHandshakeAck.connectionID")
	if err != nil {
		return nil, err
	}
	epoch, err := readUint64(r, "ReadHandshakeAck.epoch")
	if err != nil {
		return nil, err
	}
	clientID, err := readOptionalUint32(r, "ReadHandshakeAck.clientID")
	if err != nil {
		return nil, err
	}

	message := &HandshakeAck{
		Flags:        flags,
		NodeID:       nodeID,
		DocumentKey:  key,
		ConnectionID: connectionID,
		ClientID:     clientID,
		Epoch:        epoch,
	}
	if err := message.Validate(); err != nil {
		return nil, wrapParseError("ReadHandshakeAck.validate", r.Offset(), err)
	}
	return message, nil
}

func decodeDocumentSyncRequest(r *ybinary.Reader, flags Flags) (*DocumentSyncRequest, error) {
	routed, err := readRoutedPayload(
		r,
		"ReadDocumentSyncRequest.documentKey",
		"ReadDocumentSyncRequest.connectionID",
		"ReadDocumentSyncRequest.epoch",
	)
	if err != nil {
		return nil, err
	}
	message := &DocumentSyncRequest{
		Flags:        flags,
		DocumentKey:  routed.DocumentKey,
		ConnectionID: routed.ConnectionID,
		Epoch:        routed.Epoch,
		StateVector:  routed.Body,
	}
	if err := message.Validate(); err != nil {
		return nil, wrapParseError("ReadDocumentSyncRequest.validate", r.Offset(), err)
	}
	return message, nil
}

func decodeDocumentSyncRequestV2(r *ybinary.Reader, flags Flags) (*DocumentSyncRequestV2, error) {
	routed, err := readRoutedPayload(
		r,
		"ReadDocumentSyncRequestV2.documentKey",
		"ReadDocumentSyncRequestV2.connectionID",
		"ReadDocumentSyncRequestV2.epoch",
	)
	if err != nil {
		return nil, err
	}
	message := &DocumentSyncRequestV2{
		Flags:        flags,
		DocumentKey:  routed.DocumentKey,
		ConnectionID: routed.ConnectionID,
		Epoch:        routed.Epoch,
		StateVector:  routed.Body,
	}
	if err := message.Validate(); err != nil {
		return nil, wrapParseError("ReadDocumentSyncRequestV2.validate", r.Offset(), err)
	}
	return message, nil
}

func decodeDocumentSyncResponse(r *ybinary.Reader, flags Flags) (*DocumentSyncResponse, error) {
	routed, err := readRoutedPayload(
		r,
		"ReadDocumentSyncResponse.documentKey",
		"ReadDocumentSyncResponse.connectionID",
		"ReadDocumentSyncResponse.epoch",
	)
	if err != nil {
		return nil, err
	}
	message := &DocumentSyncResponse{
		Flags:        flags,
		DocumentKey:  routed.DocumentKey,
		ConnectionID: routed.ConnectionID,
		Epoch:        routed.Epoch,
		UpdateV1:     routed.Body,
	}
	if err := message.Validate(); err != nil {
		return nil, wrapParseError("ReadDocumentSyncResponse.validate", r.Offset(), err)
	}
	return message, nil
}

func decodeDocumentSyncResponseV2(r *ybinary.Reader, flags Flags) (*DocumentSyncResponseV2, error) {
	routed, err := readRoutedPayload(
		r,
		"ReadDocumentSyncResponseV2.documentKey",
		"ReadDocumentSyncResponseV2.connectionID",
		"ReadDocumentSyncResponseV2.epoch",
	)
	if err != nil {
		return nil, err
	}
	message := &DocumentSyncResponseV2{
		Flags:        flags,
		DocumentKey:  routed.DocumentKey,
		ConnectionID: routed.ConnectionID,
		Epoch:        routed.Epoch,
		UpdateV2:     routed.Body,
	}
	if err := message.Validate(); err != nil {
		return nil, wrapParseError("ReadDocumentSyncResponseV2.validate", r.Offset(), err)
	}
	return message, nil
}

func decodeDocumentUpdate(r *ybinary.Reader, flags Flags) (*DocumentUpdate, error) {
	routed, err := readRoutedPayload(
		r,
		"ReadDocumentUpdate.documentKey",
		"ReadDocumentUpdate.connectionID",
		"ReadDocumentUpdate.epoch",
	)
	if err != nil {
		return nil, err
	}
	message := &DocumentUpdate{
		Flags:        flags,
		DocumentKey:  routed.DocumentKey,
		ConnectionID: routed.ConnectionID,
		Epoch:        routed.Epoch,
		UpdateV1:     routed.Body,
	}
	if err := message.Validate(); err != nil {
		return nil, wrapParseError("ReadDocumentUpdate.validate", r.Offset(), err)
	}
	return message, nil
}

func decodeDocumentUpdateV2(r *ybinary.Reader, flags Flags) (*DocumentUpdateV2, error) {
	routed, err := readRoutedPayload(
		r,
		"ReadDocumentUpdateV2.documentKey",
		"ReadDocumentUpdateV2.connectionID",
		"ReadDocumentUpdateV2.epoch",
	)
	if err != nil {
		return nil, err
	}
	message := &DocumentUpdateV2{
		Flags:        flags,
		DocumentKey:  routed.DocumentKey,
		ConnectionID: routed.ConnectionID,
		Epoch:        routed.Epoch,
		UpdateV2:     routed.Body,
	}
	if err := message.Validate(); err != nil {
		return nil, wrapParseError("ReadDocumentUpdateV2.validate", r.Offset(), err)
	}
	return message, nil
}

func decodeDocumentUpdateV2FromEdge(r *ybinary.Reader, flags Flags) (*DocumentUpdateV2FromEdge, error) {
	routed, err := readRoutedPayload(
		r,
		"ReadDocumentUpdateV2FromEdge.documentKey",
		"ReadDocumentUpdateV2FromEdge.connectionID",
		"ReadDocumentUpdateV2FromEdge.epoch",
	)
	if err != nil {
		return nil, err
	}
	message := &DocumentUpdateV2FromEdge{
		Flags:        flags,
		DocumentKey:  routed.DocumentKey,
		ConnectionID: routed.ConnectionID,
		Epoch:        routed.Epoch,
		UpdateV2:     routed.Body,
	}
	if err := message.Validate(); err != nil {
		return nil, wrapParseError("ReadDocumentUpdateV2FromEdge.validate", r.Offset(), err)
	}
	return message, nil
}

func decodeAwarenessUpdate(r *ybinary.Reader, flags Flags) (*AwarenessUpdate, error) {
	routed, err := readRoutedPayload(
		r,
		"ReadAwarenessUpdate.documentKey",
		"ReadAwarenessUpdate.connectionID",
		"ReadAwarenessUpdate.epoch",
	)
	if err != nil {
		return nil, err
	}
	message := &AwarenessUpdate{
		Flags:        flags,
		DocumentKey:  routed.DocumentKey,
		ConnectionID: routed.ConnectionID,
		Epoch:        routed.Epoch,
		Payload:      routed.Body,
	}
	if err := message.Validate(); err != nil {
		return nil, wrapParseError("ReadAwarenessUpdate.validate", r.Offset(), err)
	}
	return message, nil
}

func decodeQueryAwarenessRequest(r *ybinary.Reader, flags Flags) (*QueryAwarenessRequest, error) {
	route, err := readRoutedRoute(
		r,
		"ReadQueryAwarenessRequest.documentKey",
		"ReadQueryAwarenessRequest.connectionID",
		"ReadQueryAwarenessRequest.epoch",
	)
	if err != nil {
		return nil, err
	}
	message := &QueryAwarenessRequest{
		Flags:        flags,
		DocumentKey:  route.DocumentKey,
		ConnectionID: route.ConnectionID,
		Epoch:        route.Epoch,
	}
	if err := message.Validate(); err != nil {
		return nil, wrapParseError("ReadQueryAwarenessRequest.validate", r.Offset(), err)
	}
	return message, nil
}

func decodeQueryAwarenessResponse(r *ybinary.Reader, flags Flags) (*QueryAwarenessResponse, error) {
	routed, err := readRoutedPayload(
		r,
		"ReadQueryAwarenessResponse.documentKey",
		"ReadQueryAwarenessResponse.connectionID",
		"ReadQueryAwarenessResponse.epoch",
	)
	if err != nil {
		return nil, err
	}
	message := &QueryAwarenessResponse{
		Flags:        flags,
		DocumentKey:  routed.DocumentKey,
		ConnectionID: routed.ConnectionID,
		Epoch:        routed.Epoch,
		Payload:      routed.Body,
	}
	if err := message.Validate(); err != nil {
		return nil, wrapParseError("ReadQueryAwarenessResponse.validate", r.Offset(), err)
	}
	return message, nil
}

func decodeDisconnect(r *ybinary.Reader, flags Flags) (*Disconnect, error) {
	route, err := readRoutedRoute(
		r,
		"ReadDisconnect.documentKey",
		"ReadDisconnect.connectionID",
		"ReadDisconnect.epoch",
	)
	if err != nil {
		return nil, err
	}
	message := &Disconnect{
		Flags:        flags,
		DocumentKey:  route.DocumentKey,
		ConnectionID: route.ConnectionID,
		Epoch:        route.Epoch,
	}
	if err := message.Validate(); err != nil {
		return nil, wrapParseError("ReadDisconnect.validate", r.Offset(), err)
	}
	return message, nil
}

func decodeClose(r *ybinary.Reader, flags Flags) (*Close, error) {
	route, err := readRoutedRoute(
		r,
		"ReadClose.documentKey",
		"ReadClose.connectionID",
		"ReadClose.epoch",
	)
	if err != nil {
		return nil, err
	}

	reason := ""
	if r.Remaining() > 0 {
		reason, err = readString(r, "ReadClose.reason")
		if err != nil {
			return nil, err
		}
	}
	message := &Close{
		Flags:        flags,
		DocumentKey:  route.DocumentKey,
		ConnectionID: route.ConnectionID,
		Epoch:        route.Epoch,
		Retryable:    flags&FlagCloseRetryable != 0,
		Reason:       reason,
	}
	if err := message.Validate(); err != nil {
		return nil, wrapParseError("ReadClose.validate", r.Offset(), err)
	}
	return message, nil
}

func decodePing(r *ybinary.Reader, flags Flags) (*Ping, error) {
	nonce, err := readUint64(r, "ReadPing.nonce")
	if err != nil {
		return nil, err
	}
	message := &Ping{
		Flags: flags,
		Nonce: nonce,
	}
	if err := message.Validate(); err != nil {
		return nil, wrapParseError("ReadPing.validate", r.Offset(), err)
	}
	return message, nil
}

func decodePong(r *ybinary.Reader, flags Flags) (*Pong, error) {
	nonce, err := readUint64(r, "ReadPong.nonce")
	if err != nil {
		return nil, err
	}
	message := &Pong{
		Flags: flags,
		Nonce: nonce,
	}
	if err := message.Validate(); err != nil {
		return nil, wrapParseError("ReadPong.validate", r.Offset(), err)
	}
	return message, nil
}

func validateNodeRoute(nodeID string, key storage.DocumentKey, connectionID string, epoch uint64) error {
	if strings.TrimSpace(nodeID) == "" {
		return ErrInvalidNodeID
	}
	return validateRoute(key, connectionID, epoch)
}

func validateRoute(key storage.DocumentKey, connectionID string, epoch uint64) error {
	if err := key.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(connectionID) == "" {
		return ErrInvalidConnectionID
	}
	if epoch == 0 {
		return ErrInvalidEpoch
	}
	return nil
}

func validateRoutedBody(key storage.DocumentKey, connectionID string, epoch uint64, body []byte) error {
	if err := validateRoute(key, connectionID, epoch); err != nil {
		return err
	}
	if len(body) == 0 {
		return ErrMissingPayload
	}
	return nil
}

func appendRoutedPayload(dst []byte, key storage.DocumentKey, connectionID string, epoch uint64, body []byte) ([]byte, error) {
	dst, err := appendRoutedRoute(dst, key, connectionID, epoch)
	if err != nil {
		return nil, err
	}
	return append(dst, body...), nil
}

func appendRoutedRoute(dst []byte, key storage.DocumentKey, connectionID string, epoch uint64) ([]byte, error) {
	var err error

	dst, err = appendDocumentKey(dst, key)
	if err != nil {
		return nil, err
	}
	dst, err = appendString(dst, connectionID)
	if err != nil {
		return nil, err
	}
	return appendUint64(dst, epoch), nil
}

func appendDocumentKey(dst []byte, key storage.DocumentKey) ([]byte, error) {
	var err error

	dst, err = appendString(dst, key.Namespace)
	if err != nil {
		return nil, err
	}
	return appendString(dst, key.DocumentID)
}

func appendString(dst []byte, value string) ([]byte, error) {
	if uint64(len(value)) > maxHeaderPayloadLength {
		return nil, fmt.Errorf("%w: %d", ErrPayloadTooLarge, len(value))
	}
	dst = varint.Append(dst, uint32(len(value)))
	return append(dst, value...), nil
}

func appendUint64(dst []byte, value uint64) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, fixedUint64Size)...)
	binary.BigEndian.PutUint64(dst[start:start+fixedUint64Size], value)
	return dst
}

func appendUint32(dst []byte, value uint32) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, fixedUint32Size)...)
	binary.BigEndian.PutUint32(dst[start:start+fixedUint32Size], value)
	return dst
}

func readNodeID(r *ybinary.Reader, op string) (string, error) {
	start := r.Offset()
	nodeID, err := readString(r, op)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(nodeID) == "" {
		return "", wrapParseError(op, start, ErrInvalidNodeID)
	}
	return nodeID, nil
}

func readDocumentKey(r *ybinary.Reader, op string) (storage.DocumentKey, error) {
	start := r.Offset()
	namespace, err := readString(r, op+".namespace")
	if err != nil {
		return storage.DocumentKey{}, err
	}
	documentID, err := readString(r, op+".documentID")
	if err != nil {
		return storage.DocumentKey{}, err
	}

	key := storage.DocumentKey{
		Namespace:  namespace,
		DocumentID: documentID,
	}
	if err := key.Validate(); err != nil {
		return storage.DocumentKey{}, wrapParseError(op, start, err)
	}
	return key, nil
}

func readString(r *ybinary.Reader, op string) (string, error) {
	start := r.Offset()
	length, _, err := varint.Read(r)
	if err != nil {
		return "", wrapParseError(op+".len", start, err)
	}

	data, err := r.ReadN(int(length))
	if err != nil {
		return "", wrapParseError(op, r.Offset(), err)
	}
	return string(data), nil
}

func readUint64(r *ybinary.Reader, op string) (uint64, error) {
	start := r.Offset()
	raw, err := r.ReadN(fixedUint64Size)
	if err != nil {
		return 0, wrapParseError(op, start, err)
	}
	return binary.BigEndian.Uint64(raw), nil
}

func readUint32(r *ybinary.Reader, op string) (uint32, error) {
	start := r.Offset()
	raw, err := r.ReadN(fixedUint32Size)
	if err != nil {
		return 0, wrapParseError(op, start, err)
	}
	return binary.BigEndian.Uint32(raw), nil
}

func readOptionalUint32(r *ybinary.Reader, op string) (uint32, error) {
	if r.Remaining() == 0 {
		return 0, nil
	}
	return readUint32(r, op)
}

func readRoutedRoute(r *ybinary.Reader, keyOp string, connectionOp string, epochOp string) (*routedRoute, error) {
	key, err := readDocumentKey(r, keyOp)
	if err != nil {
		return nil, err
	}
	connectionID, err := readString(r, connectionOp)
	if err != nil {
		return nil, err
	}
	epoch, err := readUint64(r, epochOp)
	if err != nil {
		return nil, err
	}
	return &routedRoute{
		DocumentKey:  key,
		ConnectionID: connectionID,
		Epoch:        epoch,
	}, nil
}

func readRoutedPayload(r *ybinary.Reader, keyOp string, connectionOp string, epochOp string) (*routedPayload, error) {
	route, err := readRoutedRoute(r, keyOp, connectionOp, epochOp)
	if err != nil {
		return nil, err
	}
	body, err := readRemainingBytes(r)
	if err != nil {
		return nil, err
	}
	return &routedPayload{
		routedRoute: *route,
		Body:        body,
	}, nil
}

func readRemainingBytes(r *ybinary.Reader) ([]byte, error) {
	if r.Remaining() == 0 {
		return nil, nil
	}
	raw, err := r.ReadN(r.Remaining())
	if err != nil {
		return nil, wrapParseError("readRemainingBytes", r.Offset(), err)
	}
	return append([]byte(nil), raw...), nil
}

func wrapParseError(op string, offset int, err error) error {
	if err == nil {
		return nil
	}
	return &ParseError{Op: op, Offset: offset, Err: err}
}
