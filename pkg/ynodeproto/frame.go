package ynodeproto

import (
	"encoding/binary"
	"fmt"
)

const (
	maxHeaderPayloadLength = uint64(^uint32(0))
	maxIntValue            = int(^uint(0) >> 1)
)

// Header representa o prefixo fixo de um frame inter-node.
type Header struct {
	Version       uint8
	Type          MessageType
	Flags         Flags
	PayloadLength uint32
}

// Validate garante que o header é suportado pelo codec atual.
func (h Header) Validate() error {
	if h.Version != CurrentVersion {
		return fmt.Errorf("%w: %d", ErrUnsupportedVersion, h.Version)
	}
	if !h.Type.Valid() {
		return fmt.Errorf("%w: %d", ErrUnknownMessageType, uint8(h.Type))
	}
	return nil
}

// NewHeader cria um header v1 validado a partir do tamanho do payload.
func NewHeader(typ MessageType, flags Flags, payloadLength int) (Header, error) {
	if payloadLength < 0 {
		return Header{}, ErrInvalidPayloadLength
	}
	if uint64(payloadLength) > maxHeaderPayloadLength {
		return Header{}, fmt.Errorf("%w: %d", ErrPayloadTooLarge, payloadLength)
	}

	header := Header{
		Version:       CurrentVersion,
		Type:          typ,
		Flags:         flags,
		PayloadLength: uint32(payloadLength),
	}
	if err := header.Validate(); err != nil {
		return Header{}, err
	}
	return header, nil
}

// AppendHeader serializa um header válido no final de dst.
func AppendHeader(dst []byte, header Header) ([]byte, error) {
	if err := header.Validate(); err != nil {
		return dst, err
	}

	start := len(dst)
	dst = append(dst, make([]byte, HeaderSize)...)
	dst[start] = header.Version
	dst[start+1] = byte(header.Type)
	binary.BigEndian.PutUint16(dst[start+2:start+4], uint16(header.Flags))
	binary.BigEndian.PutUint32(dst[start+4:start+8], header.PayloadLength)
	return dst, nil
}

// EncodeHeader retorna um buffer novo contendo o header serializado.
func EncodeHeader(header Header) ([]byte, error) {
	return AppendHeader(make([]byte, 0, HeaderSize), header)
}

// DecodeHeader lê e valida um header v1 a partir do prefixo de src.
func DecodeHeader(src []byte) (Header, error) {
	if len(src) < HeaderSize {
		return Header{}, fmt.Errorf("%w: precisa=%d recebeu=%d", ErrIncompleteHeader, HeaderSize, len(src))
	}

	header := Header{
		Version:       src[0],
		Type:          MessageType(src[1]),
		Flags:         Flags(binary.BigEndian.Uint16(src[2:4])),
		PayloadLength: binary.BigEndian.Uint32(src[4:8]),
	}
	if err := header.Validate(); err != nil {
		return Header{}, err
	}
	return header, nil
}

// Frame representa uma mensagem inter-node com header e payload bruto.
type Frame struct {
	Header  Header
	Payload []byte
}

// NewFrame cria um frame validado e desacoplado do slice de payload de entrada.
func NewFrame(typ MessageType, flags Flags, payload []byte) (*Frame, error) {
	header, err := NewHeader(typ, flags, len(payload))
	if err != nil {
		return nil, err
	}

	return &Frame{
		Header:  header,
		Payload: append([]byte(nil), payload...),
	}, nil
}

// Validate garante coerência interna entre header e payload.
func (f *Frame) Validate() error {
	if f == nil {
		return ErrNilFrame
	}
	if err := f.Header.Validate(); err != nil {
		return err
	}
	if uint64(len(f.Payload)) > maxHeaderPayloadLength {
		return fmt.Errorf("%w: %d", ErrPayloadTooLarge, len(f.Payload))
	}
	if f.Header.PayloadLength != uint32(len(f.Payload)) {
		return fmt.Errorf(
			"%w: header=%d payload=%d",
			ErrPayloadLengthMismatch,
			f.Header.PayloadLength,
			len(f.Payload),
		)
	}
	return nil
}

// AppendFrame serializa um frame completo no final de dst.
func AppendFrame(dst []byte, frame *Frame) ([]byte, error) {
	if err := frame.Validate(); err != nil {
		return dst, err
	}

	dst, err := AppendHeader(dst, frame.Header)
	if err != nil {
		return dst, err
	}
	dst = append(dst, frame.Payload...)
	return dst, nil
}

// EncodeFrame retorna um buffer novo contendo um frame completo.
func EncodeFrame(frame *Frame) ([]byte, error) {
	if frame == nil {
		return nil, ErrNilFrame
	}
	return AppendFrame(make([]byte, 0, HeaderSize+len(frame.Payload)), frame)
}

// DecodeFramePrefix decodifica o primeiro frame completo em src.
// O retorno consumed informa quantos bytes pertencem ao frame lido.
func DecodeFramePrefix(src []byte) (*Frame, int, error) {
	return decodeFramePrefix(src, true)
}

// DecodeFramePrefixView decodifica o primeiro frame completo sem copiar o
// payload. O frame retornado referencia src; use apenas quando o caller controla
// a vida útil e imutabilidade do buffer.
func DecodeFramePrefixView(src []byte) (*Frame, int, error) {
	return decodeFramePrefix(src, false)
}

func decodeFramePrefix(src []byte, copyPayload bool) (*Frame, int, error) {
	header, err := DecodeHeader(src)
	if err != nil {
		return nil, 0, err
	}

	totalSize, err := encodedFrameSize(header)
	if err != nil {
		return nil, 0, err
	}
	if len(src) < totalSize {
		return nil, 0, fmt.Errorf(
			"%w: precisa=%d recebeu=%d",
			ErrIncompletePayload,
			header.PayloadLength,
			len(src)-HeaderSize,
		)
	}

	payload := src[HeaderSize:totalSize]
	if copyPayload {
		payload = append([]byte(nil), payload...)
	}
	frame := &Frame{
		Header:  header,
		Payload: payload,
	}
	if err := frame.Validate(); err != nil {
		return nil, 0, err
	}
	return frame, totalSize, nil
}

// DecodeFrame decodifica exatamente um frame e rejeita bytes extras.
func DecodeFrame(src []byte) (*Frame, error) {
	frame, consumed, err := DecodeFramePrefix(src)
	if err != nil {
		return nil, err
	}
	if consumed != len(src) {
		return nil, fmt.Errorf("%w: consumiu=%d total=%d", ErrTrailingBytes, consumed, len(src))
	}
	return frame, nil
}

// DecodeFrameView decodifica exatamente um frame sem copiar o payload.
func DecodeFrameView(src []byte) (*Frame, error) {
	frame, consumed, err := DecodeFramePrefixView(src)
	if err != nil {
		return nil, err
	}
	if consumed != len(src) {
		return nil, fmt.Errorf(
			"%w: consumido=%d total=%d",
			ErrTrailingBytes,
			consumed,
			len(src),
		)
	}
	return frame, nil
}

func encodedFrameSize(header Header) (int, error) {
	if uint64(header.PayloadLength) > uint64(maxIntValue-HeaderSize) {
		return 0, fmt.Errorf("%w: %d", ErrPayloadTooLarge, header.PayloadLength)
	}
	return HeaderSize + int(header.PayloadLength), nil
}
