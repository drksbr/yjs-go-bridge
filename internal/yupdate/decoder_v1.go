package yupdate

import (
	"fmt"

	ybinary "github.com/drksbr/yjs-crdt-golang-server/internal/binary"
	"github.com/drksbr/yjs-crdt-golang-server/internal/varint"
	"github.com/drksbr/yjs-crdt-golang-server/internal/ytypes"
)

type decoderV1 struct {
	reader ybinary.Reader
}

func newDecoderV1(data []byte) *decoderV1 {
	return &decoderV1{reader: ybinary.NewReaderValue(data)}
}

func (d *decoderV1) offset() int {
	return d.reader.Offset()
}

func (d *decoderV1) remaining() int {
	return d.reader.Remaining()
}

func (d *decoderV1) readVarUint(op string) (uint32, error) {
	value, _, err := varint.Read(&d.reader)
	if err != nil {
		return 0, wrapError(op, d.offset(), err)
	}
	return value, nil
}

func (d *decoderV1) readInfo() (byte, error) {
	value, err := d.reader.ReadByte()
	if err != nil {
		return 0, wrapError("ReadInfo", d.offset(), err)
	}
	return value, nil
}

func (d *decoderV1) readID(op string) (ytypes.ID, error) {
	id, _, err := ytypes.ReadID(&d.reader)
	if err != nil {
		return ytypes.ID{}, wrapError(op, d.offset(), err)
	}
	return id, nil
}

func (d *decoderV1) readString(op string) (string, error) {
	length, err := d.readVarUint(op + ".len")
	if err != nil {
		return "", err
	}

	data, err := d.reader.ReadN(int(length))
	if err != nil {
		return "", wrapError(op, d.offset(), err)
	}
	return string(data), nil
}

func (d *decoderV1) readBuf(op string) ([]byte, error) {
	length, err := d.readVarUint(op + ".len")
	if err != nil {
		return nil, err
	}

	buf, err := d.reader.ReadN(int(length))
	if err != nil {
		return nil, wrapError(op, d.offset(), err)
	}
	return buf, nil
}

func (d *decoderV1) readParentInfo() (bool, error) {
	value, err := d.readVarUint("ReadParentInfo")
	if err != nil {
		return false, err
	}
	switch value {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, wrapError("ReadParentInfo.value", d.offset(), fmt.Errorf("parent info invalido: %d", value))
	}
}

func utf16Length(s string) uint32 {
	var length uint32
	for _, r := range s {
		if r <= 0xffff {
			length++
			continue
		}
		length += 2
	}
	return length
}
