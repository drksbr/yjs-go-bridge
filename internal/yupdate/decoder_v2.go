package yupdate

import (
	"fmt"
	"math"

	ybinary "github.com/drksbr/yjs-crdt-golang-server/internal/binary"
	"github.com/drksbr/yjs-crdt-golang-server/internal/varint"
	"github.com/drksbr/yjs-crdt-golang-server/internal/ytypes"
)

type decoderV2 struct {
	rest       ybinary.Reader
	keyClock   *intDiffOptRleDecoder
	client     *uintOptRleDecoder
	leftClock  *intDiffOptRleDecoder
	rightClock *intDiffOptRleDecoder
	info       *rleByteDecoder
	strings    *stringDecoderV2
	parentInfo *rleByteDecoder
	typeRef    *uintOptRleDecoder
	lengths    *uintOptRleDecoder
	keys       []string
	dsCurrVal  uint32
}

func newDecoderV2(data []byte) (*decoderV2, error) {
	reader := ybinary.NewReaderValue(data)
	featureFlag, err := readLib0VarUint(&reader, "ReadV2FeatureFlag")
	if err != nil {
		return nil, err
	}
	if featureFlag != 0 {
		return nil, unsupportedV2FeatureFlag(featureFlag)
	}

	keyClockData, err := readLib0VarUint8ArrayView(&reader, "ReadV2KeyClockEncoder")
	if err != nil {
		return nil, err
	}
	clientData, err := readLib0VarUint8ArrayView(&reader, "ReadV2ClientEncoder")
	if err != nil {
		return nil, err
	}
	leftClockData, err := readLib0VarUint8ArrayView(&reader, "ReadV2LeftClockEncoder")
	if err != nil {
		return nil, err
	}
	rightClockData, err := readLib0VarUint8ArrayView(&reader, "ReadV2RightClockEncoder")
	if err != nil {
		return nil, err
	}
	infoData, err := readLib0VarUint8ArrayView(&reader, "ReadV2InfoEncoder")
	if err != nil {
		return nil, err
	}
	stringData, err := readLib0VarUint8ArrayView(&reader, "ReadV2StringEncoder")
	if err != nil {
		return nil, err
	}
	parentInfoData, err := readLib0VarUint8ArrayView(&reader, "ReadV2ParentInfoEncoder")
	if err != nil {
		return nil, err
	}
	typeRefData, err := readLib0VarUint8ArrayView(&reader, "ReadV2TypeRefEncoder")
	if err != nil {
		return nil, err
	}
	lengthData, err := readLib0VarUint8ArrayView(&reader, "ReadV2LenEncoder")
	if err != nil {
		return nil, err
	}

	restData, err := reader.ReadN(reader.Remaining())
	if err != nil {
		return nil, wrapError("ReadV2Rest", reader.Offset(), err)
	}
	strings, err := newStringDecoderV2(stringData, "ReadV2StringEncoder")
	if err != nil {
		return nil, err
	}

	return &decoderV2{
		rest:       ybinary.NewReaderValue(restData),
		keyClock:   newIntDiffOptRleDecoder(keyClockData, "ReadV2KeyClock"),
		client:     newUintOptRleDecoder(clientData, "ReadV2Client"),
		leftClock:  newIntDiffOptRleDecoder(leftClockData, "ReadV2LeftClock"),
		rightClock: newIntDiffOptRleDecoder(rightClockData, "ReadV2RightClock"),
		info:       newRleByteDecoder(infoData, "ReadV2Info"),
		strings:    strings,
		parentInfo: newRleByteDecoder(parentInfoData, "ReadV2ParentInfo"),
		typeRef:    newUintOptRleDecoder(typeRefData, "ReadV2TypeRef"),
		lengths:    newUintOptRleDecoder(lengthData, "ReadV2Len"),
	}, nil
}

func (d *decoderV2) offset() int {
	return d.rest.Offset()
}

func (d *decoderV2) remaining() int {
	return d.rest.Remaining()
}

func (d *decoderV2) ensureDrained() error {
	if err := d.keyClock.ensureDrained(); err != nil {
		return err
	}
	if err := d.client.ensureDrained(); err != nil {
		return err
	}
	if err := d.leftClock.ensureDrained(); err != nil {
		return err
	}
	if err := d.rightClock.ensureDrained(); err != nil {
		return err
	}
	if err := d.info.ensureDrained(); err != nil {
		return err
	}
	if err := d.strings.ensureDrained(); err != nil {
		return err
	}
	if err := d.parentInfo.ensureDrained(); err != nil {
		return err
	}
	if err := d.typeRef.ensureDrained(); err != nil {
		return err
	}
	if err := d.lengths.ensureDrained(); err != nil {
		return err
	}
	return nil
}

func (d *decoderV2) readRestVarUint(op string) (uint32, error) {
	return readLib0VarUint(&d.rest, op)
}

func (d *decoderV2) readClient() (uint32, error) {
	return d.client.read()
}

func (d *decoderV2) readInfo() (byte, error) {
	return d.info.read()
}

func (d *decoderV2) readString() (string, error) {
	return d.strings.read()
}

func (d *decoderV2) readParentInfo() (bool, error) {
	value, err := d.parentInfo.read()
	if err != nil {
		return false, err
	}
	switch value {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, wrapError("ReadV2ParentInfo.value", d.parentInfo.reader.Offset(), fmt.Errorf("parent info invalido: %d", value))
	}
}

func (d *decoderV2) readTypeRef() (uint32, error) {
	return d.typeRef.read()
}

func (d *decoderV2) readLen() (uint32, error) {
	return d.lengths.read()
}

func (d *decoderV2) readLeftID() (ytypes.ID, error) {
	client, err := d.readClient()
	if err != nil {
		return ytypes.ID{}, err
	}
	clock, err := d.leftClock.read()
	if err != nil {
		return ytypes.ID{}, err
	}
	return ytypes.ID{Client: client, Clock: clock}, nil
}

func (d *decoderV2) readRightID() (ytypes.ID, error) {
	client, err := d.readClient()
	if err != nil {
		return ytypes.ID{}, err
	}
	clock, err := d.rightClock.read()
	if err != nil {
		return ytypes.ID{}, err
	}
	return ytypes.ID{Client: client, Clock: clock}, nil
}

func (d *decoderV2) readKey() (string, error) {
	keyClock, err := d.keyClock.read()
	if err != nil {
		return "", err
	}
	if uint64(keyClock) < uint64(len(d.keys)) {
		return d.keys[keyClock], nil
	}

	key, err := d.readString()
	if err != nil {
		return "", err
	}
	d.keys = append(d.keys, key)
	return key, nil
}

func (d *decoderV2) readBuf(op string) ([]byte, error) {
	return readLib0VarUint8ArrayView(&d.rest, op)
}

func (d *decoderV2) readVarString(op string) (string, error) {
	return readLib0VarString(&d.rest, op)
}

func (d *decoderV2) readVarIntRaw(op string) ([]byte, error) {
	return readLib0VarIntRaw(&d.rest, op)
}

func (d *decoderV2) readAnyRaw(op string) ([]byte, error) {
	start := d.offset()
	tag, err := d.rest.ReadByte()
	if err != nil {
		return nil, wrapError(op+".tag", start, err)
	}
	return d.readAnyRawWithTag(op, start, tag)
}

func (d *decoderV2) readAnyRawWithTag(op string, start int, tag byte) ([]byte, error) {
	raw := []byte{tag}
	switch tag {
	case 127, 126, 121, 120:
		return raw, nil
	case 125:
		value, err := d.readVarIntRaw(op + ".varint")
		if err != nil {
			return nil, err
		}
		return append(raw, value...), nil
	case 124:
		value, err := readLib0BigEndian(&d.rest, op+".float32", 4)
		if err != nil {
			return nil, err
		}
		return append(raw, value...), nil
	case 123, 122:
		value, err := readLib0BigEndian(&d.rest, op+".float64", 8)
		if err != nil {
			return nil, err
		}
		return append(raw, value...), nil
	case 119:
		value, err := d.readVarString(op + ".string")
		if err != nil {
			return nil, err
		}
		return appendLib0VarString(raw, value), nil
	case 118:
		length, err := d.readRestVarUint(op + ".object.len")
		if err != nil {
			return nil, err
		}
		if err := validateDecodedCollectionLength(op+".object.len", d.offset(), length); err != nil {
			return nil, err
		}
		raw = appendVarUintV1(raw, length)
		for i := uint32(0); i < length; i++ {
			key, err := d.readVarString(op + ".object.key")
			if err != nil {
				return nil, err
			}
			raw = appendLib0VarString(raw, key)
			value, err := d.readAnyRaw(op + ".object.value")
			if err != nil {
				return nil, err
			}
			raw = append(raw, value...)
		}
		return raw, nil
	case 117:
		length, err := d.readRestVarUint(op + ".array.len")
		if err != nil {
			return nil, err
		}
		if err := validateDecodedCollectionLength(op+".array.len", d.offset(), length); err != nil {
			return nil, err
		}
		raw = appendVarUintV1(raw, length)
		for i := uint32(0); i < length; i++ {
			value, err := d.readAnyRaw(op + ".array.value")
			if err != nil {
				return nil, err
			}
			raw = append(raw, value...)
		}
		return raw, nil
	case 116:
		value, err := d.readBuf(op + ".buf")
		if err != nil {
			return nil, err
		}
		return appendLib0VarUint8Array(raw, value), nil
	default:
		return nil, wrapError(op, start, fmt.Errorf("tag any desconhecida: %d", tag))
	}
}

func (d *decoderV2) readJSONRawCompat(op string) ([]byte, error) {
	start := d.offset()
	first, err := d.rest.ReadByte()
	if err != nil {
		return nil, wrapError(op+".tag", start, err)
	}

	if isLib0AnyTag(first) {
		return d.readAnyRawWithTag(op+".any", start, first)
	}
	return d.readVarStringRawWithFirst(op+".json", start, first)
}

func (d *decoderV2) readVarStringRawWithFirst(op string, start int, first byte) ([]byte, error) {
	rawLen := []byte{first}
	value := uint32(first & 0x7f)
	shift := uint(7)
	i := 0
	last := first

	for last&0x80 != 0 {
		i++
		if i >= 5 {
			return nil, wrapError(op+".len", start, varint.ErrOverflow)
		}
		next, err := d.rest.ReadByte()
		if err != nil {
			return nil, wrapError(op+".len", start, varint.ErrUnexpectedEOF)
		}
		rawLen = append(rawLen, next)
		last = next

		if i == 4 {
			if next&0x80 != 0 || next > 0x0f {
				return nil, wrapError(op+".len", start, varint.ErrOverflow)
			}
		}
		value |= uint32(next&0x7f) << shift
		shift += 7
	}

	if encodedVarUintLen(value) != len(rawLen) {
		return nil, wrapError(op+".len", start, varint.ErrNonCanonical)
	}

	payload, err := d.rest.ReadN(int(value))
	if err != nil {
		return nil, wrapError(op, d.offset(), err)
	}
	raw := append([]byte{}, rawLen...)
	raw = append(raw, payload...)
	return raw, nil
}

func (d *decoderV2) readDSClock() (uint32, error) {
	diff, err := d.readRestVarUint("ReadV2DSClock")
	if err != nil {
		return 0, err
	}
	next := uint64(d.dsCurrVal) + uint64(diff)
	if next > math.MaxUint32 {
		return 0, wrapError("ReadV2DSClock", d.offset(), varint.ErrOverflow)
	}
	d.dsCurrVal = uint32(next)
	return d.dsCurrVal, nil
}

func (d *decoderV2) readDSLen() (uint32, error) {
	encoded, err := d.readRestVarUint("ReadV2DSLen")
	if err != nil {
		return 0, err
	}
	length := uint64(encoded) + 1
	next := uint64(d.dsCurrVal) + length
	if length > math.MaxUint32 || next > math.MaxUint32 {
		return 0, wrapError("ReadV2DSLen", d.offset(), varint.ErrOverflow)
	}
	d.dsCurrVal = uint32(next)
	return uint32(length), nil
}
