package yupdate

import (
	"fmt"
	"math"
	"unicode/utf16"
	"unicode/utf8"

	ybinary "github.com/drksbr/yjs-crdt-golang-server/internal/binary"
	"github.com/drksbr/yjs-crdt-golang-server/internal/varint"
)

type uintOptRleDecoder struct {
	reader ybinary.Reader
	value  uint32
	count  int64
	op     string
}

func newUintOptRleDecoder(data []byte, op string) *uintOptRleDecoder {
	return &uintOptRleDecoder{reader: ybinary.NewReaderValue(data), op: op}
}

func (d *uintOptRleDecoder) read() (uint32, error) {
	if d.count == 0 {
		value, negative, err := readLib0VarInt(&d.reader, d.op+".value")
		if err != nil {
			return 0, err
		}
		if value < 0 {
			value = -value
		}
		if value > math.MaxUint32 {
			return 0, wrapError(d.op+".value", d.reader.Offset(), varint.ErrOverflow)
		}

		d.value = uint32(value)
		d.count = 1
		if negative {
			count, err := readLib0VarUint(&d.reader, d.op+".count")
			if err != nil {
				return 0, err
			}
			d.count = int64(count) + 2
		}
	}

	d.count--
	return d.value, nil
}

func (d *uintOptRleDecoder) ensureDrained() error {
	if d == nil {
		return nil
	}
	if d.count > 0 || d.reader.Remaining() != 0 {
		return wrapError(d.op+".trailing", d.reader.Offset(), ErrTrailingBytes)
	}
	return nil
}

type intDiffOptRleDecoder struct {
	reader ybinary.Reader
	value  int64
	diff   int64
	count  int64
	op     string
}

func newIntDiffOptRleDecoder(data []byte, op string) *intDiffOptRleDecoder {
	return &intDiffOptRleDecoder{reader: ybinary.NewReaderValue(data), op: op}
}

func (d *intDiffOptRleDecoder) read() (uint32, error) {
	if d.count == 0 {
		encodedDiff, _, err := readLib0VarInt(&d.reader, d.op+".diff")
		if err != nil {
			return 0, err
		}

		hasCount := encodedDiff&1 != 0
		d.diff = floorDiv2(encodedDiff)
		d.count = 1
		if hasCount {
			count, err := readLib0VarUint(&d.reader, d.op+".count")
			if err != nil {
				return 0, err
			}
			d.count = int64(count) + 2
		}
	}

	d.value += d.diff
	d.count--
	if d.value < 0 || d.value > math.MaxUint32 {
		return 0, wrapError(d.op+".value", d.reader.Offset(), varint.ErrOverflow)
	}
	return uint32(d.value), nil
}

func (d *intDiffOptRleDecoder) ensureDrained() error {
	if d == nil {
		return nil
	}
	if d.count > 0 || d.reader.Remaining() != 0 {
		return wrapError(d.op+".trailing", d.reader.Offset(), ErrTrailingBytes)
	}
	return nil
}

type rleByteDecoder struct {
	reader ybinary.Reader
	value  byte
	count  int64
	op     string
}

func newRleByteDecoder(data []byte, op string) *rleByteDecoder {
	return &rleByteDecoder{reader: ybinary.NewReaderValue(data), op: op}
}

func (d *rleByteDecoder) read() (byte, error) {
	if d.count == 0 {
		value, err := d.reader.ReadByte()
		if err != nil {
			return 0, wrapError(d.op+".value", d.reader.Offset(), err)
		}
		d.value = value
		if d.reader.Remaining() > 0 {
			count, err := readLib0VarUint(&d.reader, d.op+".count")
			if err != nil {
				return 0, err
			}
			d.count = int64(count) + 1
		} else {
			d.count = -1
		}
	}

	d.count--
	return d.value, nil
}

func (d *rleByteDecoder) ensureDrained() error {
	if d == nil {
		return nil
	}
	if d.count > 0 || d.reader.Remaining() != 0 {
		return wrapError(d.op+".trailing", d.reader.Offset(), ErrTrailingBytes)
	}
	return nil
}

type stringDecoderV2 struct {
	lengths *uintOptRleDecoder
	table   string
	units   []uint16
	pos     int
	op      string
}

func newStringDecoderV2(data []byte, op string) (*stringDecoderV2, error) {
	reader := ybinary.NewReaderValue(data)
	value, err := readLib0VarString(&reader, op+".table")
	if err != nil {
		return nil, err
	}

	remaining, err := reader.ReadN(reader.Remaining())
	if err != nil {
		return nil, wrapError(op+".lengths", reader.Offset(), err)
	}
	decoder := &stringDecoderV2{
		lengths: newUintOptRleDecoder(remaining, op+".lengths"),
		table:   value,
		op:      op,
	}
	if !isASCIIString(value) {
		decoder.units = encodeUTF16Units(value)
	}
	return decoder, nil
}

func (d *stringDecoderV2) read() (string, error) {
	length, err := d.lengths.read()
	if err != nil {
		return "", err
	}

	end := d.pos + int(length)
	if end < d.pos || end > d.utf16Length() {
		return "", wrapError(d.op+".slice", d.pos, varint.ErrUnexpectedEOF)
	}

	var value string
	if d.units == nil {
		value = d.table[d.pos:end]
	} else {
		value = string(utf16.Decode(d.units[d.pos:end]))
	}
	d.pos = end
	return value, nil
}

func (d *stringDecoderV2) ensureDrained() error {
	if d == nil {
		return nil
	}
	if d.pos != d.utf16Length() {
		return wrapError(d.op+".trailing", d.pos, ErrTrailingBytes)
	}
	return d.lengths.ensureDrained()
}

func (d *stringDecoderV2) utf16Length() int {
	if d.units == nil {
		return len(d.table)
	}
	return len(d.units)
}

func isASCIIString(value string) bool {
	for idx := 0; idx < len(value); idx++ {
		if value[idx] >= utf8.RuneSelf {
			return false
		}
	}
	return true
}

func encodeUTF16Units(value string) []uint16 {
	units := make([]uint16, 0, utf16Length(value))
	for _, current := range value {
		switch {
		case current <= 0xffff:
			units = append(units, uint16(current))
		default:
			r1, r2 := utf16.EncodeRune(current)
			units = append(units, uint16(r1), uint16(r2))
		}
	}
	return units
}

func floorDiv2(value int64) int64 {
	quotient := value / 2
	if value < 0 && value%2 != 0 {
		quotient--
	}
	return quotient
}

func readLib0VarUint(reader *ybinary.Reader, op string) (uint32, error) {
	value, _, err := varint.Read(reader)
	if err != nil {
		return 0, wrapError(op, reader.Offset(), err)
	}
	return value, nil
}

func readLib0VarInt(reader *ybinary.Reader, op string) (value int64, negative bool, err error) {
	first, err := reader.ReadByte()
	if err != nil {
		return 0, false, wrapError(op, reader.Offset(), varint.ErrUnexpectedEOF)
	}

	negative = first&0x40 != 0
	value = int64(first & 0x3f)
	mult := int64(64)
	if first&0x80 == 0 {
		if negative {
			value = -value
		}
		return value, negative, nil
	}

	for i := 0; i < 9; i++ {
		b, err := reader.ReadByte()
		if err != nil {
			return 0, false, wrapError(op, reader.Offset(), varint.ErrUnexpectedEOF)
		}
		if mult > math.MaxInt64/128 {
			return 0, false, wrapError(op, reader.Offset(), varint.ErrOverflow)
		}

		next := int64(b & 0x7f)
		if next > (math.MaxInt64-value)/mult {
			return 0, false, wrapError(op, reader.Offset(), varint.ErrOverflow)
		}
		value += next * mult
		if b&0x80 == 0 {
			if negative {
				value = -value
			}
			return value, negative, nil
		}
		mult *= 128
	}

	return 0, false, wrapError(op, reader.Offset(), varint.ErrOverflow)
}

func readLib0VarString(reader *ybinary.Reader, op string) (string, error) {
	length, err := readLib0VarUint(reader, op+".len")
	if err != nil {
		return "", err
	}

	data, err := reader.ReadN(int(length))
	if err != nil {
		return "", wrapError(op, reader.Offset(), err)
	}
	return string(data), nil
}

func readLib0VarUint8Array(reader *ybinary.Reader, op string) ([]byte, error) {
	data, err := readLib0VarUint8ArrayView(reader, op)
	if err != nil {
		return nil, err
	}
	copied := make([]byte, len(data))
	copy(copied, data)
	return copied, nil
}

func readLib0VarUint8ArrayView(reader *ybinary.Reader, op string) ([]byte, error) {
	length, err := readLib0VarUint(reader, op+".len")
	if err != nil {
		return nil, err
	}

	data, err := reader.ReadN(int(length))
	if err != nil {
		return nil, wrapError(op, reader.Offset(), err)
	}
	return data, nil
}

func readLib0VarIntRaw(reader *ybinary.Reader, op string) ([]byte, error) {
	raw := make([]byte, 0, 10)
	for i := 0; i < 10; i++ {
		b, err := reader.ReadByte()
		if err != nil {
			return nil, wrapError(op, reader.Offset(), varint.ErrUnexpectedEOF)
		}
		raw = append(raw, b)
		if b&0x80 == 0 {
			return raw, nil
		}
	}
	return nil, wrapError(op, reader.Offset(), varint.ErrOverflow)
}

func readLib0BigEndian(reader *ybinary.Reader, op string, size int) ([]byte, error) {
	data, err := reader.ReadN(size)
	if err != nil {
		return nil, wrapError(op, reader.Offset(), err)
	}
	return append([]byte(nil), data...), nil
}

func appendLib0VarString(dst []byte, value string) []byte {
	return appendVarStringV1(dst, value)
}

func appendLib0VarUint8Array(dst []byte, value []byte) []byte {
	dst = appendVarUintV1(dst, uint32(len(value)))
	return append(dst, value...)
}

func unsupportedV2FeatureFlag(flag uint32) error {
	return fmt.Errorf("%w: feature flag %d", ErrUnsupportedUpdateFormatV2, flag)
}
