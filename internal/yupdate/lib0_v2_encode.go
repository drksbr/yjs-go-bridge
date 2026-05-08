package yupdate

import "strings"

type uintOptRleEncoder struct {
	value uint32
	count int
	data  []byte
}

func (e *uintOptRleEncoder) write(value uint32) {
	if e.count > 0 && e.value == value {
		e.count++
		return
	}
	e.flush()
	e.value = value
	e.count = 1
}

func (e *uintOptRleEncoder) bytes() []byte {
	e.flush()
	return e.data
}

func (e *uintOptRleEncoder) flush() {
	if e.count == 0 {
		return
	}
	e.data = appendLib0VarIntSigned(e.data, int64(e.value), e.count > 1)
	if e.count > 1 {
		e.data = appendVarUintV1(e.data, uint32(e.count-2))
	}
	e.count = 0
}

type intDiffOptRleEncoder struct {
	value int64
	diff  int64
	count int
	data  []byte
}

func (e *intDiffOptRleEncoder) write(value uint32) {
	next := int64(value)
	diff := next - e.value
	if e.count > 0 && e.diff == diff {
		e.value = next
		e.count++
		return
	}
	e.flush()
	e.value = next
	e.diff = diff
	e.count = 1
}

func (e *intDiffOptRleEncoder) bytes() []byte {
	e.flush()
	return e.data
}

func (e *intDiffOptRleEncoder) flush() {
	if e.count == 0 {
		return
	}
	encodedDiff := e.diff * 2
	if e.count > 1 {
		encodedDiff++
	}
	e.data = appendLib0VarInt(e.data, encodedDiff)
	if e.count > 1 {
		e.data = appendVarUintV1(e.data, uint32(e.count-2))
	}
	e.count = 0
}

type rleByteEncoder struct {
	value byte
	count int
	data  []byte
}

func (e *rleByteEncoder) write(value byte) {
	if e.count > 0 && e.value == value {
		e.count++
		return
	}
	if e.count > 0 {
		e.data = appendVarUintV1(e.data, uint32(e.count-1))
	}
	e.value = value
	e.count = 1
	e.data = append(e.data, value)
}

func (e *rleByteEncoder) bytes() []byte {
	return e.data
}

type stringEncoderV2 struct {
	table   strings.Builder
	lengths uintOptRleEncoder
}

func (e *stringEncoderV2) write(value string) {
	e.table.WriteString(value)
	e.lengths.write(utf16Length(value))
}

func (e *stringEncoderV2) bytes() []byte {
	out := appendLib0VarString(nil, e.table.String())
	return append(out, e.lengths.bytes()...)
}

func appendLib0VarInt(dst []byte, value int64) []byte {
	negative := value < 0
	if negative {
		value = -value
	}
	return appendLib0VarIntSigned(dst, value, negative)
}

func appendLib0VarIntSigned(dst []byte, value int64, negative bool) []byte {
	first := byte(value & 0x3f)
	value /= 64
	if negative {
		first |= 0x40
	}
	if value > 0 {
		first |= 0x80
	}
	dst = append(dst, first)
	for value > 0 {
		next := byte(value & 0x7f)
		value /= 128
		if value > 0 {
			next |= 0x80
		}
		dst = append(dst, next)
	}
	return dst
}
