package yupdate

import (
	"bytes"
	"fmt"
	"unicode/utf16"

	"github.com/drksbr/yjs-crdt-golang-server/internal/varint"
)

func (c ParsedContent) AppendV1(dst []byte) ([]byte, error) {
	if c.Raw == nil {
		return nil, fmt.Errorf("%w: ref=%d", ErrUnsupportedContentEncode, c.Ref)
	}
	return append(dst, c.Raw...), nil
}

func (c ParsedContent) Slice(diff uint32) (ParsedContent, error) {
	return c.SliceWindow(diff, 0)
}

func (c ParsedContent) SliceWindow(startOffset, endTrim uint32) (ParsedContent, error) {
	length := c.LengthVal
	if uint64(startOffset)+uint64(endTrim) >= uint64(length) {
		return ParsedContent{}, ErrInvalidSliceOffset
	}
	if startOffset == 0 && endTrim == 0 {
		return c.clone(), nil
	}

	newLen := length - startOffset - endTrim
	switch c.Ref {
	case itemContentDeleted:
		next := c.clone()
		next.LengthVal = newLen
		next.Raw = appendVarUintV1(nil, next.LengthVal)
		return next, nil
	case itemContentJSON:
		next := c.clone()
		start := int(startOffset)
		end := start + int(newLen)
		next.JSON = append([]string(nil), c.JSON[start:end]...)
		next.LengthVal = uint32(len(next.JSON))
		next.Raw = encodeJSONStringArray(next.JSON)
		return next, nil
	case itemContentString:
		text, err := sliceUTF16Window(c.Text, startOffset, newLen)
		if err != nil {
			return ParsedContent{}, err
		}
		next := c.clone()
		next.Text = text
		next.LengthVal = utf16Length(text)
		next.Raw = appendVarStringV1(nil, text)
		return next, nil
	case itemContentAny:
		next := c.clone()
		start := int(startOffset)
		end := start + int(newLen)
		next.Any = cloneRawSlices(c.Any[start:end])
		next.LengthVal = uint32(len(next.Any))
		next.Raw = encodeAnyArray(next.Any)
		return next, nil
	case itemContentBinary, itemContentEmbed, itemContentFormat, itemContentType, itemContentDoc:
		return ParsedContent{}, fmt.Errorf("%w: ref=%d", ErrUnsupportedContentSlice, c.Ref)
	default:
		return ParsedContent{}, fmt.Errorf("%w: ref=%d", ErrUnsupportedContentSlice, c.Ref)
	}
}

func (c ParsedContent) clone() ParsedContent {
	return ParsedContent{
		Ref:       c.Ref,
		LengthVal: c.LengthVal,
		Countable: c.Countable,
		TypeRef:   c.TypeRef,
		TypeName:  c.TypeName,
		Raw:       bytes.Clone(c.Raw),
		Text:      c.Text,
		JSON:      append([]string(nil), c.JSON...),
		Any:       cloneRawSlices(c.Any),
	}
}

func cloneRawSlices(src [][]byte) [][]byte {
	if len(src) == 0 {
		return nil
	}
	out := make([][]byte, 0, len(src))
	for _, item := range src {
		out = append(out, bytes.Clone(item))
	}
	return out
}

func encodeJSONStringArray(values []string) []byte {
	dst := appendVarUintV1(nil, uint32(len(values)))
	for _, value := range values {
		dst = appendVarStringV1(dst, value)
	}
	return dst
}

func encodeAnyArray(values [][]byte) []byte {
	dst := appendVarUintV1(nil, uint32(len(values)))
	for _, value := range values {
		dst = append(dst, value...)
	}
	return dst
}

func sliceUTF16Window(s string, start, length uint32) (string, error) {
	units := utf16.Encode([]rune(s))
	end := uint64(start) + uint64(length)
	if end > uint64(len(units)) {
		return "", ErrInvalidSliceOffset
	}
	window := units[start:end]
	if len(window) == 0 {
		return "", nil
	}
	// O Yjs fatia strings em unidades UTF-16. Quando o corte cai no meio de um
	// surrogate pair, o encode subsequente acaba produzindo U+FFFD nas bordas.
	// `utf16.Decode` reproduz isso ao converter pares inválidos em replacement
	// character, preservando o comprimento estrutural em unidades UTF-16.
	return string(utf16.Decode(window)), nil
}

func appendVarUintV1(dst []byte, value uint32) []byte {
	return varint.Append(dst, value)
}

func appendVarStringV1(dst []byte, value string) []byte {
	dst = varint.Append(dst, uint32(len(value)))
	return append(dst, value...)
}
