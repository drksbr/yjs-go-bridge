package yupdate

import (
	"fmt"
	"slices"

	"github.com/drksbr/yjs-crdt-golang-server/internal/ytypes"
)

type updateEncoderV2 struct {
	rest       []byte
	keyClock   intDiffOptRleEncoder
	client     uintOptRleEncoder
	leftClock  intDiffOptRleEncoder
	rightClock intDiffOptRleEncoder
	info       rleByteEncoder
	strings    stringEncoderV2
	parentInfo rleByteEncoder
	typeRef    uintOptRleEncoder
	lengths    uintOptRleEncoder
	keyClockN  uint32
}

// EncodeV2 serializa um update materializado no wire format compacto do Yjs V2.
func EncodeV2(decoded *DecodedUpdate) ([]byte, error) {
	if decoded == nil {
		decoded = &DecodedUpdate{DeleteSet: ytypes.NewDeleteSet()}
	}
	encoder := &updateEncoderV2{}
	if err := encoder.writeStructGroups(groupStructsByClient(decoded.Structs)); err != nil {
		return nil, err
	}
	encoder.writeDeleteSet(decoded.DeleteSet)
	return encoder.bytes(), nil
}

func encodeEmptyUpdateV2() []byte {
	encoded, err := EncodeV2(nil)
	if err != nil {
		return nil
	}
	return encoded
}

func (e *updateEncoderV2) writeStructGroups(groups map[uint32][]ytypes.Struct) error {
	clients := make([]uint32, 0, len(groups))
	for client := range groups {
		if len(groups[client]) != 0 {
			clients = append(clients, client)
		}
	}
	slices.SortFunc(clients, func(a, b uint32) int {
		switch {
		case a > b:
			return -1
		case a < b:
			return 1
		default:
			return 0
		}
	})

	e.rest = appendVarUintV1(e.rest, uint32(len(clients)))
	for _, client := range clients {
		structs := groups[client]
		startClock := structs[0].ID().Clock
		e.rest = appendVarUintV1(e.rest, uint32(len(structs)))
		e.client.write(client)
		e.rest = appendVarUintV1(e.rest, startClock)
		for _, current := range structs {
			if err := e.writeStruct(current); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *updateEncoderV2) writeStruct(current ytypes.Struct) error {
	switch value := current.(type) {
	case ytypes.GC:
		e.info.write(0)
		e.lengths.write(value.Length())
		return nil
	case ytypes.Skip:
		e.info.write(10)
		e.rest = appendVarUintV1(e.rest, value.Length())
		return nil
	case *ytypes.Item:
		return e.writeItem(value)
	default:
		return fmt.Errorf("yupdate: struct nao suportada para encode v2: %T", current)
	}
}

func (e *updateEncoderV2) writeItem(item *ytypes.Item) error {
	content, ok := item.Content.(ParsedContent)
	if !ok {
		return fmt.Errorf("%w: %T", ErrUnsupportedContentEncode, item.Content)
	}

	header := ytypes.ItemHeader{
		ContentRef:     content.ContentRef(),
		HasOrigin:      item.Origin != nil,
		HasRightOrigin: item.RightOrigin != nil,
		HasParentSub:   item.Origin == nil && item.RightOrigin == nil && item.ParentSub != "",
	}
	info, err := header.Encode()
	if err != nil {
		return err
	}
	e.info.write(info)

	if item.Origin != nil {
		e.writeLeftID(*item.Origin)
	}
	if item.RightOrigin != nil {
		e.writeRightID(*item.RightOrigin)
	}
	if item.Origin == nil && item.RightOrigin == nil {
		switch item.Parent.Kind() {
		case ytypes.ParentRoot:
			e.parentInfo.write(1)
			e.strings.write(item.Parent.Root())
		case ytypes.ParentID:
			parentID, _ := item.Parent.ID()
			e.parentInfo.write(0)
			e.writeLeftID(parentID)
		default:
			return fmt.Errorf("yupdate: item sem parent wire")
		}
		if item.ParentSub != "" {
			e.strings.write(item.ParentSub)
		}
	}
	return e.writeContent(content)
}

func (e *updateEncoderV2) writeLeftID(id ytypes.ID) {
	e.client.write(id.Client)
	e.leftClock.write(id.Clock)
}

func (e *updateEncoderV2) writeRightID(id ytypes.ID) {
	e.client.write(id.Client)
	e.rightClock.write(id.Clock)
}

func (e *updateEncoderV2) writeKey(key string) {
	e.keyClock.write(e.keyClockN)
	e.keyClockN++
	e.strings.write(key)
}

func (e *updateEncoderV2) writeContent(content ParsedContent) error {
	switch content.Ref {
	case itemContentDeleted:
		e.lengths.write(content.LengthVal)
	case itemContentJSON:
		e.lengths.write(content.LengthVal)
		for _, value := range content.JSON {
			e.strings.write(value)
		}
	case itemContentBinary:
		if content.Raw == nil {
			return fmt.Errorf("%w: ref=%d", ErrUnsupportedContentEncode, content.Ref)
		}
		e.rest = append(e.rest, content.Raw...)
	case itemContentString:
		e.strings.write(content.Text)
	case itemContentEmbed:
		if content.Raw == nil {
			return fmt.Errorf("%w: ref=%d", ErrUnsupportedContentEncode, content.Ref)
		}
		e.rest = append(e.rest, content.Raw...)
	case itemContentFormat:
		if content.Raw == nil {
			return fmt.Errorf("%w: ref=%d", ErrUnsupportedContentEncode, content.Ref)
		}
		e.writeKey(content.TypeName)
		e.rest = append(e.rest, content.Raw[len(appendVarStringV1(nil, content.TypeName)):]...)
	case itemContentType:
		e.typeRef.write(content.TypeRef)
		switch content.TypeRef {
		case typeRefYXmlElement, typeRefYXmlHook:
			e.writeKey(content.TypeName)
		}
	case itemContentAny:
		e.lengths.write(content.LengthVal)
		for _, value := range content.Any {
			e.rest = append(e.rest, value...)
		}
	case itemContentDoc:
		if content.Raw == nil {
			return fmt.Errorf("%w: ref=%d", ErrUnsupportedContentEncode, content.Ref)
		}
		e.strings.write(content.TypeName)
		e.rest = append(e.rest, content.Raw[len(appendVarStringV1(nil, content.TypeName)):]...)
	default:
		return fmt.Errorf("%w: ref=%d", ErrUnsupportedContentEncode, content.Ref)
	}
	return nil
}

func (e *updateEncoderV2) writeDeleteSet(ds *ytypes.DeleteSet) {
	e.rest = appendVarUintV1(e.rest, uint32(ds.ClientCount()))
	ds.ForEachClientDesc(func(client uint32, ranges []ytypes.DeleteRange) {
		e.rest = appendVarUintV1(e.rest, client)
		e.rest = appendVarUintV1(e.rest, uint32(len(ranges)))
		current := uint32(0)
		for _, r := range ranges {
			e.rest = appendVarUintV1(e.rest, r.Clock-current)
			e.rest = appendVarUintV1(e.rest, r.Length-1)
			current = r.Clock + r.Length
		}
	})
}

func (e *updateEncoderV2) bytes() []byte {
	out := appendVarUintV1(nil, 0)
	out = appendLib0VarUint8Array(out, e.keyClock.bytes())
	out = appendLib0VarUint8Array(out, e.client.bytes())
	out = appendLib0VarUint8Array(out, e.leftClock.bytes())
	out = appendLib0VarUint8Array(out, e.rightClock.bytes())
	out = appendLib0VarUint8Array(out, e.info.bytes())
	out = appendLib0VarUint8Array(out, e.strings.bytes())
	out = appendLib0VarUint8Array(out, e.parentInfo.bytes())
	out = appendLib0VarUint8Array(out, e.typeRef.bytes())
	out = appendLib0VarUint8Array(out, e.lengths.bytes())
	return append(out, e.rest...)
}
