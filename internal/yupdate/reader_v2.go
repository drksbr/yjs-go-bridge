package yupdate

import (
	"fmt"

	"github.com/drksbr/yjs-crdt-golang-server/internal/ytypes"
)

// DecodeV2 materializa um update Yjs V2 no mesmo modelo interno usado pelo
// decoder V1. As APIs públicas V2 continuam emitindo bytes V1 canônicos em vez
// de preservar o wire format V2 na saída.
func DecodeV2(update []byte) (*DecodedUpdate, error) {
	decoder, err := newDecoderV2(update)
	if err != nil {
		return nil, err
	}

	clientBlocks, err := decoder.readRestVarUint("ReadV2ClientBlockCount")
	if err != nil {
		return nil, err
	}

	structs := make([]ytypes.Struct, 0)
	for i := uint32(0); i < clientBlocks; i++ {
		count, err := decoder.readRestVarUint("ReadV2StructCount")
		if err != nil {
			return nil, err
		}
		client, err := decoder.readClient()
		if err != nil {
			return nil, err
		}
		clock, err := decoder.readRestVarUint("ReadV2StartClock")
		if err != nil {
			return nil, err
		}

		for j := uint32(0); j < count; j++ {
			info, err := decoder.readInfo()
			if err != nil {
				return nil, err
			}

			next, err := readStructV2(decoder, info, client, clock)
			if err != nil {
				return nil, err
			}
			structs = append(structs, next)
			clock += next.Length()
		}
	}

	ds, err := readDeleteSetV2(decoder)
	if err != nil {
		return nil, err
	}
	if decoder.remaining() != 0 {
		return nil, wrapError("ReadV2DeleteSet.trailing", decoder.offset(), ErrTrailingBytes)
	}
	if err := decoder.ensureDrained(); err != nil {
		return nil, err
	}

	return &DecodedUpdate{Structs: structs, DeleteSet: ds}, nil
}

func readStructV2(decoder *decoderV2, info byte, client, clock uint32) (ytypes.Struct, error) {
	id := ytypes.ID{Client: client, Clock: clock}

	switch {
	case info == 10:
		length, err := decoder.readRestVarUint("ReadV2SkipLength")
		if err != nil {
			return nil, err
		}
		return ytypes.NewSkip(id, length)
	case info&ytypes.ItemContentRefMask != 0:
		return readItemV2(decoder, info, id)
	default:
		length, err := decoder.readLen()
		if err != nil {
			return nil, err
		}
		return ytypes.NewGC(id, length)
	}
}

func readItemV2(decoder *decoderV2, info byte, id ytypes.ID) (ytypes.Struct, error) {
	var origin *ytypes.ID
	if info&ytypes.ItemHasOrigin != 0 {
		readID, err := decoder.readLeftID()
		if err != nil {
			return nil, err
		}
		origin = &readID
	}

	var rightOrigin *ytypes.ID
	if info&ytypes.ItemHasRightOrigin != 0 {
		readID, err := decoder.readRightID()
		if err != nil {
			return nil, err
		}
		rightOrigin = &readID
	}

	parent := ytypes.ParentRef{}
	parentSub := ""
	if info&(ytypes.ItemHasOrigin|ytypes.ItemHasRightOrigin) == 0 {
		isRoot, err := decoder.readParentInfo()
		if err != nil {
			return nil, err
		}
		if isRoot {
			name, err := decoder.readString()
			if err != nil {
				return nil, err
			}
			parent, err = ytypes.NewParentRoot(name)
			if err != nil {
				return nil, err
			}
		} else {
			parentID, err := decoder.readLeftID()
			if err != nil {
				return nil, err
			}
			parent = ytypes.NewParentID(parentID)
		}

		if info&ytypes.ItemHasParentSub != 0 {
			value, err := decoder.readString()
			if err != nil {
				return nil, err
			}
			parentSub = value
		}
	}

	content, err := readItemContentV2(decoder, info)
	if err != nil {
		return nil, err
	}

	return ytypes.NewItemOwnedIDs(id, content, ytypes.ItemOptions{
		Origin:      origin,
		RightOrigin: rightOrigin,
		Parent:      parent,
		ParentSub:   parentSub,
	})
}

func readItemContentV2(decoder *decoderV2, info byte) (ParsedContent, error) {
	switch info & ytypes.ItemContentRefMask {
	case itemContentDeleted:
		length, err := decoder.readLen()
		return ParsedContent{
			Ref:       itemContentDeleted,
			LengthVal: length,
			Countable: false,
			Raw:       appendVarUintV1(nil, length),
		}, err
	case itemContentJSON:
		length, err := decoder.readLen()
		if err != nil {
			return ParsedContent{}, err
		}
		if err := validateDecodedCollectionLength("ReadV2ContentJSON.length", decoder.offset(), length); err != nil {
			return ParsedContent{}, err
		}
		values := make([]string, 0, length)
		raw := appendVarUintV1(nil, length)
		for i := uint32(0); i < length; i++ {
			value, err := decoder.readString()
			if err != nil {
				return ParsedContent{}, err
			}
			values = append(values, value)
			raw = appendVarStringV1(raw, value)
		}
		return ParsedContent{Ref: itemContentJSON, LengthVal: length, Countable: true, Raw: raw, JSON: values}, nil
	case itemContentBinary:
		buf, err := decoder.readBuf("ReadV2ContentBinary")
		if err != nil {
			return ParsedContent{}, err
		}
		raw := appendVarUintV1(nil, uint32(len(buf)))
		raw = append(raw, buf...)
		return ParsedContent{Ref: itemContentBinary, LengthVal: 1, Countable: true, Raw: raw}, nil
	case itemContentString:
		value, err := decoder.readString()
		if err != nil {
			return ParsedContent{}, err
		}
		return ParsedContent{
			Ref:       itemContentString,
			LengthVal: utf16Length(value),
			Countable: true,
			Raw:       appendVarStringV1(nil, value),
			Text:      value,
		}, nil
	case itemContentEmbed:
		raw, err := decoder.readJSONRawCompat("ReadV2ContentEmbed")
		if err != nil {
			return ParsedContent{}, err
		}
		return ParsedContent{Ref: itemContentEmbed, LengthVal: 1, Countable: true, Raw: raw}, nil
	case itemContentFormat:
		key, err := decoder.readKey()
		if err != nil {
			return ParsedContent{}, err
		}
		valueRaw, err := decoder.readJSONRawCompat("ReadV2ContentFormat.value")
		if err != nil {
			return ParsedContent{}, err
		}
		raw := appendVarStringV1(nil, key)
		raw = append(raw, valueRaw...)
		return ParsedContent{Ref: itemContentFormat, LengthVal: 1, Countable: false, TypeName: key, Raw: raw}, nil
	case itemContentType:
		typeRef, err := decoder.readTypeRef()
		if err != nil {
			return ParsedContent{}, err
		}
		typeName, err := readTypePayloadV2(decoder, typeRef)
		if err != nil {
			return ParsedContent{}, err
		}
		raw := appendVarUintV1(nil, typeRef)
		switch typeRef {
		case typeRefYXmlElement, typeRefYXmlHook:
			raw = appendVarStringV1(raw, typeName)
		}
		return ParsedContent{Ref: itemContentType, LengthVal: 1, Countable: true, TypeRef: typeRef, TypeName: typeName, Raw: raw}, nil
	case itemContentAny:
		length, err := decoder.readLen()
		if err != nil {
			return ParsedContent{}, err
		}
		if err := validateDecodedCollectionLength("ReadV2ContentAny.length", decoder.offset(), length); err != nil {
			return ParsedContent{}, err
		}
		values := make([][]byte, 0, length)
		raw := appendVarUintV1(nil, length)
		for i := uint32(0); i < length; i++ {
			valueRaw, err := decoder.readAnyRaw("ReadV2ContentAny.value")
			if err != nil {
				return ParsedContent{}, err
			}
			values = append(values, valueRaw)
			raw = append(raw, valueRaw...)
		}
		return ParsedContent{Ref: itemContentAny, LengthVal: length, Countable: true, Raw: raw, Any: values}, nil
	case itemContentDoc:
		name, err := decoder.readString()
		if err != nil {
			return ParsedContent{}, err
		}
		optsRaw, err := decoder.readAnyRaw("ReadV2ContentDoc.opts")
		if err != nil {
			return ParsedContent{}, err
		}
		raw := appendVarStringV1(nil, name)
		raw = append(raw, optsRaw...)
		return ParsedContent{Ref: itemContentDoc, LengthVal: 1, Countable: true, TypeName: name, Raw: raw}, nil
	default:
		return ParsedContent{}, wrapError("ReadV2ItemContent", decoder.offset(), fmt.Errorf("%w: %d", ErrUnknownContentRef, info&ytypes.ItemContentRefMask))
	}
}

func readTypePayloadV2(decoder *decoderV2, typeRef uint32) (string, error) {
	switch typeRef {
	case typeRefYArray, typeRefYMap, typeRefYText, typeRefYXmlFragment, typeRefYXmlText:
		return "", nil
	case typeRefYXmlElement, typeRefYXmlHook:
		return decoder.readKey()
	default:
		return "", wrapError("ReadV2TypePayload", decoder.offset(), fmt.Errorf("%w: %d", ErrUnknownTypeRef, typeRef))
	}
}

func readDeleteSetV2(decoder *decoderV2) (*ytypes.DeleteSet, error) {
	count, err := decoder.readRestVarUint("ReadV2DeleteSet.clients")
	if err != nil {
		return nil, err
	}

	ds := ytypes.NewDeleteSet()
	for i := uint32(0); i < count; i++ {
		decoder.dsCurrVal = 0
		client, err := decoder.readRestVarUint("ReadV2DeleteSet.client")
		if err != nil {
			return nil, err
		}
		rangeCount, err := decoder.readRestVarUint("ReadV2DeleteSet.rangeCount")
		if err != nil {
			return nil, err
		}

		for j := uint32(0); j < rangeCount; j++ {
			clock, err := decoder.readDSClock()
			if err != nil {
				return nil, err
			}
			length, err := decoder.readDSLen()
			if err != nil {
				return nil, err
			}
			if err := ds.Add(client, clock, length); err != nil {
				return nil, fmt.Errorf("yupdate: adicionar range deletado v2: %w", err)
			}
		}
	}

	return ds, nil
}
