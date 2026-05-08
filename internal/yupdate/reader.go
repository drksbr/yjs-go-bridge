package yupdate

import "github.com/drksbr/yjs-crdt-golang-server/internal/ytypes"

// DecodedUpdate representa um update V1 completamente lido.
type DecodedUpdate struct {
	Structs   []ytypes.Struct
	DeleteSet *ytypes.DeleteSet
}

// LazyReaderV1 percorre structs do update sem materializar a lista inteira de uma vez.
type LazyReaderV1 struct {
	decoder           *decoderV1
	remainingClients  uint32
	remainingStructs  uint32
	currentClient     uint32
	currentClock      uint32
	current           ytypes.Struct
	filterSkips       bool
	structsExhausted  bool
	deleteSetConsumed bool
}

// NewLazyReaderV1 cria um leitor lazy já posicionado no primeiro struct, quando houver.
func NewLazyReaderV1(update []byte, filterSkips bool) (*LazyReaderV1, error) {
	decoder := newDecoderV1(update)
	count, err := decoder.readVarUint("ReadClientBlockCount")
	if err != nil {
		return nil, err
	}

	reader := &LazyReaderV1{
		decoder:          decoder,
		remainingClients: count,
		filterSkips:      filterSkips,
	}
	if err := reader.advance(); err != nil {
		return nil, err
	}
	return reader, nil
}

// Current retorna o struct atual ou nil quando o fluxo terminou.
func (r *LazyReaderV1) Current() ytypes.Struct {
	return r.current
}

// Next avança para o próximo struct.
func (r *LazyReaderV1) Next() error {
	return r.advance()
}

// ReadDeleteSet lê o bloco final de deleções após o término dos structs.
func (r *LazyReaderV1) ReadDeleteSet() (*ytypes.DeleteSet, error) {
	if !r.structsExhausted {
		return nil, ErrDeleteSetBeforeStructsEnd
	}
	if r.deleteSetConsumed {
		return nil, nil
	}

	ds, err := readDeleteSetV1(r.decoder)
	if err != nil {
		return nil, err
	}
	r.deleteSetConsumed = true

	if r.decoder.remaining() != 0 {
		return nil, wrapError("ReadDeleteSet.trailing", r.decoder.offset(), ErrTrailingBytes)
	}
	return ds, nil
}

// DecodeV1 materializa o update inteiro em memória.
func DecodeV1(update []byte) (*DecodedUpdate, error) {
	reader, err := NewLazyReaderV1(update, false)
	if err != nil {
		return nil, err
	}

	structs := make([]ytypes.Struct, 0)
	for current := reader.Current(); current != nil; current = reader.Current() {
		structs = append(structs, current)
		if err := reader.Next(); err != nil {
			return nil, err
		}
	}

	ds, err := reader.ReadDeleteSet()
	if err != nil {
		return nil, err
	}
	return &DecodedUpdate{Structs: structs, DeleteSet: ds}, nil
}

func (r *LazyReaderV1) advance() error {
	for {
		next, err := r.nextStruct()
		if err != nil {
			return err
		}

		if next == nil || !r.filterSkips || next.Kind() != ytypes.KindSkip {
			r.current = next
			return nil
		}
	}
}

func (r *LazyReaderV1) nextStruct() (ytypes.Struct, error) {
	for r.remainingStructs == 0 {
		if r.remainingClients == 0 {
			r.structsExhausted = true
			return nil, nil
		}

		count, err := r.decoder.readVarUint("ReadStructCount")
		if err != nil {
			return nil, err
		}
		client, err := r.decoder.readVarUint("ReadClient")
		if err != nil {
			return nil, err
		}
		clock, err := r.decoder.readVarUint("ReadStartClock")
		if err != nil {
			return nil, err
		}

		r.remainingClients--
		r.remainingStructs = count
		r.currentClient = client
		r.currentClock = clock
	}

	info, err := r.decoder.readInfo()
	if err != nil {
		return nil, err
	}
	r.remainingStructs--

	switch {
	case info == 10:
		length, err := r.decoder.readVarUint("ReadSkipLength")
		if err != nil {
			return nil, err
		}
		id := ytypes.ID{Client: r.currentClient, Clock: r.currentClock}
		skip, err := ytypes.NewSkip(id, length)
		if err != nil {
			return nil, err
		}
		r.currentClock += length
		return skip, nil
	case info&ytypes.ItemContentRefMask != 0:
		return r.readItem(info)
	default:
		length, err := r.decoder.readVarUint("ReadGCLength")
		if err != nil {
			return nil, err
		}
		id := ytypes.ID{Client: r.currentClient, Clock: r.currentClock}
		gc, err := ytypes.NewGC(id, length)
		if err != nil {
			return nil, err
		}
		r.currentClock += length
		return gc, nil
	}
}

func (r *LazyReaderV1) readItem(info byte) (ytypes.Struct, error) {
	id := ytypes.ID{Client: r.currentClient, Clock: r.currentClock}

	var origin *ytypes.ID
	if info&ytypes.ItemHasOrigin != 0 {
		readID, err := r.decoder.readID("ReadOrigin")
		if err != nil {
			return nil, err
		}
		origin = &readID
	}

	var rightOrigin *ytypes.ID
	if info&ytypes.ItemHasRightOrigin != 0 {
		readID, err := r.decoder.readID("ReadRightOrigin")
		if err != nil {
			return nil, err
		}
		rightOrigin = &readID
	}

	parent := ytypes.ParentRef{}
	parentSub := ""
	if info&(ytypes.ItemHasOrigin|ytypes.ItemHasRightOrigin) == 0 {
		isRoot, err := r.decoder.readParentInfo()
		if err != nil {
			return nil, err
		}
		if isRoot {
			name, err := r.decoder.readString("ReadParentRoot")
			if err != nil {
				return nil, err
			}
			parent, err = ytypes.NewParentRoot(name)
			if err != nil {
				return nil, err
			}
		} else {
			parentID, err := r.decoder.readID("ReadParentID")
			if err != nil {
				return nil, err
			}
			parent = ytypes.NewParentID(parentID)
		}

		if info&ytypes.ItemHasParentSub != 0 {
			value, err := r.decoder.readString("ReadParentSub")
			if err != nil {
				return nil, err
			}
			parentSub = value
		}
	}

	content, err := readItemContentV1(r.decoder, info)
	if err != nil {
		return nil, err
	}

	item, err := ytypes.NewItemOwnedIDs(id, content, ytypes.ItemOptions{
		Origin:      origin,
		RightOrigin: rightOrigin,
		Parent:      parent,
		ParentSub:   parentSub,
	})
	if err != nil {
		return nil, err
	}

	r.currentClock += item.Length()
	return item, nil
}
