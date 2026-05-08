package yupdate

import "github.com/drksbr/yjs-crdt-golang-server/internal/ytypes"

type lazyWriterV1 struct {
	currClient    uint32
	written       uint32
	fragmentCount uint32
	current       []byte
	body          []byte
}

func newLazyWriterV1() *lazyWriterV1 {
	return &lazyWriterV1{}
}

func (w *lazyWriterV1) write(current ytypes.Struct, startOffset, endTrim uint32) error {
	if w.written > 0 && w.currClient != current.ID().Client {
		w.flush()
	}
	if w.written == 0 {
		startID, err := current.ID().Offset(startOffset)
		if err != nil {
			return err
		}
		w.currClient = current.ID().Client
		w.current = appendVarUintV1(w.current[:0], w.currClient)
		w.current = appendVarUintV1(w.current, startID.Clock)
	}

	var err error
	w.current, err = appendStructRangeV1(w.current, current, startOffset, endTrim)
	if err != nil {
		return err
	}
	w.written++
	return nil
}

func (w *lazyWriterV1) finish(dst []byte) ([]byte, error) {
	w.flush()

	dst = appendVarUintV1(dst, w.fragmentCount)
	dst = append(dst, w.body...)
	w.reset()
	return dst, nil
}

func (w *lazyWriterV1) flush() {
	if w.written == 0 {
		return
	}
	w.body = appendVarUintV1(w.body, w.written)
	w.body = append(w.body, w.current...)
	w.fragmentCount++
	w.current = w.current[:0]
	w.written = 0
}

func (w *lazyWriterV1) reset() {
	w.currClient = 0
	w.written = 0
	w.fragmentCount = 0
	w.current = w.current[:0]
	w.body = w.body[:0]
}

func appendStructRangeV1(dst []byte, current ytypes.Struct, startOffset, endTrim uint32) ([]byte, error) {
	if startOffset == 0 && endTrim == 0 {
		return appendStructV1(dst, current)
	}

	sliced, err := sliceStructWindowV1(current, startOffset, endTrim)
	if err != nil {
		return nil, err
	}
	return appendStructV1(dst, sliced)
}
