package yupdate

import (
	"github.com/drksbr/yjs-crdt-golang-server/internal/varint"
	"github.com/drksbr/yjs-crdt-golang-server/internal/ytypes"
)

// EncodeStateVectorFromUpdateV1 reproduz a lógica do Yjs para updates V1.
// O state vector só avança por cliente enquanto os clocks formam uma sequência
// contínua iniciada em zero; `Skip` interrompe a contagem para aquele cliente.
func EncodeStateVectorFromUpdateV1(update []byte) ([]byte, error) {
	stateVector, err := stateVectorFromUpdateV1(update)
	if err != nil {
		return nil, err
	}
	return encodeStateVectorMap(stateVector), nil
}

func stateVectorFromUpdateV1(update []byte) (map[uint32]uint32, error) {
	reader, err := NewLazyReaderV1(update, false)
	if err != nil {
		return nil, err
	}

	current := reader.Current()
	if current == nil {
		return map[uint32]uint32{}, nil
	}

	stateVector := make(map[uint32]uint32)
	currClient := current.ID().Client
	stopCounting := current.ID().Clock != 0
	currClock := uint32(0)
	if !stopCounting {
		currClock = current.EndClock()
	}

	for current != nil {
		if current.ID().Client != currClient {
			if currClock != 0 {
				stateVector[currClient] = currClock
			}
			currClient = current.ID().Client
			currClock = 0
			stopCounting = current.ID().Clock != 0
		}

		if current.Kind() == ytypes.KindSkip {
			stopCounting = true
		}
		if !stopCounting {
			currClock = current.EndClock()
		}

		if err := reader.Next(); err != nil {
			return nil, err
		}
		current = reader.Current()
	}

	if currClock != 0 {
		stateVector[currClient] = currClock
	}
	return stateVector, nil
}

// DecodeStateVectorV1 interpreta um state vector V1 em memória.
func DecodeStateVectorV1(data []byte) (map[uint32]uint32, error) {
	count, n, err := varint.Decode(data)
	if err != nil {
		return nil, err
	}

	out := make(map[uint32]uint32, count)
	rest := data[n:]
	for i := uint32(0); i < count; i++ {
		client, cn, err := varint.Decode(rest)
		if err != nil {
			return nil, err
		}
		clock, kn, err := varint.Decode(rest[cn:])
		if err != nil {
			return nil, err
		}
		out[client] = clock
		rest = rest[cn+kn:]
	}
	return out, nil
}
