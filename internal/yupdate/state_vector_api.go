package yupdate

import (
	"context"
	"sort"

	"github.com/drksbr/yjs-crdt-golang-server/internal/varint"
	"github.com/drksbr/yjs-crdt-golang-server/internal/ytypes"
)

// DecodeStateVector decodifica um state vector em mapa por client.
// Mantido como wrapper público da versão V1, com semântica equivalente.
func DecodeStateVector(data []byte) (map[uint32]uint32, error) {
	return DecodeStateVectorV1(data)
}

// StateVectorFromUpdatesContext agrega state vectors por cliente a partir de uma
// lista de payloads, respeitando cancelamento do contexto.
func StateVectorFromUpdatesContext(ctx context.Context, updates ...[]byte) (map[uint32]uint32, error) {
	format, err := detectAggregateUpdateFormatSkippingEmptyContext(ctx, updates...)
	if err != nil {
		return nil, err
	}
	switch format {
	case UpdateFormatUnknown:
		return map[uint32]uint32{}, nil
	case UpdateFormatV2:
		filtered := make([][]byte, 0, len(updates))
		for _, update := range updates {
			if len(update) == 0 {
				continue
			}
			filtered = append(filtered, update)
		}
		merged, err := aggregatePayloadsInParallel(ctx, filtered, 0, decodeMergeUpdate, mergeDecodedUpdatesV1)
		if err != nil {
			return nil, err
		}
		return stateVectorFromBlockSet(merged.blockSet), nil
	}

	stateVectors, err := aggregatePayloadsInParallel(ctx, updates, 0, extractStateVectorFromUpdateV1, mergeStateVectors)
	if err != nil {
		return nil, err
	}
	return stateVectors, nil
}

func stateVectorFromStructs(structs []ytypes.Struct) map[uint32]uint32 {
	stateVector := make(map[uint32]uint32)
	for _, current := range structs {
		if current == nil {
			continue
		}
		client := current.ID().Client
		endClock := current.EndClock()
		if endClock > stateVector[client] {
			stateVector[client] = endClock
		}
	}
	return stateVector
}

func stateVectorFromBlockSet(blockSet *blockSetV1) map[uint32]uint32 {
	if blockSet == nil || len(blockSet.clients) == 0 {
		return map[uint32]uint32{}
	}
	stateVector := make(map[uint32]uint32, len(blockSet.clients))
	for client, structs := range blockSet.clients {
		for _, current := range structs {
			if current == nil {
				continue
			}
			endClock := current.EndClock()
			if endClock > stateVector[client] {
				stateVector[client] = endClock
			}
		}
	}
	return stateVector
}

// StateVectorFromUpdates agrega state vectors por cliente a partir de uma lista
// de payloads de update V1.
func StateVectorFromUpdates(updates ...[]byte) (map[uint32]uint32, error) {
	return StateVectorFromUpdatesContext(context.Background(), updates...)
}

// EncodeStateVectorFromUpdatesContext agrega os state vectors de múltiplos
// updates e serializa no formato V1 estável, respeitando cancelamento.
func EncodeStateVectorFromUpdatesContext(ctx context.Context, updates ...[]byte) ([]byte, error) {
	stateVector, err := StateVectorFromUpdatesContext(ctx, updates...)
	if err != nil {
		return nil, err
	}
	return encodeStateVectorMap(stateVector), nil
}

// EncodeStateVectorFromUpdates agrega os state vectors de múltiplos updates e
// serializa no formato V1 estável.
func EncodeStateVectorFromUpdates(updates ...[]byte) ([]byte, error) {
	return EncodeStateVectorFromUpdatesContext(context.Background(), updates...)
}

func extractStateVectorFromUpdateV1(_ context.Context, _ int, update []byte) (map[uint32]uint32, error) {
	if len(update) == 0 {
		return map[uint32]uint32{}, nil
	}
	return stateVectorFromUpdateV1(update)
}

func mergeStateVectors(_ context.Context, vectors []map[uint32]uint32) (map[uint32]uint32, error) {
	if len(vectors) == 0 {
		return map[uint32]uint32{}, nil
	}

	capacity := 0
	for _, vector := range vectors {
		capacity += len(vector)
	}
	merged := make(map[uint32]uint32, capacity)
	for _, vector := range vectors {
		if len(vector) == 0 {
			continue
		}
		for client, clock := range vector {
			current, ok := merged[client]
			if !ok || clock > current {
				merged[client] = clock
			}
		}
	}
	return merged, nil
}

func encodeStateVectorMap(stateVector map[uint32]uint32) []byte {
	if len(stateVector) == 0 {
		return varint.Append(nil, 0)
	}

	clients := make([]uint32, 0, len(stateVector))
	for client := range stateVector {
		clients = append(clients, client)
	}
	sort.Slice(clients, func(i, j int) bool {
		return clients[i] < clients[j]
	})

	out := varint.Append(nil, uint32(len(stateVector)))
	for _, client := range clients {
		out = varint.Append(out, client)
		out = varint.Append(out, stateVector[client])
	}
	return out
}
