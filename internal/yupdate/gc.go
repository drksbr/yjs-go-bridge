package yupdate

import (
	"github.com/drksbr/yjs-crdt-golang-server/internal/ytypes"
)

func garbageCollectDeletedContent(decoded *DecodedUpdate) (*DecodedUpdate, error) {
	if decoded == nil || decoded.DeleteSet == nil || decoded.DeleteSet.IsEmpty() {
		return decoded, nil
	}

	referenced := referencedItemClocks(decoded.Structs)
	deleteRanges := make(map[uint32][]ytypes.DeleteRange, decoded.DeleteSet.ClientCount())
	decoded.DeleteSet.ForEachClient(func(client uint32, ranges []ytypes.DeleteRange) {
		deleteRanges[client] = ranges
	})
	structs := make([]ytypes.Struct, 0, len(decoded.Structs))
	for _, current := range decoded.Structs {
		var ranges []ytypes.DeleteRange
		if current != nil {
			ranges = deleteRanges[current.ID().Client]
		}
		next, err := garbageCollectStruct(current, ranges, referenced)
		if err != nil {
			return nil, err
		}
		structs = append(structs, next...)
	}

	return &DecodedUpdate{
		Structs:   structs,
		DeleteSet: decoded.DeleteSet.Clone(),
	}, nil
}

func garbageCollectStruct(current ytypes.Struct, ranges []ytypes.DeleteRange, referenced map[uint32][]uint32) ([]ytypes.Struct, error) {
	if current == nil || len(ranges) == 0 {
		return []ytypes.Struct{current}, nil
	}

	start := current.ID().Clock
	end := current.EndClock()

	result := make([]ytypes.Struct, 0, 3)
	cursor := start
	overlapped := false
	for _, r := range ranges {
		rangeStart := r.Clock
		rangeEnd64 := uint64(r.Clock) + uint64(r.Length)
		if rangeEnd64 <= uint64(cursor) {
			continue
		}
		if uint64(rangeStart) >= uint64(end) {
			break
		}

		if rangeStart > cursor {
			keepEnd := minUint32(rangeStart, end)
			segment, err := sliceStructByClock(current, cursor, keepEnd)
			if err != nil {
				return nil, err
			}
			result = append(result, segment)
			cursor = keepEnd
		}

		deleteStart := maxUint32(cursor, rangeStart)
		deleteEnd := minUint32(end, uint32(minUint64(rangeEnd64, uint64(end))))
		if deleteEnd > deleteStart {
			segment, err := garbageCollectStructWindow(current, deleteStart, deleteEnd, referenced)
			if err != nil {
				return nil, err
			}
			result = append(result, segment)
			cursor = deleteEnd
			overlapped = true
		}
		if cursor >= end {
			break
		}
	}

	if !overlapped {
		return []ytypes.Struct{current}, nil
	}
	if cursor < end {
		segment, err := sliceStructByClock(current, cursor, end)
		if err != nil {
			return nil, err
		}
		result = append(result, segment)
	}
	return result, nil
}

func garbageCollectStructWindow(current ytypes.Struct, startClock, endClock uint32, referenced map[uint32][]uint32) (ytypes.Struct, error) {
	if item, ok := current.(*ytypes.Item); ok && item.Keep() {
		return sliceStructByClock(current, startClock, endClock)
	}
	if hasReferencedClock(current.ID().Client, startClock, endClock, referenced) {
		return sliceStructByClock(current, startClock, endClock)
	}

	id, err := current.ID().Offset(startClock - current.ID().Clock)
	if err != nil {
		return nil, err
	}
	return ytypes.NewGC(id, endClock-startClock)
}

func referencedItemClocks(structs []ytypes.Struct) map[uint32][]uint32 {
	referenced := make(map[uint32][]uint32)
	for _, current := range structs {
		item, ok := current.(*ytypes.Item)
		if !ok {
			continue
		}
		if item.Origin != nil {
			referenced[item.Origin.Client] = append(referenced[item.Origin.Client], item.Origin.Clock)
		}
		if item.RightOrigin != nil {
			referenced[item.RightOrigin.Client] = append(referenced[item.RightOrigin.Client], item.RightOrigin.Clock)
		}
	}
	return referenced
}

func hasReferencedClock(client uint32, startClock uint32, endClock uint32, referenced map[uint32][]uint32) bool {
	if len(referenced) == 0 {
		return false
	}
	for _, clock := range referenced[client] {
		if clock >= startClock && clock < endClock {
			return true
		}
	}
	return false
}

func sliceStructByClock(current ytypes.Struct, startClock, endClock uint32) (ytypes.Struct, error) {
	if startClock == current.ID().Clock && endClock == current.EndClock() {
		return current, nil
	}
	return sliceStructWindowV1(current, startClock-current.ID().Clock, current.EndClock()-endClock)
}

func minUint32(a, b uint32) uint32 {
	if a < b {
		return a
	}
	return b
}

func maxUint32(a, b uint32) uint32 {
	if a > b {
		return a
	}
	return b
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
