package yidset

// SliceSegment descreve um trecho de um intervalo consultado e se ele existe no set.
type SliceSegment struct {
	Clock  uint32
	Length uint32
	Exists bool
}

// Slice retorna uma partição do intervalo [clock, clock+length) em segmentos
// existentes e ausentes dentro do set para um cliente.
func (s *IdSet) Slice(client, clock, length uint32) []SliceSegment {
	if length == 0 {
		return nil
	}

	end, ok := rangeEnd(clock, length)
	if !ok {
		return []SliceSegment{{Clock: clock, Length: length, Exists: false}}
	}

	ranges := s.clients[client]
	if len(ranges) == 0 {
		return []SliceSegment{{Clock: clock, Length: length, Exists: false}}
	}

	segments := make([]SliceSegment, 0, len(ranges)+1)
	cursor := clock
	for _, current := range ranges {
		if current.End() <= uint64(cursor) {
			continue
		}
		if uint64(current.Clock) >= end {
			break
		}

		start := current.Clock
		if start < cursor {
			start = cursor
		}
		currentEnd := uint32(current.End())
		if uint64(currentEnd) > end {
			currentEnd = uint32(end)
		}
		if cursor < start {
			segments = append(segments, SliceSegment{Clock: cursor, Length: start - cursor, Exists: false})
		}
		if start < currentEnd {
			segments = append(segments, SliceSegment{Clock: start, Length: currentEnd - start, Exists: true})
			cursor = currentEnd
		}
	}

	if uint64(cursor) < end {
		segments = append(segments, SliceSegment{Clock: cursor, Length: uint32(end) - cursor, Exists: false})
	}
	if len(segments) == 0 {
		return []SliceSegment{{Clock: clock, Length: length, Exists: false}}
	}
	return segments
}

// IntersectSets retorna a interseção dos ranges de dois conjuntos.
func IntersectSets(left, right *IdSet) *IdSet {
	if left == nil || right == nil {
		return New()
	}

	result := New()
	result.clients = make(map[uint32][]Range)
	for _, client := range left.Clients() {
		a := left.clients[client]
		b := right.clients[client]
		if len(a) == 0 || len(b) == 0 {
			continue
		}

		resRanges := make([]Range, 0)
		for i, j := 0, 0; i < len(a) && j < len(b); {
			start := a[i].Clock
			if b[j].Clock > start {
				start = b[j].Clock
			}

			end := a[i].End()
			if b[j].End() < end {
				end = b[j].End()
			}
			if uint64(start) < end {
				resRanges = append(resRanges, Range{
					Clock:  start,
					Length: uint32(end - uint64(start)),
				})
			}

			if a[i].End() < b[j].End() {
				i++
			} else {
				j++
			}
		}

		if len(resRanges) > 0 {
			result.clients[client] = resRanges
		}
	}
	return result
}

// SubtractSets retorna a diferença entre left e remove (left \ remove).
func SubtractSets(left, remove *IdSet) *IdSet {
	if left == nil {
		return New()
	}

	result := left.Clone()
	if remove == nil {
		return result
	}

	if err := validateSetRanges(left); err != nil {
		return New()
	}
	if err := validateSetRanges(remove); err != nil {
		return New()
	}
	if err := result.Subtract(remove); err != nil {
		// Para manter o contrato atual sem expor panics neste pacote interno,
		// trata inconsistências internas como vazio.
		return New()
	}
	return result
}

// Subtract remove todos os ranges presentes em other do conjunto atual.
func (s *IdSet) Subtract(other *IdSet) error {
	if s == nil || s.IsEmpty() || other == nil || other.IsEmpty() {
		return nil
	}

	if s.clients == nil {
		return nil
	}

	next := make(map[uint32][]Range)
	for client, baseRanges := range s.clients {
		removals := other.clients[client]
		if len(baseRanges) == 0 {
			continue
		}
		if err := validateRangeList(baseRanges); err != nil {
			return err
		}
		if err := validateRangeList(removals); err != nil {
			return err
		}

		if len(removals) == 0 {
			next[client] = append(next[client], baseRanges...)
			continue
		}

		diff := subtractRanges(baseRanges, removals)
		for _, r := range diff {
			if err := addRange(next, client, r.Clock, r.Length); err != nil {
				return err
			}
		}
	}

	s.clients = next
	return nil
}

func validateSetRanges(set *IdSet) error {
	if set == nil {
		return nil
	}
	for _, ranges := range set.clients {
		if err := validateRangeList(ranges); err != nil {
			return err
		}
	}
	return nil
}

func validateRangeList(ranges []Range) error {
	for _, r := range ranges {
		if _, ok := rangeEnd(r.Clock, r.Length); !ok {
			return ErrRangeOverflow
		}
		if r.Length == 0 {
			return ErrInvalidLength
		}
	}
	return nil
}

func subtractRanges(left, right []Range) []Range {
	if len(right) == 0 {
		return append([]Range(nil), left...)
	}

	out := make([]Range, 0, len(left))
	j := 0

	for _, current := range left {
		currentStart := uint64(current.Clock)
		currentEnd := current.End()
		cursor := currentStart

		for j < len(right) && right[j].End() <= currentStart {
			j++
		}
		for j < len(right) && uint64(right[j].Clock) < currentEnd {
			removeStart := uint64(right[j].Clock)
			removeEnd := right[j].End()

			if removeStart > cursor {
				out = append(out, Range{
					Clock:  uint32(cursor),
					Length: uint32(removeStart - cursor),
				})
			}

			if removeEnd >= currentEnd {
				cursor = currentEnd
				break
			}
			cursor = removeEnd
			j++
		}

		if cursor < currentEnd {
			out = append(out, Range{
				Clock:  uint32(cursor),
				Length: uint32(currentEnd - cursor),
			})
		}
	}

	return out
}

func addRange(target map[uint32][]Range, client, clock, length uint32) error {
	existing := target[client]
	next, err := NewRange(clock, length)
	if err != nil {
		return err
	}

	if len(existing) == 0 {
		target[client] = []Range{next}
		return nil
	}

	merged := make([]Range, 0, len(existing)+1)
	inserted := false

	for _, current := range existing {
		if current.End() < uint64(next.Clock) {
			merged = append(merged, current)
			continue
		}

		if next.End() < uint64(current.Clock) {
			if !inserted {
				merged = append(merged, next)
				inserted = true
			}
			merged = append(merged, current)
			continue
		}

		next = mergeRanges(next, current)
	}

	if !inserted {
		merged = append(merged, next)
	}

	target[client] = merged
	return nil
}
