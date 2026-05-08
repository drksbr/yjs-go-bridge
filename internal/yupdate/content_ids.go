package yupdate

import (
	"context"
	"fmt"

	"github.com/drksbr/yjs-crdt-golang-server/internal/varint"
	"github.com/drksbr/yjs-crdt-golang-server/internal/yidset"
	"github.com/drksbr/yjs-crdt-golang-server/internal/ytypes"
)

// ContentIDs representa os ranges de inserts e deletes presentes em um update.
type ContentIDs struct {
	Inserts *yidset.IdSet
	Deletes *yidset.IdSet
}

// NewContentIDs cria uma estrutura vazia pronta para uso.
func NewContentIDs() *ContentIDs {
	return &ContentIDs{
		Inserts: yidset.New(),
		Deletes: yidset.New(),
	}
}

// Clone cria uma cópia profunda dos ranges.
func (c *ContentIDs) Clone() *ContentIDs {
	if c == nil {
		return NewContentIDs()
	}
	return &ContentIDs{
		Inserts: c.Inserts.Clone(),
		Deletes: c.Deletes.Clone(),
	}
}

// IsEmpty informa se inserts e deletes estão vazios.
func (c *ContentIDs) IsEmpty() bool {
	if c == nil {
		return true
	}
	return c.Inserts.IsEmpty() && c.Deletes.IsEmpty()
}

// CreateContentIDsFromUpdateV1 reproduz a extração de content ids do Yjs para updates V1.
func CreateContentIDsFromUpdateV1(update []byte) (*ContentIDs, error) {
	return ReadUpdateToContentIDsV1(update)
}

// CreateContentIDsFromUpdateV2 extrai content ids de um único update V2 sem
// materializar um payload V1 intermediário.
func CreateContentIDsFromUpdateV2(update []byte) (*ContentIDs, error) {
	decoded, err := DecodeV2(update)
	if err != nil {
		return nil, err
	}
	return contentIDsFromDecodedUpdate(decoded)
}

// ReadUpdateToContentIDsV1 segue a semântica atual do Yjs para content ids.
func ReadUpdateToContentIDsV1(update []byte) (*ContentIDs, error) {
	reader, err := NewLazyReaderV1(update, true)
	if err != nil {
		return nil, err
	}

	contentIDs := NewContentIDs()
	var (
		hasPending bool
		lastClient uint32
		lastClock  uint32
		lastLen    uint32
	)

	flushInsert := func() error {
		if !hasPending {
			return nil
		}
		if err := contentIDs.Inserts.Add(lastClient, lastClock, lastLen); err != nil {
			return err
		}
		hasPending = false
		return nil
	}

	for current := reader.Current(); current != nil; current = reader.Current() {
		id := current.ID()
		if hasPending && lastClient == id.Client && uint64(lastClock)+uint64(lastLen) == uint64(id.Clock) {
			lastLen += current.Length()
		} else {
			if err := flushInsert(); err != nil {
				return nil, err
			}
			hasPending = true
			lastClient = id.Client
			lastClock = id.Clock
			lastLen = current.Length()
		}

		if err := reader.Next(); err != nil {
			return nil, err
		}
	}

	if err := flushInsert(); err != nil {
		return nil, err
	}

	deleteSet, err := reader.ReadDeleteSet()
	if err != nil {
		return nil, err
	}
	if deleteSet != nil {
		var addErr error
		deleteSet.ForEachClient(func(client uint32, ranges []ytypes.DeleteRange) {
			if addErr != nil {
				return
			}
			for _, r := range ranges {
				if err := contentIDs.Deletes.Add(client, r.Clock, r.Length); err != nil {
					addErr = err
					return
				}
			}
		})
		if addErr != nil {
			return nil, addErr
		}
	}
	return contentIDs, nil
}

func contentIDsFromDecodedUpdate(decoded *DecodedUpdate) (*ContentIDs, error) {
	contentIDs := NewContentIDs()
	if decoded == nil {
		return contentIDs, nil
	}
	var (
		hasPending bool
		lastClient uint32
		lastClock  uint32
		lastLen    uint32
	)
	flushInsert := func() error {
		if !hasPending {
			return nil
		}
		if err := contentIDs.Inserts.Add(lastClient, lastClock, lastLen); err != nil {
			return err
		}
		hasPending = false
		return nil
	}
	for _, current := range decoded.Structs {
		if current == nil || isSkip(current) {
			continue
		}
		id := current.ID()
		if hasPending && lastClient == id.Client && uint64(lastClock)+uint64(lastLen) == uint64(id.Clock) {
			lastLen += current.Length()
			continue
		}
		if err := flushInsert(); err != nil {
			return nil, err
		}
		hasPending = true
		lastClient = id.Client
		lastClock = id.Clock
		lastLen = current.Length()
	}
	if err := flushInsert(); err != nil {
		return nil, err
	}
	if decoded.DeleteSet != nil {
		var addErr error
		decoded.DeleteSet.ForEachClient(func(client uint32, ranges []ytypes.DeleteRange) {
			if addErr != nil {
				return
			}
			for _, r := range ranges {
				if err := contentIDs.Deletes.Add(client, r.Clock, r.Length); err != nil {
					addErr = err
					return
				}
			}
		})
		if addErr != nil {
			return nil, addErr
		}
	}
	return contentIDs, nil
}

// MergeContentIDs combina de forma determinística os ranges de inserts e deletes.
//
// O resultado é uma nova estrutura com união por cliente e range normalizado.
func MergeContentIDs(a *ContentIDs, b ...*ContentIDs) *ContentIDs {
	out := NewContentIDs()
	mergeSet := func(target *yidset.IdSet, source *yidset.IdSet) {
		if source == nil {
			return
		}
		source.ForEachClient(func(client uint32, ranges []yidset.Range) {
			for _, r := range ranges {
				_ = target.Add(client, r.Clock, r.Length)
			}
		})
	}

	if a != nil {
		mergeSet(out.Inserts, a.Inserts)
		mergeSet(out.Deletes, a.Deletes)
	}
	for _, current := range b {
		if current == nil {
			continue
		}
		mergeSet(out.Inserts, current.Inserts)
		mergeSet(out.Deletes, current.Deletes)
	}
	return out
}

// EncodeContentIDs serializa ContentIDs em formato estável:
// - varuint de quantidade de clientes em inserts
// - blocos de cliente/ranges para inserts
// - varuint de quantidade de clientes em deletes
// - blocos de cliente/ranges para deletes
// Clientes e ranges seguem a ordenação da estrutura IdSet (clientes crescentes, ranges normalizados).
func EncodeContentIDs(contentIDs *ContentIDs) ([]byte, error) {
	if contentIDs == nil {
		contentIDs = NewContentIDs()
	}

	out := appendIDSetToWire(nil, contentIDs.Inserts)
	out = appendIDSetToWire(out, contentIDs.Deletes)
	return out, nil
}

func appendIDSetToWire(dst []byte, set *yidset.IdSet) []byte {
	dst = varint.Append(dst, uint32(set.ClientCount()))

	set.ForEachClient(func(client uint32, ranges []yidset.Range) {
		dst = varint.Append(dst, client)
		dst = varint.Append(dst, uint32(len(ranges)))
		for _, r := range ranges {
			dst = varint.Append(dst, r.Clock)
			dst = varint.Append(dst, r.Length)
		}
	})
	return dst
}

// DecodeContentIDs decodifica a representação estável de ContentIDs.
// Payloads com bytes remanescentes são rejeitados para evitar ambiguidades.
func DecodeContentIDs(data []byte) (*ContentIDs, error) {
	remaining := data

	inserts, consumed, err := decodeContentIDSet(remaining)
	if err != nil {
		return nil, err
	}
	remaining = remaining[consumed:]

	deletes, consumed, err := decodeContentIDSet(remaining)
	if err != nil {
		return nil, err
	}
	remaining = remaining[consumed:]

	if len(remaining) != 0 {
		return nil, ErrContentIDsTrailingBytes
	}

	return &ContentIDs{
		Inserts: inserts,
		Deletes: deletes,
	}, nil
}

// IntersectContentIDs retorna a interseção dos ranges por client entre dois ContentIDs.
//
// Inserções e deleções são processadas de forma independente para evitar
// acoplamento entre os dois domínios de ranges.
func IntersectContentIDs(a, b *ContentIDs) *ContentIDs {
	if a == nil || b == nil {
		return NewContentIDs()
	}

	return &ContentIDs{
		Inserts: yidset.IntersectSets(a.Inserts, b.Inserts),
		Deletes: yidset.IntersectSets(a.Deletes, b.Deletes),
	}
}

// DiffContentIDs retorna a diferença setorial de content ids:
// subject \\ remove (por cliente e por tipo de range).
//
// Inserções e deleções são reduzidas separadamente para preservar
// independência estrutural entre os campos.
func DiffContentIDs(subject, remove *ContentIDs) *ContentIDs {
	if subject == nil {
		return NewContentIDs()
	}
	removeInserts := (*yidset.IdSet)(nil)
	removeDeletes := (*yidset.IdSet)(nil)
	if remove != nil {
		removeInserts = remove.Inserts
		removeDeletes = remove.Deletes
	}

	return &ContentIDs{
		Inserts: yidset.SubtractSets(subject.Inserts, removeInserts),
		Deletes: yidset.SubtractSets(subject.Deletes, removeDeletes),
	}
}

// IsSubsetContentIDs informa se todos os ranges de subject estão contidos em container.
//
// O check é feito por campo de forma independente, reaproveitando a diferença de
// yidset para evitar lógica manual de recorrência por ranges.
func IsSubsetContentIDs(subject, container *ContentIDs) bool {
	if subject == nil {
		return true
	}

	insertDiff := yidset.SubtractSets(subject.Inserts, container.Inserts)
	deleteDiff := yidset.SubtractSets(subject.Deletes, container.Deletes)
	return insertDiff.IsEmpty() && deleteDiff.IsEmpty()
}

// ContentIDsFromUpdatesContext agrega ContentIDs extraídos de múltiplos updates
// suportados, respeitando cancelamento do contexto.
//
// Entradas nil ou com payload vazio são tratadas como no-op, mantendo o estado
// atual de agregação.
func ContentIDsFromUpdatesContext(ctx context.Context, updates ...[]byte) (*ContentIDs, error) {
	format, err := detectAggregateUpdateFormatSkippingEmptyContext(ctx, updates...)
	if err != nil {
		return nil, err
	}
	switch format {
	case UpdateFormatUnknown:
		return NewContentIDs(), nil
	case UpdateFormatV2:
		return aggregatePayloadsInParallel(ctx, updates, 0, extractContentIDsFromUpdateV2, mergeContentIDPayloads)
	}

	return aggregatePayloadsInParallel(ctx, updates, 0, extractContentIDsFromUpdateV1, mergeContentIDPayloads)
}

// ContentIDsFromUpdates agrega ContentIDs extraídos de múltiplos updates suportados.
//
// Entradas nil ou com payload vazio são tratadas como no-op, mantendo o estado
// atual de agregação.
func ContentIDsFromUpdates(updates ...[]byte) (*ContentIDs, error) {
	out, err := ContentIDsFromUpdatesContext(context.Background(), updates...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func extractContentIDsFromUpdateV1(_ context.Context, _ int, update []byte) (*ContentIDs, error) {
	if len(update) == 0 {
		return NewContentIDs(), nil
	}
	return CreateContentIDsFromUpdateV1(update)
}

func extractContentIDsFromUpdateV2(_ context.Context, _ int, update []byte) (*ContentIDs, error) {
	if len(update) == 0 {
		return NewContentIDs(), nil
	}
	return CreateContentIDsFromUpdateV2(update)
}

func mergeContentIDPayloads(_ context.Context, contents []*ContentIDs) (*ContentIDs, error) {
	out := NewContentIDs()
	for _, contentIDs := range contents {
		if contentIDs == nil || contentIDs.IsEmpty() {
			continue
		}
		if err := yidset.InsertIntoIdSet(out.Inserts, contentIDs.Inserts); err != nil {
			return nil, err
		}
		if err := yidset.InsertIntoIdSet(out.Deletes, contentIDs.Deletes); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func decodeContentIDSet(data []byte) (*yidset.IdSet, int, error) {
	clientCount, consumed, err := varint.Decode(data)
	if err != nil {
		return nil, 0, fmt.Errorf("%w: %v", err, ErrInvalidContentIDsPayload)
	}

	rest := data[consumed:]
	out := yidset.New()

	for i := uint32(0); i < clientCount; i++ {
		client, consumed, err := varint.Decode(rest)
		if err != nil {
			return nil, 0, fmt.Errorf("%w: %v", err, ErrInvalidContentIDsPayload)
		}
		rest = rest[consumed:]

		rangeCount, consumed, err := varint.Decode(rest)
		if err != nil {
			return nil, 0, fmt.Errorf("%w: %v", err, ErrInvalidContentIDsPayload)
		}
		rest = rest[consumed:]

		for j := uint32(0); j < rangeCount; j++ {
			clock, consumed, err := varint.Decode(rest)
			if err != nil {
				return nil, 0, fmt.Errorf("%w: %v", err, ErrInvalidContentIDsPayload)
			}
			rest = rest[consumed:]

			length, consumed, err := varint.Decode(rest)
			if err != nil {
				return nil, 0, fmt.Errorf("%w: %v", err, ErrInvalidContentIDsPayload)
			}
			rest = rest[consumed:]

			if err := out.Add(client, clock, length); err != nil {
				return nil, 0, fmt.Errorf("%w: adicionar range: %v", err, ErrInvalidContentIDsPayload)
			}
		}
	}

	return out, len(data) - len(rest), nil
}
