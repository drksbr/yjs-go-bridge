package yjsbridge

import (
	"errors"
	"fmt"

	"github.com/drksbr/yjs-crdt-golang-server/internal/yidset"
	"github.com/drksbr/yjs-crdt-golang-server/internal/yupdate"
)

// IDRange representa um intervalo [clock, clock+length) associado a um client.
type IDRange struct {
	// Client identifica o cliente/cliente-id proprietário do range.
	Client uint32
	// Clock identifica o relógio inicial (inclusive) do range.
	Clock uint32
	// Length define quantos itens consecutivos o range cobre.
	Length uint32
}

// ErrNilContentIDs sinaliza uso de receiver nulo em mutações de ContentIDs.
var ErrNilContentIDs = errors.New("yjsbridge: content ids cannot be nil")

// ContentIDs representa ranges de inserts e deletes sem expor tipos internos.
type ContentIDs struct {
	inserts *yidset.IdSet
	deletes *yidset.IdSet
}

// NewContentIDs cria uma estrutura vazia pronta para uso.
func NewContentIDs() *ContentIDs {
	return &ContentIDs{
		inserts: yidset.New(),
		deletes: yidset.New(),
	}
}

// Clone cria uma cópia profunda dos ranges.
func (c *ContentIDs) Clone() *ContentIDs {
	if c == nil {
		return NewContentIDs()
	}
	return &ContentIDs{
		inserts: c.insertSet().Clone(),
		deletes: c.deleteSet().Clone(),
	}
}

// IsEmpty informa se inserts e deletes estão vazios.
func (c *ContentIDs) IsEmpty() bool {
	if c == nil {
		return true
	}
	return c.insertSet().IsEmpty() && c.deleteSet().IsEmpty()
}

// AddInsert registra um range de inserts.
func (c *ContentIDs) AddInsert(client, clock, length uint32) error {
	if c == nil {
		return ErrNilContentIDs
	}
	if c.inserts == nil {
		c.inserts = yidset.New()
	}
	return c.inserts.Add(client, clock, length)
}

// AddDelete registra um range de deletes.
func (c *ContentIDs) AddDelete(client, clock, length uint32) error {
	if c == nil {
		return ErrNilContentIDs
	}
	if c.deletes == nil {
		c.deletes = yidset.New()
	}
	return c.deletes.Add(client, clock, length)
}

// InsertRanges retorna os ranges de inserts em ordem determinística.
func (c *ContentIDs) InsertRanges() []IDRange {
	return collectRanges(c.insertSet())
}

// DeleteRanges retorna os ranges de deletes em ordem determinística.
func (c *ContentIDs) DeleteRanges() []IDRange {
	return collectRanges(c.deleteSet())
}

// EncodeContentIDs serializa ranges em payload estável.
func EncodeContentIDs(contentIDs *ContentIDs) ([]byte, error) {
	return yupdate.EncodeContentIDs(contentIDs.toInternal())
}

// DecodeContentIDs decodifica o payload estável em ranges públicos.
func DecodeContentIDs(data []byte) (*ContentIDs, error) {
	contentIDs, err := yupdate.DecodeContentIDs(data)
	if err != nil {
		if errors.Is(err, yupdate.ErrContentIDsTrailingBytes) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: %v", yupdate.ErrInvalidContentIDsPayload, err)
	}
	return wrapContentIDs(contentIDs), nil
}

// MergeContentIDs combina ranges de forma determinística.
func MergeContentIDs(a *ContentIDs, b ...*ContentIDs) *ContentIDs {
	inputs := make([]*yupdate.ContentIDs, 0, len(b)+1)
	inputs = append(inputs, a.toInternal())
	for _, current := range b {
		inputs = append(inputs, current.toInternal())
	}
	if len(inputs) == 0 {
		return NewContentIDs()
	}
	return wrapContentIDs(yupdate.MergeContentIDs(inputs[0], inputs[1:]...))
}

// IntersectContentIDs retorna a interseção de inserts e deletes por client.
func IntersectContentIDs(a, b *ContentIDs) *ContentIDs {
	return wrapContentIDs(yupdate.IntersectContentIDs(a.toInternal(), b.toInternal()))
}

// DiffContentIDs retorna a diferença setorial `subject \ remove`.
func DiffContentIDs(subject, remove *ContentIDs) *ContentIDs {
	return wrapContentIDs(yupdate.DiffContentIDs(subject.toInternal(), remove.toInternal()))
}

// IsSubsetContentIDs informa se todos os ranges de subject cabem em container.
func IsSubsetContentIDs(subject, container *ContentIDs) bool {
	return yupdate.IsSubsetContentIDs(subject.toInternal(), container.toInternal())
}

func wrapContentIDs(contentIDs *yupdate.ContentIDs) *ContentIDs {
	if contentIDs == nil {
		return NewContentIDs()
	}
	return &ContentIDs{
		inserts: contentIDs.Inserts.Clone(),
		deletes: contentIDs.Deletes.Clone(),
	}
}

func (c *ContentIDs) toInternal() *yupdate.ContentIDs {
	if c == nil {
		return yupdate.NewContentIDs()
	}
	return &yupdate.ContentIDs{
		Inserts: c.insertSet().Clone(),
		Deletes: c.deleteSet().Clone(),
	}
}

func (c *ContentIDs) insertSet() *yidset.IdSet {
	if c == nil || c.inserts == nil {
		return yidset.New()
	}
	return c.inserts
}

func (c *ContentIDs) deleteSet() *yidset.IdSet {
	if c == nil || c.deletes == nil {
		return yidset.New()
	}
	return c.deletes
}

func collectRanges(set *yidset.IdSet) []IDRange {
	if set == nil || set.IsEmpty() {
		return []IDRange{}
	}

	out := make([]IDRange, 0, set.TotalRangeCount())
	set.ForEachClient(func(client uint32, ranges []yidset.Range) {
		for _, current := range ranges {
			out = append(out, IDRange{
				Client: client,
				Clock:  current.Clock,
				Length: current.Length,
			})
		}
	})
	return out
}
