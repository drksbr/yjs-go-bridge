package ytypes

import (
	"errors"

	"github.com/drksbr/yjs-crdt-golang-server/internal/yidset"
)

// DeleteRange é um alias semântico para ranges deletados.
// A implementação concreta fica em `internal/yidset`, que já normaliza
// ordenação, sobreposição e adjacência.
type DeleteRange = yidset.Range

// DeleteSet representa deletions por cliente sem duplicar a lógica genérica
// de intervalos que pertence ao pacote `yidset`.
type DeleteSet struct {
	ids *yidset.Set
}

// NewDeleteSet cria um delete set vazio.
func NewDeleteSet() *DeleteSet {
	return &DeleteSet{ids: yidset.New()}
}

// Clone cria uma cópia profunda do conjunto.
func (ds *DeleteSet) Clone() *DeleteSet {
	if ds == nil || ds.ids == nil {
		return NewDeleteSet()
	}
	return &DeleteSet{ids: ds.ids.Clone()}
}

// IsEmpty informa se não há ranges de deleção registrados.
func (ds *DeleteSet) IsEmpty() bool {
	return ds == nil || ds.ids == nil || ds.ids.IsEmpty()
}

// ClientCount returns the number of clients with deletions.
func (ds *DeleteSet) ClientCount() int {
	if ds == nil || ds.ids == nil {
		return 0
	}
	return ds.ids.ClientCount()
}

// Add registra uma deleção normalizando overlaps e adjacências.
func (ds *DeleteSet) Add(client, clock, length uint32) error {
	return normalizeDeleteSetError(ds.ensure().Add(client, clock, length))
}

// AddID registra uma deleção a partir de um ID base.
func (ds *DeleteSet) AddID(id ID, length uint32) error {
	return ds.Add(id.Client, id.Clock, length)
}

// Merge incorpora os ranges de outro delete set.
func (ds *DeleteSet) Merge(other *DeleteSet) error {
	if other == nil || other.ids == nil {
		return nil
	}
	return normalizeDeleteSetError(ds.ensure().Merge(other.ids))
}

// Has informa se um ID pertence a alguma faixa deletada.
func (ds *DeleteSet) Has(id ID) bool {
	if ds == nil || ds.ids == nil {
		return false
	}
	return ds.ids.Has(id.Client, id.Clock)
}

// Clients retorna os clientes em ordem crescente para iteração determinística.
func (ds *DeleteSet) Clients() []uint32 {
	if ds == nil || ds.ids == nil {
		return nil
	}
	return ds.ids.Clients()
}

// Ranges retorna uma cópia das faixas deletadas do cliente.
func (ds *DeleteSet) Ranges(client uint32) []DeleteRange {
	if ds == nil || ds.ids == nil {
		return nil
	}
	return ds.ids.Ranges(client)
}

// ForEachClient iterates clients in ascending order and exposes normalized
// ranges for read-only use.
func (ds *DeleteSet) ForEachClient(fn func(client uint32, ranges []DeleteRange)) {
	if ds == nil || ds.ids == nil {
		return
	}
	ds.ids.ForEachClient(func(client uint32, ranges []yidset.Range) {
		fn(client, ranges)
	})
}

// ForEachClientDesc iterates clients in descending order and exposes normalized
// ranges for read-only use.
func (ds *DeleteSet) ForEachClientDesc(fn func(client uint32, ranges []DeleteRange)) {
	if ds == nil || ds.ids == nil {
		return
	}
	ds.ids.ForEachClientDesc(func(client uint32, ranges []yidset.Range) {
		fn(client, ranges)
	})
}

func (ds *DeleteSet) ensure() *yidset.Set {
	if ds.ids == nil {
		ds.ids = yidset.New()
	}
	return ds.ids
}

func normalizeDeleteSetError(err error) error {
	switch {
	case errors.Is(err, yidset.ErrInvalidLength):
		return ErrInvalidLength
	case errors.Is(err, yidset.ErrRangeOverflow):
		return ErrStructOverflow
	default:
		return err
	}
}
