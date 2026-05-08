package yupdate

import (
	"fmt"

	ybinary "github.com/drksbr/yjs-crdt-golang-server/internal/binary"
	"github.com/drksbr/yjs-crdt-golang-server/internal/varint"
	"github.com/drksbr/yjs-crdt-golang-server/internal/ytypes"
)

// AppendDeleteSetBlockV1 escreve o bloco de delete set do update V1 em dst.
//
// Layout compatível com o Yjs:
// - varuint com a quantidade de clientes
// - para cada cliente, em ordem decrescente:
//   - client id
//   - quantidade de ranges deletados
//   - pares clock/length em ordem crescente de clock
func AppendDeleteSetBlockV1(dst []byte, ds *ytypes.DeleteSet) []byte {
	dst = varint.Append(dst, uint32(ds.ClientCount()))
	ds.ForEachClientDesc(func(client uint32, ranges []ytypes.DeleteRange) {
		dst = varint.Append(dst, client)
		dst = varint.Append(dst, uint32(len(ranges)))
		for _, r := range ranges {
			dst = varint.Append(dst, r.Clock)
			dst = varint.Append(dst, r.Length)
		}
	})
	return dst
}

// EncodeDeleteSetBlockV1 retorna a codificação V1 completa do bloco de delete set.
func EncodeDeleteSetBlockV1(ds *ytypes.DeleteSet) []byte {
	return AppendDeleteSetBlockV1(nil, ds)
}

// ReadDeleteSetBlockV1 lê um bloco de delete set V1 da posição atual do reader.
func ReadDeleteSetBlockV1(r *ybinary.Reader) (*ytypes.DeleteSet, error) {
	count, err := readDeleteSetVarUint("ler quantidade de clientes", r)
	if err != nil {
		return nil, err
	}

	ds := ytypes.NewDeleteSet()
	for i := uint32(0); i < count; i++ {
		client, err := readDeleteSetVarUint("ler client id", r)
		if err != nil {
			return nil, err
		}

		rangeCount, err := readDeleteSetVarUint("ler quantidade de ranges", r)
		if err != nil {
			return nil, err
		}

		for j := uint32(0); j < rangeCount; j++ {
			clock, err := readDeleteSetVarUint("ler clock do range", r)
			if err != nil {
				return nil, err
			}

			length, err := readDeleteSetVarUint("ler comprimento do range", r)
			if err != nil {
				return nil, err
			}

			if err := ds.Add(client, clock, length); err != nil {
				return nil, fmt.Errorf("yupdate: adicionar range deletado: %w", err)
			}
		}
	}

	return ds, nil
}

// DecodeDeleteSetBlockV1 lê um bloco de delete set V1 a partir de src.
// O retorno consumed informa quantos bytes foram avançados, inclusive em caso de erro.
func DecodeDeleteSetBlockV1(src []byte) (ds *ytypes.DeleteSet, consumed int, err error) {
	r := ybinary.NewReader(src)
	ds, err = ReadDeleteSetBlockV1(r)
	return ds, r.Offset(), err
}

func readDeleteSetVarUint(op string, r *ybinary.Reader) (uint32, error) {
	value, _, err := varint.Read(r)
	if err != nil {
		return 0, fmt.Errorf("yupdate: %s: %w", op, err)
	}
	return value, nil
}
