package yupdate

import "github.com/drksbr/yjs-crdt-golang-server/internal/ytypes"

func readDeleteSetV1(d *decoderV1) (*ytypes.DeleteSet, error) {
	return ReadDeleteSetBlockV1(&d.reader)
}
