package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/drksbr/yjs-crdt-golang-server/pkg/storage"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/yjsbridge"
)

var (
	errUninitializedStore = errors.New("postgres: store nao inicializado")
)

// Store persiste snapshots em PostgreSQL.
type Store struct {
	pool   *pgxpool.Pool
	schema string
}

var _ storage.SnapshotStore = (*Store)(nil)
var _ storage.SnapshotCheckpointStore = (*Store)(nil)
var _ storage.SnapshotCheckpointEpochStore = (*Store)(nil)
var _ storage.AuthoritativeSnapshotStore = (*Store)(nil)
var _ storage.AuthoritativeSnapshotCheckpointStore = (*Store)(nil)

// New cria um store PostgreSQL e executa automigration por padrão.
func New(ctx context.Context, cfg Config) (*Store, error) {
	cfg = cfg.normalized()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	poolConfig, err := pgxpool.ParseConfig(cfg.ConnectionString)
	if err != nil {
		return nil, err
	}
	poolConfig.ConnConfig.RuntimeParams["application_name"] = cfg.ApplicationName
	if cfg.MinConns > 0 {
		poolConfig.MinConns = cfg.MinConns
	}
	if cfg.MaxConns > 0 {
		poolConfig.MaxConns = cfg.MaxConns
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, err
	}

	store := &Store{
		pool:   pool,
		schema: cfg.Schema,
	}
	if !cfg.SkipMigrations {
		if err := store.AutoMigrate(ctx); err != nil {
			pool.Close()
			return nil, err
		}
	}
	return store, nil
}

// Close libera o pool de conexões.
func (s *Store) Close() {
	if s != nil && s.pool != nil {
		s.pool.Close()
	}
}

func (s *Store) requirePool() (*pgxpool.Pool, error) {
	if s == nil || s.pool == nil {
		return nil, errUninitializedStore
	}
	return s.pool, nil
}

// SaveSnapshot grava ou substitui o snapshot de um documento.
func (s *Store) SaveSnapshot(ctx context.Context, key storage.DocumentKey, snapshot *yjsbridge.PersistedSnapshot) (*storage.SnapshotRecord, error) {
	return s.SaveSnapshotCheckpoint(ctx, key, snapshot, 0)
}

// SaveSnapshotCheckpoint grava ou substitui o snapshot de um documento,
// persistindo o high-water mark que ele já incorpora.
func (s *Store) SaveSnapshotCheckpoint(ctx context.Context, key storage.DocumentKey, snapshot *yjsbridge.PersistedSnapshot, through storage.UpdateOffset) (*storage.SnapshotRecord, error) {
	return s.SaveSnapshotCheckpointEpoch(ctx, key, snapshot, through, 0)
}

// SaveSnapshotCheckpointEpoch grava ou substitui o snapshot de um documento,
// persistindo o high-water mark e o epoch observados naquele checkpoint.
func (s *Store) SaveSnapshotCheckpointEpoch(ctx context.Context, key storage.DocumentKey, snapshot *yjsbridge.PersistedSnapshot, through storage.UpdateOffset, epoch uint64) (*storage.SnapshotRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := key.Validate(); err != nil {
		return nil, err
	}
	if snapshot == nil {
		return nil, storage.ErrNilPersistedSnapshot
	}
	pool, err := s.requirePool()
	if err != nil {
		return nil, err
	}

	payloadV1, payloadV2, err := encodePersistedSnapshotPayloads(snapshot)
	if err != nil {
		return nil, err
	}
	storedAt, err := s.saveSnapshotPool(ctx, pool, key, payloadV1, payloadV2, through, epoch)
	if err != nil {
		return nil, err
	}

	return &storage.SnapshotRecord{
		Key:      key,
		Snapshot: snapshot.Clone(),
		Through:  through,
		Epoch:    epoch,
		StoredAt: storedAt,
	}, nil
}

// SaveSnapshotAuthoritative grava ou substitui o snapshot do documento,
// exigindo que o placement + lease persistidos ainda correspondam ao fence.
func (s *Store) SaveSnapshotAuthoritative(
	ctx context.Context,
	key storage.DocumentKey,
	snapshot *yjsbridge.PersistedSnapshot,
	fence storage.AuthorityFence,
) (*storage.SnapshotRecord, error) {
	return s.SaveSnapshotCheckpointAuthoritative(ctx, key, snapshot, 0, fence)
}

// SaveSnapshotCheckpointAuthoritative grava ou substitui o snapshot do
// documento, persistindo o checkpoint `through` sob fencing autoritativo.
func (s *Store) SaveSnapshotCheckpointAuthoritative(
	ctx context.Context,
	key storage.DocumentKey,
	snapshot *yjsbridge.PersistedSnapshot,
	through storage.UpdateOffset,
	fence storage.AuthorityFence,
) (*storage.SnapshotRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := key.Validate(); err != nil {
		return nil, err
	}
	if snapshot == nil {
		return nil, storage.ErrNilPersistedSnapshot
	}
	if err := fence.Validate(); err != nil {
		return nil, err
	}
	pool, err := s.requirePool()
	if err != nil {
		return nil, err
	}

	payloadV1, payloadV2, err := encodePersistedSnapshotPayloads(snapshot)
	if err != nil {
		return nil, err
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	if err := s.validateAuthorityWriteTx(ctx, tx, key, fence, time.Now().UTC()); err != nil {
		return nil, err
	}

	storedAt, err := s.saveSnapshotTx(ctx, tx, key, payloadV1, payloadV2, through, fence.Owner.Epoch)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	return &storage.SnapshotRecord{
		Key:      key,
		Snapshot: snapshot.Clone(),
		Through:  through,
		Epoch:    fence.Owner.Epoch,
		StoredAt: storedAt,
	}, nil
}

// LoadSnapshot carrega o snapshot persistido do documento.
func (s *Store) LoadSnapshot(ctx context.Context, key storage.DocumentKey) (*storage.SnapshotRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := key.Validate(); err != nil {
		return nil, err
	}
	pool, err := s.requirePool()
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf(`
SELECT snapshot_v1, snapshot_v2, through_offset, owner_epoch, stored_at
FROM %s.document_snapshots
WHERE namespace = $1 AND document_id = $2
`, quoteIdentifier(s.schema))

	var payloadV1 []byte
	var payloadV2 []byte
	var through int64
	var epoch int64
	var storedAt time.Time
	if err := pool.QueryRow(ctx, query, key.Namespace, key.DocumentID).Scan(&payloadV1, &payloadV2, &through, &epoch, &storedAt); err != nil {
		if isNoRows(err) {
			return nil, storage.ErrSnapshotNotFound
		}
		return nil, err
	}

	snapshot, err := decodePersistedSnapshotPayload(payloadV1, payloadV2)
	if err != nil {
		return nil, err
	}
	throughOffset, err := int64ToOffset(through)
	if err != nil {
		return nil, err
	}
	epochValue, err := int64ToUint64("owner epoch", epoch)
	if err != nil {
		return nil, err
	}

	return &storage.SnapshotRecord{
		Key:      key,
		Snapshot: snapshot,
		Through:  throughOffset,
		Epoch:    epochValue,
		StoredAt: storedAt,
	}, nil
}

func isNoRows(err error) bool {
	return errors.Is(err, pgx.ErrNoRows)
}

func (s *Store) saveSnapshotPool(ctx context.Context, pool *pgxpool.Pool, key storage.DocumentKey, payloadV1, payloadV2 []byte, through storage.UpdateOffset, epoch uint64) (time.Time, error) {
	query, args, err := s.saveSnapshotQuery(key, payloadV1, payloadV2, through, epoch)
	if err != nil {
		return time.Time{}, err
	}

	var storedAt time.Time
	if err := pool.QueryRow(ctx, query, args...).Scan(&storedAt); err != nil {
		return time.Time{}, err
	}
	return storedAt, nil
}

func (s *Store) saveSnapshotTx(ctx context.Context, tx pgx.Tx, key storage.DocumentKey, payloadV1, payloadV2 []byte, through storage.UpdateOffset, epoch uint64) (time.Time, error) {
	query, args, err := s.saveSnapshotQuery(key, payloadV1, payloadV2, through, epoch)
	if err != nil {
		return time.Time{}, err
	}

	var storedAt time.Time
	if err := tx.QueryRow(ctx, query, args...).Scan(&storedAt); err != nil {
		return time.Time{}, err
	}
	return storedAt, nil
}

func (s *Store) saveSnapshotQuery(key storage.DocumentKey, payloadV1, payloadV2 []byte, through storage.UpdateOffset, epoch uint64) (string, []any, error) {
	throughOffset, err := offsetToInt64(through)
	if err != nil {
		return "", nil, err
	}
	epochValue, err := uint64ToInt64("owner epoch", epoch)
	if err != nil {
		return "", nil, err
	}

	query := fmt.Sprintf(`
INSERT INTO %s.document_snapshots(namespace, document_id, snapshot_v1, snapshot_v2, through_offset, owner_epoch, stored_at)
VALUES ($1, $2, $3, $4, $5, $6, now())
ON CONFLICT (namespace, document_id)
DO UPDATE SET snapshot_v1 = EXCLUDED.snapshot_v1, snapshot_v2 = EXCLUDED.snapshot_v2, through_offset = EXCLUDED.through_offset, owner_epoch = EXCLUDED.owner_epoch, stored_at = now()
RETURNING stored_at
`, quoteIdentifier(s.schema))
	return query, []any{key.Namespace, key.DocumentID, nullableBytes(payloadV1), nullableBytes(payloadV2), throughOffset, epochValue}, nil
}

func encodePersistedSnapshotPayloads(snapshot *yjsbridge.PersistedSnapshot) ([]byte, []byte, error) {
	payloadV1, err := yjsbridge.EncodePersistedSnapshotV1(snapshot)
	if err != nil {
		return nil, nil, err
	}
	payloadV2, err := yjsbridge.EncodePersistedSnapshotV2(snapshot)
	if err != nil {
		return nil, nil, err
	}
	return payloadV1, payloadV2, nil
}

func decodePersistedSnapshotPayload(payloadV1, payloadV2 []byte) (*yjsbridge.PersistedSnapshot, error) {
	if len(payloadV1) > 0 && len(payloadV2) > 0 {
		payloadV2AsV1, err := yjsbridge.ConvertUpdateToV1(payloadV2)
		if err != nil {
			return nil, err
		}
		merged, err := yjsbridge.MergeUpdates(payloadV1, payloadV2AsV1)
		if err != nil {
			return nil, err
		}
		return yjsbridge.PersistedSnapshotFromUpdate(merged)
	}
	if len(payloadV2) > 0 {
		return yjsbridge.DecodePersistedSnapshotV2(payloadV2)
	}
	return yjsbridge.DecodePersistedSnapshotV1(payloadV1)
}

func nullableBytes(payload []byte) any {
	if payload == nil {
		return nil
	}
	return payload
}
