package postgres

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/drksbr/yjs-crdt-golang-server/pkg/storage"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/yjsbridge"
)

var _ storage.DistributedStore = (*Store)(nil)
var _ storage.AuthoritativeUpdateLogStore = (*Store)(nil)
var _ storage.UpdateLogStoreV2 = (*Store)(nil)
var _ storage.AuthoritativeUpdateLogStoreV2 = (*Store)(nil)
var _ storage.PlacementListStore = (*Store)(nil)
var _ storage.LeaseHandoffStore = (*Store)(nil)

func (s *Store) AppendUpdate(ctx context.Context, key storage.DocumentKey, update []byte) (*storage.UpdateLogRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := key.Validate(); err != nil {
		return nil, err
	}
	if len(update) == 0 {
		return nil, storage.ErrInvalidUpdatePayload
	}
	pool, err := s.requirePool()
	if err != nil {
		return nil, err
	}
	updateV2, _ := yjsbridge.ConvertUpdateToV2(update)

	query := fmt.Sprintf(`
WITH allocated AS (
	INSERT INTO %s.document_update_log_heads AS heads (namespace, document_id, next_offset)
	VALUES ($1, $2, 2)
	ON CONFLICT (namespace, document_id)
	DO UPDATE SET next_offset = heads.next_offset + 1
	RETURNING next_offset - 1 AS log_offset
)
INSERT INTO %s.document_update_logs(namespace, document_id, log_offset, update_v1, update_v2, owner_epoch, stored_at)
SELECT $1, $2, allocated.log_offset, $3, $4, 0, now()
FROM allocated
RETURNING log_offset, owner_epoch, stored_at
`, quoteIdentifier(s.schema), quoteIdentifier(s.schema))

	var offset int64
	var epoch int64
	var storedAt time.Time
	if err := pool.QueryRow(ctx, query, key.Namespace, key.DocumentID, update, nullableBytes(updateV2)).Scan(&offset, &epoch, &storedAt); err != nil {
		return nil, err
	}

	logOffset, err := int64ToOffset(offset)
	if err != nil {
		return nil, err
	}
	epochValue, err := int64ToUint64("owner epoch", epoch)
	if err != nil {
		return nil, err
	}

	return &storage.UpdateLogRecord{
		Key:      key,
		Offset:   logOffset,
		UpdateV1: append([]byte(nil), update...),
		UpdateV2: append([]byte(nil), updateV2...),
		Epoch:    epochValue,
		StoredAt: storedAt,
	}, nil
}

func (s *Store) AppendUpdateV2(ctx context.Context, key storage.DocumentKey, update []byte) (*storage.UpdateLogRecord, error) {
	return s.appendUpdateV2(ctx, key, update, nil, 0, nil)
}

// AppendUpdateAuthoritative adiciona um update V1 ao fim do log do documento,
// exigindo que o placement + lease persistidos ainda correspondam ao fence.
func (s *Store) AppendUpdateAuthoritative(
	ctx context.Context,
	key storage.DocumentKey,
	update []byte,
	fence storage.AuthorityFence,
) (*storage.UpdateLogRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := key.Validate(); err != nil {
		return nil, err
	}
	if len(update) == 0 {
		return nil, storage.ErrInvalidUpdatePayload
	}
	if err := fence.Validate(); err != nil {
		return nil, err
	}
	pool, err := s.requirePool()
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

	logOffset, storedAt, err := s.appendUpdateTx(ctx, tx, key, update, nil, fence.Owner.Epoch)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	return &storage.UpdateLogRecord{
		Key:      key,
		Offset:   logOffset,
		UpdateV1: append([]byte(nil), update...),
		Epoch:    fence.Owner.Epoch,
		StoredAt: storedAt,
	}, nil
}

func (s *Store) AppendUpdateV2Authoritative(
	ctx context.Context,
	key storage.DocumentKey,
	update []byte,
	fence storage.AuthorityFence,
) (*storage.UpdateLogRecord, error) {
	return s.appendUpdateV2(ctx, key, update, nil, fence.Owner.Epoch, &fence)
}

func (s *Store) appendUpdateV2(
	ctx context.Context,
	key storage.DocumentKey,
	updateV2 []byte,
	updateV1 []byte,
	epoch uint64,
	fence *storage.AuthorityFence,
) (*storage.UpdateLogRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := key.Validate(); err != nil {
		return nil, err
	}
	if len(updateV2) == 0 {
		return nil, storage.ErrInvalidUpdatePayload
	}
	if fence != nil {
		if err := fence.Validate(); err != nil {
			return nil, err
		}
	}
	pool, err := s.requirePool()
	if err != nil {
		return nil, err
	}
	if fence == nil {
		logOffset, storedAt, err := s.appendUpdatePool(ctx, pool, key, updateV1, updateV2, epoch)
		if err != nil {
			return nil, err
		}
		return &storage.UpdateLogRecord{
			Key:      key,
			Offset:   logOffset,
			UpdateV2: append([]byte(nil), updateV2...),
			UpdateV1: append([]byte(nil), updateV1...),
			Epoch:    epoch,
			StoredAt: storedAt,
		}, nil
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback(context.Background())
	}()
	if err := s.validateAuthorityWriteTx(ctx, tx, key, *fence, time.Now().UTC()); err != nil {
		return nil, err
	}
	logOffset, storedAt, err := s.appendUpdateTx(ctx, tx, key, updateV1, updateV2, epoch)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &storage.UpdateLogRecord{
		Key:      key,
		Offset:   logOffset,
		UpdateV2: append([]byte(nil), updateV2...),
		UpdateV1: append([]byte(nil), updateV1...),
		Epoch:    epoch,
		StoredAt: storedAt,
	}, nil
}

func (s *Store) ListUpdates(ctx context.Context, key storage.DocumentKey, after storage.UpdateOffset, limit int) ([]*storage.UpdateLogRecord, error) {
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

	afterOffset, err := offsetToInt64(after)
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf(`
SELECT log_offset, update_v1, update_v2, owner_epoch, stored_at
FROM %s.document_update_logs
WHERE namespace = $1 AND document_id = $2 AND log_offset > $3
ORDER BY log_offset ASC
`, quoteIdentifier(s.schema))

	args := []any{key.Namespace, key.DocumentID, afterOffset}
	if limit > 0 {
		query += "LIMIT $4"
		args = append(args, limit)
	}

	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []*storage.UpdateLogRecord
	if limit > 0 {
		records = make([]*storage.UpdateLogRecord, 0, limit)
	}
	for rows.Next() {
		var offset int64
		var payloadV1 []byte
		var payloadV2 []byte
		var epoch int64
		var storedAt time.Time
		if err := rows.Scan(&offset, &payloadV1, &payloadV2, &epoch, &storedAt); err != nil {
			return nil, err
		}

		logOffset, err := int64ToOffset(offset)
		if err != nil {
			return nil, err
		}
		epochValue, err := int64ToUint64("owner epoch", epoch)
		if err != nil {
			return nil, err
		}

		records = append(records, &storage.UpdateLogRecord{
			Key:      key,
			Offset:   logOffset,
			UpdateV2: append([]byte(nil), payloadV2...),
			UpdateV1: append([]byte(nil), payloadV1...),
			Epoch:    epochValue,
			StoredAt: storedAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func (s *Store) TrimUpdates(ctx context.Context, key storage.DocumentKey, through storage.UpdateOffset) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := key.Validate(); err != nil {
		return err
	}
	pool, err := s.requirePool()
	if err != nil {
		return err
	}

	throughOffset, err := offsetToInt64(through)
	if err != nil {
		return err
	}

	query := fmt.Sprintf(`
DELETE FROM %s.document_update_logs
WHERE namespace = $1 AND document_id = $2 AND log_offset <= $3
`, quoteIdentifier(s.schema))
	_, err = pool.Exec(ctx, query, key.Namespace, key.DocumentID, throughOffset)
	return err
}

// TrimUpdatesAuthoritative remove registros com offset <= through, exigindo
// que o placement + lease persistidos ainda correspondam ao fence.
func (s *Store) TrimUpdatesAuthoritative(
	ctx context.Context,
	key storage.DocumentKey,
	through storage.UpdateOffset,
	fence storage.AuthorityFence,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := key.Validate(); err != nil {
		return err
	}
	if err := fence.Validate(); err != nil {
		return err
	}
	pool, err := s.requirePool()
	if err != nil {
		return err
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	if err := s.validateAuthorityWriteTx(ctx, tx, key, fence, time.Now().UTC()); err != nil {
		return err
	}

	if err := s.trimUpdatesTx(ctx, tx, key, through); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) SavePlacement(ctx context.Context, placement storage.PlacementRecord) (*storage.PlacementRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := placement.Validate(); err != nil {
		return nil, err
	}
	pool, err := s.requirePool()
	if err != nil {
		return nil, err
	}

	version, err := uint64ToInt64("version", placement.Version)
	if err != nil {
		return nil, err
	}
	updatedAt := normalizeTime(placement.UpdatedAt)

	query := fmt.Sprintf(`
INSERT INTO %s.document_placements(namespace, document_id, shard_id, version, updated_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (namespace, document_id)
DO UPDATE SET shard_id = EXCLUDED.shard_id, version = EXCLUDED.version, updated_at = EXCLUDED.updated_at
RETURNING version, updated_at
`, quoteIdentifier(s.schema))

	var storedVersion int64
	var storedAt time.Time
	if err := pool.QueryRow(ctx, query, placement.Key.Namespace, placement.Key.DocumentID, placement.ShardID, version, updatedAt).Scan(&storedVersion, &storedAt); err != nil {
		return nil, err
	}

	normalizedVersion, err := int64ToUint64("version", storedVersion)
	if err != nil {
		return nil, err
	}

	return &storage.PlacementRecord{
		Key:       placement.Key,
		ShardID:   placement.ShardID,
		Version:   normalizedVersion,
		UpdatedAt: storedAt,
	}, nil
}

func (s *Store) LoadPlacement(ctx context.Context, key storage.DocumentKey) (*storage.PlacementRecord, error) {
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
SELECT shard_id, version, updated_at
FROM %s.document_placements
WHERE namespace = $1 AND document_id = $2
`, quoteIdentifier(s.schema))

	var shardID string
	var version int64
	var updatedAt time.Time
	if err := pool.QueryRow(ctx, query, key.Namespace, key.DocumentID).Scan(&shardID, &version, &updatedAt); err != nil {
		if isNoRows(err) {
			return nil, storage.ErrPlacementNotFound
		}
		return nil, err
	}

	normalizedVersion, err := int64ToUint64("version", version)
	if err != nil {
		return nil, err
	}

	return &storage.PlacementRecord{
		Key:       key,
		ShardID:   storage.ShardID(shardID),
		Version:   normalizedVersion,
		UpdatedAt: updatedAt,
	}, nil
}

func (s *Store) ListPlacements(ctx context.Context, opts storage.PlacementListOptions) ([]*storage.PlacementRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	pool, err := s.requirePool()
	if err != nil {
		return nil, err
	}

	args := make([]any, 0, 2)
	query := fmt.Sprintf(`
SELECT namespace, document_id, shard_id, version, updated_at
FROM %s.document_placements
`, quoteIdentifier(s.schema))
	if opts.Namespace != "" {
		args = append(args, opts.Namespace)
		query += fmt.Sprintf("WHERE namespace = $%d\n", len(args))
	}
	query += "ORDER BY namespace, document_id\n"
	if opts.Limit > 0 {
		args = append(args, opts.Limit)
		query += fmt.Sprintf("LIMIT $%d\n", len(args))
	}

	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	placements := make([]*storage.PlacementRecord, 0)
	for rows.Next() {
		var namespace string
		var documentID string
		var shardID string
		var version int64
		var updatedAt time.Time
		if err := rows.Scan(&namespace, &documentID, &shardID, &version, &updatedAt); err != nil {
			return nil, err
		}
		normalizedVersion, err := int64ToUint64("version", version)
		if err != nil {
			return nil, err
		}
		record := &storage.PlacementRecord{
			Key: storage.DocumentKey{
				Namespace:  namespace,
				DocumentID: documentID,
			},
			ShardID:   storage.ShardID(shardID),
			Version:   normalizedVersion,
			UpdatedAt: updatedAt,
		}
		if err := record.Validate(); err != nil {
			return nil, err
		}
		placements = append(placements, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return placements, nil
}

func (s *Store) SaveLease(ctx context.Context, lease storage.LeaseRecord) (*storage.LeaseRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	if lease.AcquiredAt.IsZero() {
		lease.AcquiredAt = now
	}
	if err := lease.Validate(); err != nil {
		return nil, err
	}
	opTime := lease.AcquiredAt
	pool, err := s.requirePool()
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

	if err := s.ensureLeaseGenerationLockTx(ctx, tx, lease.ShardID); err != nil {
		return nil, err
	}

	current, lastEpoch, err := s.loadLeaseStateTx(ctx, tx, lease.ShardID)
	if err != nil {
		return nil, err
	}

	switch {
	case current == nil:
		if lastEpoch > 0 && lease.Owner.Epoch <= lastEpoch {
			return nil, storage.NewLeaseStaleEpochError(lease.ShardID, lastEpoch, lease.Owner.Epoch)
		}
	case sameLeaseRecord(current, &lease):
		// renew/update of the same active generation keeps the original start time.
		lease.AcquiredAt = current.AcquiredAt
	case current.ExpiresAt.After(opTime):
		if lease.Owner.Epoch <= current.Owner.Epoch {
			return nil, storage.NewLeaseStaleEpochError(lease.ShardID, current.Owner.Epoch, lease.Owner.Epoch)
		}
		return nil, fmt.Errorf("%w: shard %s token %q", storage.ErrLeaseConflict, lease.ShardID, current.Token)
	default:
		if lastEpoch < current.Owner.Epoch {
			lastEpoch = current.Owner.Epoch
		}
		if lease.Owner.Epoch <= lastEpoch {
			return nil, storage.NewLeaseStaleEpochError(lease.ShardID, lastEpoch, lease.Owner.Epoch)
		}
	}

	epoch, err := uint64ToInt64("owner epoch", lease.Owner.Epoch)
	if err != nil {
		return nil, err
	}
	acquiredAt := normalizeTime(lease.AcquiredAt)

	query := fmt.Sprintf(`
INSERT INTO %s.shard_leases(shard_id, owner_node_id, owner_epoch, token, acquired_at, expires_at)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (shard_id)
DO UPDATE SET owner_node_id = EXCLUDED.owner_node_id, owner_epoch = EXCLUDED.owner_epoch, token = EXCLUDED.token, acquired_at = EXCLUDED.acquired_at, expires_at = EXCLUDED.expires_at
RETURNING owner_epoch, acquired_at, expires_at
`, quoteIdentifier(s.schema))

	var storedEpoch int64
	var storedAcquiredAt time.Time
	var storedExpiresAt time.Time
	if err := tx.QueryRow(ctx, query, lease.ShardID, lease.Owner.NodeID, epoch, lease.Token, acquiredAt, lease.ExpiresAt).Scan(&storedEpoch, &storedAcquiredAt, &storedExpiresAt); err != nil {
		return nil, err
	}

	normalizedEpoch, err := int64ToUint64("owner epoch", storedEpoch)
	if err != nil {
		return nil, err
	}
	if err := s.upsertLeaseGenerationTx(ctx, tx, lease.ShardID, normalizedEpoch); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	return &storage.LeaseRecord{
		ShardID: lease.ShardID,
		Owner: storage.OwnerInfo{
			NodeID: lease.Owner.NodeID,
			Epoch:  normalizedEpoch,
		},
		Token:      lease.Token,
		AcquiredAt: storedAcquiredAt,
		ExpiresAt:  storedExpiresAt,
	}, nil
}

// HandoffLease transfere atomicamente a lease ativa para o próximo owner,
// exigindo token atual correto e epoch exatamente monotônico.
func (s *Store) HandoffLease(
	ctx context.Context,
	shardID storage.ShardID,
	currentToken string,
	next storage.LeaseRecord,
) (*storage.LeaseRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := shardID.Validate(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(currentToken) == "" {
		return nil, fmt.Errorf("%w: currentToken obrigatorio", storage.ErrInvalidLeaseToken)
	}
	now := time.Now().UTC()
	if next.AcquiredAt.IsZero() {
		next.AcquiredAt = now
	}
	if next.ShardID != shardID {
		return nil, fmt.Errorf("%w: next shard %q != %q", storage.ErrInvalidShardID, next.ShardID, shardID)
	}
	if err := next.Validate(); err != nil {
		return nil, err
	}
	if !next.ExpiresAt.After(next.AcquiredAt) {
		return nil, fmt.Errorf("%w: expiresAt deve ser apos acquiredAt", storage.ErrInvalidLeaseExpiry)
	}

	pool, err := s.requirePool()
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

	if err := s.ensureLeaseGenerationLockTx(ctx, tx, shardID); err != nil {
		return nil, err
	}

	current, lastEpoch, err := s.loadLeaseStateTx(ctx, tx, shardID)
	if err != nil {
		return nil, err
	}
	if current == nil {
		return nil, storage.ErrLeaseNotFound
	}
	if current.Token != currentToken {
		return nil, fmt.Errorf("%w: shard %s token %q", storage.ErrLeaseConflict, shardID, current.Token)
	}
	if !current.ExpiresAt.After(next.AcquiredAt) {
		return nil, fmt.Errorf("%w: shard %s lease expirada", storage.ErrLeaseConflict, shardID)
	}
	expectedEpoch := current.Owner.Epoch + 1
	if next.Owner.Epoch != expectedEpoch {
		return nil, storage.NewLeaseStaleEpochError(shardID, current.Owner.Epoch, next.Owner.Epoch)
	}
	if next.Owner.Epoch <= lastEpoch {
		return nil, storage.NewLeaseStaleEpochError(shardID, lastEpoch, next.Owner.Epoch)
	}

	epoch, err := uint64ToInt64("owner epoch", next.Owner.Epoch)
	if err != nil {
		return nil, err
	}
	acquiredAt := normalizeTime(next.AcquiredAt)
	query := fmt.Sprintf(`
INSERT INTO %s.shard_leases(shard_id, owner_node_id, owner_epoch, token, acquired_at, expires_at)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (shard_id)
DO UPDATE SET owner_node_id = EXCLUDED.owner_node_id, owner_epoch = EXCLUDED.owner_epoch, token = EXCLUDED.token, acquired_at = EXCLUDED.acquired_at, expires_at = EXCLUDED.expires_at
RETURNING owner_epoch, acquired_at, expires_at
`, quoteIdentifier(s.schema))

	var storedEpoch int64
	var storedAcquiredAt time.Time
	var storedExpiresAt time.Time
	if err := tx.QueryRow(ctx, query, next.ShardID, next.Owner.NodeID, epoch, next.Token, acquiredAt, next.ExpiresAt).Scan(&storedEpoch, &storedAcquiredAt, &storedExpiresAt); err != nil {
		return nil, err
	}

	normalizedEpoch, err := int64ToUint64("owner epoch", storedEpoch)
	if err != nil {
		return nil, err
	}
	if err := s.upsertLeaseGenerationTx(ctx, tx, shardID, normalizedEpoch); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	return &storage.LeaseRecord{
		ShardID: next.ShardID,
		Owner: storage.OwnerInfo{
			NodeID: next.Owner.NodeID,
			Epoch:  normalizedEpoch,
		},
		Token:      next.Token,
		AcquiredAt: storedAcquiredAt,
		ExpiresAt:  storedExpiresAt,
	}, nil
}

func (s *Store) LoadLease(ctx context.Context, shardID storage.ShardID) (*storage.LeaseRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := shardID.Validate(); err != nil {
		return nil, err
	}
	pool, err := s.requirePool()
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf(`
SELECT owner_node_id, owner_epoch, token, acquired_at, expires_at
FROM %s.shard_leases
WHERE shard_id = $1
`, quoteIdentifier(s.schema))

	var nodeID string
	var epoch int64
	var token string
	var acquiredAt time.Time
	var expiresAt time.Time
	if err := pool.QueryRow(ctx, query, shardID).Scan(&nodeID, &epoch, &token, &acquiredAt, &expiresAt); err != nil {
		if isNoRows(err) {
			return nil, storage.ErrLeaseNotFound
		}
		return nil, err
	}

	normalizedEpoch, err := int64ToUint64("owner epoch", epoch)
	if err != nil {
		return nil, err
	}

	return &storage.LeaseRecord{
		ShardID: shardID,
		Owner: storage.OwnerInfo{
			NodeID: storage.NodeID(nodeID),
			Epoch:  normalizedEpoch,
		},
		Token:      token,
		AcquiredAt: acquiredAt,
		ExpiresAt:  expiresAt,
	}, nil
}

func (s *Store) ReleaseLease(ctx context.Context, shardID storage.ShardID, token string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := shardID.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(token) == "" {
		return fmt.Errorf("%w: token obrigatorio", storage.ErrInvalidLeaseToken)
	}
	pool, err := s.requirePool()
	if err != nil {
		return err
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	current, _, err := s.loadLeaseStateTx(ctx, tx, shardID)
	if err != nil {
		return err
	}
	if current == nil || current.Token != token {
		return storage.ErrLeaseNotFound
	}
	if err := s.upsertLeaseGenerationTx(ctx, tx, shardID, current.Owner.Epoch); err != nil {
		return err
	}

	query := fmt.Sprintf(`DELETE FROM %s.shard_leases WHERE shard_id = $1 AND token = $2`, quoteIdentifier(s.schema))
	tag, err := tx.Exec(ctx, query, shardID, token)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return storage.ErrLeaseNotFound
	}
	return tx.Commit(ctx)
}

func normalizeTime(ts time.Time) time.Time {
	if ts.IsZero() {
		return time.Now().UTC()
	}
	return ts.UTC()
}

func (s *Store) loadLeaseStateTx(ctx context.Context, tx pgx.Tx, shardID storage.ShardID) (*storage.LeaseRecord, uint64, error) {
	generationQuery := fmt.Sprintf(`
SELECT last_epoch
FROM %s.shard_lease_generations
WHERE shard_id = $1
FOR UPDATE
`, quoteIdentifier(s.schema))

	var lastEpochInt int64
	err := tx.QueryRow(ctx, generationQuery, shardID).Scan(&lastEpochInt)
	lastEpoch := uint64(0)
	switch {
	case err == nil:
		lastEpoch, err = int64ToUint64("lease generation", lastEpochInt)
		if err != nil {
			return nil, 0, err
		}
	case isNoRows(err):
	default:
		return nil, 0, err
	}

	leaseQuery := fmt.Sprintf(`
SELECT owner_node_id, owner_epoch, token, acquired_at, expires_at
FROM %s.shard_leases
WHERE shard_id = $1
FOR UPDATE
`, quoteIdentifier(s.schema))

	var nodeID string
	var epoch int64
	var token string
	var acquiredAt time.Time
	var expiresAt time.Time
	err = tx.QueryRow(ctx, leaseQuery, shardID).Scan(&nodeID, &epoch, &token, &acquiredAt, &expiresAt)
	switch {
	case isNoRows(err):
		return nil, lastEpoch, nil
	case err != nil:
		return nil, 0, err
	}

	normalizedEpoch, err := int64ToUint64("owner epoch", epoch)
	if err != nil {
		return nil, 0, err
	}
	return &storage.LeaseRecord{
		ShardID: shardID,
		Owner: storage.OwnerInfo{
			NodeID: storage.NodeID(nodeID),
			Epoch:  normalizedEpoch,
		},
		Token:      token,
		AcquiredAt: acquiredAt,
		ExpiresAt:  expiresAt,
	}, lastEpoch, nil
}

func (s *Store) ensureLeaseGenerationLockTx(ctx context.Context, tx pgx.Tx, shardID storage.ShardID) error {
	query := fmt.Sprintf(`
INSERT INTO %s.shard_lease_generations(shard_id, last_epoch, updated_at)
VALUES ($1, 0, now())
ON CONFLICT (shard_id)
DO NOTHING
`, quoteIdentifier(s.schema))
	_, err := tx.Exec(ctx, query, shardID)
	return err
}

func (s *Store) upsertLeaseGenerationTx(ctx context.Context, tx pgx.Tx, shardID storage.ShardID, epoch uint64) error {
	epochInt, err := uint64ToInt64("lease generation", epoch)
	if err != nil {
		return err
	}
	query := fmt.Sprintf(`
INSERT INTO %s.shard_lease_generations(shard_id, last_epoch, updated_at)
VALUES ($1, $2, now())
ON CONFLICT (shard_id)
DO UPDATE SET last_epoch = GREATEST(%s.shard_lease_generations.last_epoch, EXCLUDED.last_epoch), updated_at = now()
`, quoteIdentifier(s.schema), quoteIdentifier(s.schema))
	_, err = tx.Exec(ctx, query, shardID, epochInt)
	return err
}

func sameLeaseRecord(current, next *storage.LeaseRecord) bool {
	if current == nil || next == nil {
		return false
	}
	return current.ShardID == next.ShardID &&
		current.Owner == next.Owner &&
		current.Token == next.Token
}

func offsetToInt64(offset storage.UpdateOffset) (int64, error) {
	return uint64ToInt64("update offset", uint64(offset))
}

func int64ToOffset(offset int64) (storage.UpdateOffset, error) {
	value, err := int64ToUint64("update offset", offset)
	if err != nil {
		return 0, err
	}
	return storage.UpdateOffset(value), nil
}

func uint64ToInt64(name string, value uint64) (int64, error) {
	if value > math.MaxInt64 {
		return 0, fmt.Errorf("postgres: %s fora do intervalo bigint: %d", name, value)
	}
	return int64(value), nil
}

func int64ToUint64(name string, value int64) (uint64, error) {
	if value < 0 {
		return 0, fmt.Errorf("postgres: %s negativo no banco: %d", name, value)
	}
	return uint64(value), nil
}

func (s *Store) appendUpdatePool(ctx context.Context, pool queryRower, key storage.DocumentKey, updateV1, updateV2 []byte, epoch uint64) (storage.UpdateOffset, time.Time, error) {
	epochValue, err := uint64ToInt64("owner epoch", epoch)
	if err != nil {
		return 0, time.Time{}, err
	}
	query := fmt.Sprintf(`
WITH allocated AS (
	INSERT INTO %s.document_update_log_heads AS heads (namespace, document_id, next_offset)
	VALUES ($1, $2, 2)
	ON CONFLICT (namespace, document_id)
	DO UPDATE SET next_offset = heads.next_offset + 1
	RETURNING next_offset - 1 AS log_offset
)
INSERT INTO %s.document_update_logs(namespace, document_id, log_offset, update_v1, update_v2, owner_epoch, stored_at)
SELECT $1, $2, allocated.log_offset, $3, $4, $5, now()
FROM allocated
RETURNING log_offset, stored_at
`, quoteIdentifier(s.schema), quoteIdentifier(s.schema))

	var offset int64
	var storedAt time.Time
	if err := pool.QueryRow(ctx, query, key.Namespace, key.DocumentID, nullableBytes(updateV1), nullableBytes(updateV2), epochValue).Scan(&offset, &storedAt); err != nil {
		return 0, time.Time{}, err
	}
	logOffset, err := int64ToOffset(offset)
	if err != nil {
		return 0, time.Time{}, err
	}
	return logOffset, storedAt, nil
}

func (s *Store) appendUpdateTx(ctx context.Context, tx pgx.Tx, key storage.DocumentKey, updateV1, updateV2 []byte, epoch uint64) (storage.UpdateOffset, time.Time, error) {
	epochValue, err := uint64ToInt64("owner epoch", epoch)
	if err != nil {
		return 0, time.Time{}, err
	}

	query := fmt.Sprintf(`
WITH allocated AS (
	INSERT INTO %s.document_update_log_heads AS heads (namespace, document_id, next_offset)
	VALUES ($1, $2, 2)
	ON CONFLICT (namespace, document_id)
	DO UPDATE SET next_offset = heads.next_offset + 1
	RETURNING next_offset - 1 AS log_offset
)
INSERT INTO %s.document_update_logs(namespace, document_id, log_offset, update_v1, update_v2, owner_epoch, stored_at)
SELECT $1, $2, allocated.log_offset, $3, $4, $5, now()
FROM allocated
RETURNING log_offset, stored_at
`, quoteIdentifier(s.schema), quoteIdentifier(s.schema))

	var offset int64
	var storedAt time.Time
	if err := tx.QueryRow(ctx, query, key.Namespace, key.DocumentID, nullableBytes(updateV1), nullableBytes(updateV2), epochValue).Scan(&offset, &storedAt); err != nil {
		return 0, time.Time{}, err
	}

	logOffset, err := int64ToOffset(offset)
	if err != nil {
		return 0, time.Time{}, err
	}
	return logOffset, storedAt, nil
}

type queryRower interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func (s *Store) trimUpdatesTx(ctx context.Context, tx pgx.Tx, key storage.DocumentKey, through storage.UpdateOffset) error {
	throughOffset, err := offsetToInt64(through)
	if err != nil {
		return err
	}

	query := fmt.Sprintf(`
DELETE FROM %s.document_update_logs
WHERE namespace = $1 AND document_id = $2 AND log_offset <= $3
`, quoteIdentifier(s.schema))
	_, err = tx.Exec(ctx, query, key.Namespace, key.DocumentID, throughOffset)
	return err
}

func (s *Store) validateAuthorityWriteTx(
	ctx context.Context,
	tx pgx.Tx,
	key storage.DocumentKey,
	fence storage.AuthorityFence,
	now time.Time,
) error {
	placementQuery := fmt.Sprintf(`
SELECT shard_id
FROM %s.document_placements
WHERE namespace = $1 AND document_id = $2
FOR UPDATE
`, quoteIdentifier(s.schema))

	var placementShardID string
	if err := tx.QueryRow(ctx, placementQuery, key.Namespace, key.DocumentID).Scan(&placementShardID); err != nil {
		if isNoRows(err) {
			return fmt.Errorf("%w: placement ausente para %s/%s", storage.ErrAuthorityLost, key.Namespace, key.DocumentID)
		}
		return err
	}
	if storage.ShardID(placementShardID) != fence.ShardID {
		return fmt.Errorf(
			"%w: placement shard %s != fence shard %s para %s/%s",
			storage.ErrAuthorityLost,
			placementShardID,
			fence.ShardID,
			key.Namespace,
			key.DocumentID,
		)
	}

	lease, err := s.loadLeaseForAuthorityWriteTx(ctx, tx, fence.ShardID)
	if err != nil {
		return err
	}
	if lease == nil {
		return fmt.Errorf("%w: lease ausente para shard %s", storage.ErrAuthorityLost, fence.ShardID)
	}
	if lease.Owner != fence.Owner {
		return fmt.Errorf(
			"%w: owner atual %s/%d != fence %s/%d para shard %s",
			storage.ErrAuthorityLost,
			lease.Owner.NodeID,
			lease.Owner.Epoch,
			fence.Owner.NodeID,
			fence.Owner.Epoch,
			fence.ShardID,
		)
	}
	if lease.Token != fence.Token {
		return fmt.Errorf("%w: token atual nao corresponde ao fence do shard %s", storage.ErrAuthorityLost, fence.ShardID)
	}
	if !lease.ExpiresAt.After(now) {
		return fmt.Errorf("%w: lease expirada para shard %s", storage.ErrAuthorityLost, fence.ShardID)
	}
	return nil
}

func (s *Store) loadLeaseForAuthorityWriteTx(ctx context.Context, tx pgx.Tx, shardID storage.ShardID) (*storage.LeaseRecord, error) {
	leaseQuery := fmt.Sprintf(`
SELECT owner_node_id, owner_epoch, token, acquired_at, expires_at
FROM %s.shard_leases
WHERE shard_id = $1
FOR UPDATE
`, quoteIdentifier(s.schema))

	var nodeID string
	var epoch int64
	var token string
	var acquiredAt time.Time
	var expiresAt time.Time
	err := tx.QueryRow(ctx, leaseQuery, shardID).Scan(&nodeID, &epoch, &token, &acquiredAt, &expiresAt)
	switch {
	case isNoRows(err):
		return nil, nil
	case err != nil:
		return nil, err
	}

	normalizedEpoch, err := int64ToUint64("owner epoch", epoch)
	if err != nil {
		return nil, err
	}
	return &storage.LeaseRecord{
		ShardID: shardID,
		Owner: storage.OwnerInfo{
			NodeID: storage.NodeID(nodeID),
			Epoch:  normalizedEpoch,
		},
		Token:      token,
		AcquiredAt: acquiredAt,
		ExpiresAt:  expiresAt,
	}, nil
}
