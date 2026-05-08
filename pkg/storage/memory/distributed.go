package memory

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/drksbr/yjs-crdt-golang-server/pkg/storage"
	"github.com/drksbr/yjs-crdt-golang-server/pkg/yjsbridge"
)

var (
	_ storage.UpdateLogStore                = (*Store)(nil)
	_ storage.UpdateLogStoreV2              = (*Store)(nil)
	_ storage.AuthoritativeUpdateLogStore   = (*Store)(nil)
	_ storage.AuthoritativeUpdateLogStoreV2 = (*Store)(nil)
	_ storage.PlacementStore                = (*Store)(nil)
	_ storage.PlacementListStore            = (*Store)(nil)
	_ storage.LeaseStore                    = (*Store)(nil)
	_ storage.LeaseHandoffStore             = (*Store)(nil)
	_ storage.DistributedStore              = (*Store)(nil)
)

// AppendUpdate adiciona um update V1 ao fim do log incremental do documento.
func (s *Store) AppendUpdate(ctx context.Context, key storage.DocumentKey, update []byte) (*storage.UpdateLogRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s == nil {
		return nil, errNilStore
	}
	if err := key.Validate(); err != nil {
		return nil, err
	}
	if len(update) == 0 {
		return nil, fmt.Errorf("%w: updateV1 obrigatorio", storage.ErrInvalidUpdatePayload)
	}
	updateV2, _ := yjsbridge.ConvertUpdateToV2(update)

	s.mu.Lock()
	defer s.mu.Unlock()

	s.ensureStoreInitializedLocked()

	offset := s.updateNext[key] + 1
	record := newUpdateLogRecord(key, offset, update, updateV2, 0, s.nowTime())
	s.updateLogs[key] = append(s.updateLogs[key], record)
	s.updateNext[key] = offset
	return record.Clone(), nil
}

// AppendUpdateV2 adiciona um update V2 ao fim do log incremental do documento.
func (s *Store) AppendUpdateV2(ctx context.Context, key storage.DocumentKey, update []byte) (*storage.UpdateLogRecord, error) {
	updateV1, err := yjsbridge.ConvertUpdateToV1(update)
	if err != nil {
		return nil, err
	}
	return s.appendUpdateV2(ctx, key, update, updateV1, 0, nil)
}

// AppendUpdateAuthoritative adiciona um update V1 ao fim do log incremental do
// documento, exigindo que o placement + lease persistidos correspondam ao
// fence informado.
func (s *Store) AppendUpdateAuthoritative(
	ctx context.Context,
	key storage.DocumentKey,
	update []byte,
	fence storage.AuthorityFence,
) (*storage.UpdateLogRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s == nil {
		return nil, errNilStore
	}
	if err := key.Validate(); err != nil {
		return nil, err
	}
	if len(update) == 0 {
		return nil, fmt.Errorf("%w: updateV1 obrigatorio", storage.ErrInvalidUpdatePayload)
	}
	if err := fence.Validate(); err != nil {
		return nil, err
	}
	updateV2, _ := yjsbridge.ConvertUpdateToV2(update)

	now := s.nowTime()

	s.mu.Lock()
	defer s.mu.Unlock()

	s.ensureStoreInitializedLocked()
	if err := s.validateAuthorityLocked(key, fence, now); err != nil {
		return nil, err
	}

	offset := s.updateNext[key] + 1
	record := newUpdateLogRecord(key, offset, update, updateV2, fence.Owner.Epoch, now)
	s.updateLogs[key] = append(s.updateLogs[key], record)
	s.updateNext[key] = offset
	return record.Clone(), nil
}

// AppendUpdateV2Authoritative adiciona um update V2 ao log sob fencing.
func (s *Store) AppendUpdateV2Authoritative(
	ctx context.Context,
	key storage.DocumentKey,
	update []byte,
	fence storage.AuthorityFence,
) (*storage.UpdateLogRecord, error) {
	updateV1, err := yjsbridge.ConvertUpdateToV1(update)
	if err != nil {
		return nil, err
	}
	return s.appendUpdateV2(ctx, key, update, updateV1, fence.Owner.Epoch, &fence)
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
	if s == nil {
		return nil, errNilStore
	}
	if err := key.Validate(); err != nil {
		return nil, err
	}
	if len(updateV2) == 0 {
		return nil, fmt.Errorf("%w: updateV2 obrigatorio", storage.ErrInvalidUpdatePayload)
	}
	if fence != nil {
		if err := fence.Validate(); err != nil {
			return nil, err
		}
	}

	now := s.nowTime()
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ensureStoreInitializedLocked()
	if fence != nil {
		if err := s.validateAuthorityLocked(key, *fence, now); err != nil {
			return nil, err
		}
	}
	offset := s.updateNext[key] + 1
	record := newUpdateLogRecord(key, offset, updateV1, updateV2, epoch, now)
	s.updateLogs[key] = append(s.updateLogs[key], record)
	s.updateNext[key] = offset
	return record.Clone(), nil
}

func newUpdateLogRecord(key storage.DocumentKey, offset storage.UpdateOffset, updateV1, updateV2 []byte, epoch uint64, storedAt time.Time) *storage.UpdateLogRecord {
	record := &storage.UpdateLogRecord{
		Key:      key,
		Offset:   offset,
		Epoch:    epoch,
		StoredAt: storedAt,
	}
	if len(updateV1) > 0 {
		record.UpdateV1 = append([]byte(nil), updateV1...)
	}
	if len(updateV2) > 0 {
		record.UpdateV2 = append([]byte(nil), updateV2...)
	}
	return record
}

// ListUpdates lista updates com offset estritamente maior que after.
func (s *Store) ListUpdates(ctx context.Context, key storage.DocumentKey, after storage.UpdateOffset, limit int) ([]*storage.UpdateLogRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s == nil {
		return nil, errNilStore
	}
	if err := key.Validate(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	records := s.updateLogs[key]
	if len(records) == 0 {
		s.mu.RUnlock()
		return nil, nil
	}

	start := firstUpdateLogIndexAfter(records, after)
	if start >= len(records) {
		s.mu.RUnlock()
		return nil, nil
	}

	maxResults := len(records) - start
	if limit > 0 && limit < maxResults {
		maxResults = limit
	}
	selected := make([]*storage.UpdateLogRecord, maxResults)
	copy(selected, records[start:start+maxResults])
	s.mu.RUnlock()

	result := make([]*storage.UpdateLogRecord, len(selected))
	for idx, record := range selected {
		result[idx] = record.Clone()
	}
	return result, nil
}

// TrimUpdates remove registros com offset menor ou igual ao limite informado.
func (s *Store) TrimUpdates(ctx context.Context, key storage.DocumentKey, through storage.UpdateOffset) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil {
		return errNilStore
	}
	if err := key.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.ensureStoreInitializedLocked()

	records := s.updateLogs[key]
	if len(records) == 0 {
		return nil
	}

	firstRemaining := firstUpdateLogIndexAfter(records, through)
	if firstRemaining == 0 {
		return nil
	}
	if firstRemaining >= len(records) {
		delete(s.updateLogs, key)
		return nil
	}

	s.updateLogs[key] = trimUpdateLogRecordPointers(records, firstRemaining)
	return nil
}

// TrimUpdatesAuthoritative remove registros com offset menor ou igual ao
// limite informado, exigindo que o fence autoritativo ainda seja válido.
func (s *Store) TrimUpdatesAuthoritative(
	ctx context.Context,
	key storage.DocumentKey,
	through storage.UpdateOffset,
	fence storage.AuthorityFence,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil {
		return errNilStore
	}
	if err := key.Validate(); err != nil {
		return err
	}
	if err := fence.Validate(); err != nil {
		return err
	}

	now := s.nowTime()

	s.mu.Lock()
	defer s.mu.Unlock()

	s.ensureStoreInitializedLocked()
	if err := s.validateAuthorityLocked(key, fence, now); err != nil {
		return err
	}

	records := s.updateLogs[key]
	if len(records) == 0 {
		return nil
	}

	firstRemaining := firstUpdateLogIndexAfter(records, through)
	if firstRemaining == 0 {
		return nil
	}
	if firstRemaining >= len(records) {
		delete(s.updateLogs, key)
		return nil
	}

	s.updateLogs[key] = trimUpdateLogRecordPointers(records, firstRemaining)
	return nil
}

func firstUpdateLogIndexAfter(records []*storage.UpdateLogRecord, after storage.UpdateOffset) int {
	return sort.Search(len(records), func(i int) bool {
		return records[i] != nil && records[i].Offset > after
	})
}

func cloneUpdateLogRecordPointers(records []*storage.UpdateLogRecord) []*storage.UpdateLogRecord {
	cloned := make([]*storage.UpdateLogRecord, len(records))
	copy(cloned, records)
	return cloned
}

func trimUpdateLogRecordPointers(records []*storage.UpdateLogRecord, firstRemaining int) []*storage.UpdateLogRecord {
	if firstRemaining <= 0 {
		return records
	}
	if firstRemaining >= len(records) {
		return nil
	}
	if firstRemaining <= len(records)/4 {
		for idx := 0; idx < firstRemaining; idx++ {
			records[idx] = nil
		}
		return records[firstRemaining:]
	}
	return cloneUpdateLogRecordPointers(records[firstRemaining:])
}

// SavePlacement grava ou substitui o placement lógico do documento.
func (s *Store) SavePlacement(ctx context.Context, placement storage.PlacementRecord) (*storage.PlacementRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s == nil {
		return nil, errNilStore
	}

	record := placement.Clone()
	if record.UpdatedAt.IsZero() {
		record.UpdatedAt = s.nowTime()
	}
	if err := record.Validate(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.ensureStoreInitializedLocked()
	s.placements[record.Key] = record
	return record.Clone(), nil
}

// LoadPlacement carrega o placement atual associado ao documento.
func (s *Store) LoadPlacement(ctx context.Context, key storage.DocumentKey) (*storage.PlacementRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s == nil {
		return nil, errNilStore
	}
	if err := key.Validate(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	record, ok := s.placements[key]
	if !ok {
		return nil, storage.ErrPlacementNotFound
	}
	return record.Clone(), nil
}

// ListPlacements lista placements conhecidos de forma determinística.
func (s *Store) ListPlacements(ctx context.Context, opts storage.PlacementListOptions) ([]*storage.PlacementRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s == nil {
		return nil, errNilStore
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	records := make([]*storage.PlacementRecord, 0, len(s.placements))
	for _, record := range s.placements {
		if opts.Namespace != "" && record.Key.Namespace != opts.Namespace {
			continue
		}
		records = append(records, record.Clone())
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].Key.Namespace != records[j].Key.Namespace {
			return records[i].Key.Namespace < records[j].Key.Namespace
		}
		return records[i].Key.DocumentID < records[j].Key.DocumentID
	})
	if opts.Limit > 0 && len(records) > opts.Limit {
		records = records[:opts.Limit]
	}
	return records, nil
}

// SaveLease grava ou renova a lease atual de ownership para o shard.
func (s *Store) SaveLease(ctx context.Context, lease storage.LeaseRecord) (*storage.LeaseRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s == nil {
		return nil, errNilStore
	}

	record := lease.Clone()
	now := s.nowTime()
	if record.AcquiredAt.IsZero() {
		record.AcquiredAt = now
	}
	if err := record.Validate(); err != nil {
		return nil, err
	}
	opTime := record.AcquiredAt

	s.mu.Lock()
	defer s.mu.Unlock()

	s.ensureStoreInitializedLocked()
	current := s.leases[record.ShardID]
	lastEpoch := s.leaseLast[record.ShardID]

	switch {
	case current == nil:
		if record.Owner.Epoch <= lastEpoch {
			return nil, storage.NewLeaseStaleEpochError(record.ShardID, lastEpoch, record.Owner.Epoch)
		}
	case sameLeaseRecord(current, record):
		// Renewals keep the original generation start time.
		record.AcquiredAt = current.AcquiredAt
	case current.ExpiresAt.After(opTime):
		if record.Owner.Epoch <= current.Owner.Epoch {
			return nil, storage.NewLeaseStaleEpochError(record.ShardID, current.Owner.Epoch, record.Owner.Epoch)
		}
		return nil, fmt.Errorf(
			"%w: shard %s token %q",
			storage.ErrLeaseConflict,
			record.ShardID,
			current.Token,
		)
	default:
		if record.Owner.Epoch <= current.Owner.Epoch || record.Owner.Epoch <= lastEpoch {
			currentEpoch := current.Owner.Epoch
			if lastEpoch > currentEpoch {
				currentEpoch = lastEpoch
			}
			return nil, storage.NewLeaseStaleEpochError(record.ShardID, currentEpoch, record.Owner.Epoch)
		}
	}
	if !record.ExpiresAt.After(record.AcquiredAt) {
		return nil, fmt.Errorf("%w: expiresAt deve ser apos acquiredAt", storage.ErrInvalidLeaseExpiry)
	}

	if record.Owner.Epoch > lastEpoch {
		s.leaseLast[record.ShardID] = record.Owner.Epoch
	}
	s.leases[record.ShardID] = record
	return record.Clone(), nil
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
	if s == nil {
		return nil, errNilStore
	}
	if err := shardID.Validate(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(currentToken) == "" {
		return nil, fmt.Errorf("%w: currentToken obrigatorio", storage.ErrInvalidLeaseToken)
	}

	record := next.Clone()
	if record.ShardID != shardID {
		return nil, fmt.Errorf("%w: next shard %q != %q", storage.ErrInvalidShardID, record.ShardID, shardID)
	}
	now := s.nowTime()
	if record.AcquiredAt.IsZero() {
		record.AcquiredAt = now
	}
	if err := record.Validate(); err != nil {
		return nil, err
	}
	if !record.ExpiresAt.After(record.AcquiredAt) {
		return nil, fmt.Errorf("%w: expiresAt deve ser apos acquiredAt", storage.ErrInvalidLeaseExpiry)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.ensureStoreInitializedLocked()
	current := s.leases[shardID]
	if current == nil {
		return nil, storage.ErrLeaseNotFound
	}
	if current.Token != currentToken {
		return nil, fmt.Errorf("%w: shard %s token %q", storage.ErrLeaseConflict, shardID, current.Token)
	}
	if !current.ExpiresAt.After(record.AcquiredAt) {
		return nil, fmt.Errorf("%w: shard %s lease expirada", storage.ErrLeaseConflict, shardID)
	}
	expectedEpoch := current.Owner.Epoch + 1
	if record.Owner.Epoch != expectedEpoch {
		return nil, storage.NewLeaseStaleEpochError(shardID, current.Owner.Epoch, record.Owner.Epoch)
	}

	lastEpoch := s.leaseLast[shardID]
	if record.Owner.Epoch <= lastEpoch {
		return nil, storage.NewLeaseStaleEpochError(shardID, lastEpoch, record.Owner.Epoch)
	}
	s.leaseLast[shardID] = record.Owner.Epoch
	s.leases[shardID] = record
	return record.Clone(), nil
}

// LoadLease carrega a lease atual de ownership do shard.
func (s *Store) LoadLease(ctx context.Context, shardID storage.ShardID) (*storage.LeaseRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s == nil {
		return nil, errNilStore
	}
	if err := shardID.Validate(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	record, ok := s.leases[shardID]
	if !ok {
		return nil, storage.ErrLeaseNotFound
	}
	return record.Clone(), nil
}

// ReleaseLease remove a lease atual quando o token informado corresponde ao owner persistido.
func (s *Store) ReleaseLease(ctx context.Context, shardID storage.ShardID, token string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil {
		return errNilStore
	}
	if err := shardID.Validate(); err != nil {
		return err
	}
	if err := validateLeaseToken(token); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.ensureStoreInitializedLocked()
	record, ok := s.leases[shardID]
	if !ok || record.Token != token {
		return storage.ErrLeaseNotFound
	}

	if record.Owner.Epoch > s.leaseLast[shardID] {
		s.leaseLast[shardID] = record.Owner.Epoch
	}
	delete(s.leases, shardID)
	return nil
}

func sameLeaseRecord(current, next *storage.LeaseRecord) bool {
	if current == nil || next == nil {
		return false
	}
	return current.ShardID == next.ShardID &&
		current.Owner == next.Owner &&
		current.Token == next.Token
}

func validateLeaseToken(token string) error {
	if strings.TrimSpace(token) == "" {
		return fmt.Errorf("%w: token obrigatorio", storage.ErrInvalidLeaseToken)
	}
	return nil
}

func (s *Store) validateAuthorityLocked(key storage.DocumentKey, fence storage.AuthorityFence, now time.Time) error {
	placement, ok := s.placements[key]
	if !ok || placement == nil {
		return fmt.Errorf("%w: placement ausente para %s/%s", storage.ErrAuthorityLost, key.Namespace, key.DocumentID)
	}
	if placement.ShardID != fence.ShardID {
		return fmt.Errorf(
			"%w: placement shard %s != fence shard %s para %s/%s",
			storage.ErrAuthorityLost,
			placement.ShardID,
			fence.ShardID,
			key.Namespace,
			key.DocumentID,
		)
	}

	lease, ok := s.leases[fence.ShardID]
	if !ok || lease == nil {
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
