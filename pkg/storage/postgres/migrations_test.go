package postgres

import (
	"strings"
	"testing"
)

func TestLoadMigrations(t *testing.T) {
	t.Parallel()

	migrations, err := loadMigrations("tenant_app")
	if err != nil {
		t.Fatalf("loadMigrations() unexpected error: %v", err)
	}
	if len(migrations) == 0 {
		t.Fatal("loadMigrations() returned no migrations")
	}
	if migrations[0].version != 1 {
		t.Fatalf("migrations[0].version = %d, want 1", migrations[0].version)
	}
	if migrations[len(migrations)-1].version < 9 {
		t.Fatalf("last migration version = %d, want at least 9", migrations[len(migrations)-1].version)
	}
	if !strings.Contains(migrations[0].sql, `"tenant_app".document_snapshots`) {
		t.Fatalf("migration sql = %q, want quoted schema substitution", migrations[0].sql)
	}

	var foundGenerationTable bool
	var foundZeroSeed bool
	var foundSnapshotThrough bool
	var foundOwnerEpochColumns bool
	var foundSnapshotV2 bool
	var foundV2CanonicalStorage bool
	for _, migration := range migrations {
		if migration.version == 3 &&
			strings.Contains(migration.sql, `"tenant_app".shard_lease_generations`) &&
			strings.Contains(migration.sql, `FROM "tenant_app".shard_leases`) {
			foundGenerationTable = true
		}
		if migration.version == 4 && strings.Contains(migration.sql, "last_epoch >= 0") {
			foundZeroSeed = true
		}
		if migration.version == 5 &&
			strings.Contains(migration.sql, `"tenant_app".document_snapshots`) &&
			strings.Contains(migration.sql, "through_offset") {
			foundSnapshotThrough = true
		}
		if migration.version == 6 &&
			strings.Contains(migration.sql, `"tenant_app".document_snapshots`) &&
			strings.Contains(migration.sql, `"tenant_app".document_update_logs`) &&
			strings.Contains(migration.sql, "owner_epoch") {
			foundOwnerEpochColumns = true
		}
		if migration.version == 7 &&
			strings.Contains(migration.sql, `"tenant_app".document_snapshots`) &&
			strings.Contains(migration.sql, "snapshot_v2") {
			foundSnapshotV2 = true
		}
		if migration.version == 9 &&
			strings.Contains(migration.sql, "ALTER COLUMN snapshot_v1 DROP NOT NULL") &&
			strings.Contains(migration.sql, "ALTER COLUMN update_v1 DROP NOT NULL") &&
			strings.Contains(migration.sql, "SET snapshot_v1 = NULL") &&
			strings.Contains(migration.sql, "SET update_v1 = NULL") {
			foundV2CanonicalStorage = true
		}
	}
	if !foundGenerationTable {
		t.Fatal("migration version 3 does not define/backfill shard_lease_generations")
	}
	if !foundZeroSeed {
		t.Fatal("migration version 4 does not relax shard_lease_generations last_epoch for zero-seed locking")
	}
	if !foundSnapshotThrough {
		t.Fatal("migration version 5 does not add document snapshot through_offset checkpointing")
	}
	if !foundOwnerEpochColumns {
		t.Fatal("migration version 6 does not add owner_epoch metadata to snapshots and update logs")
	}
	if !foundSnapshotV2 {
		t.Fatal("migration version 7 does not add optional V2 snapshot payloads")
	}
	if !foundV2CanonicalStorage {
		t.Fatal("migration version 9 does not switch duplicated V1 payloads to nullable compatibility storage")
	}
}
