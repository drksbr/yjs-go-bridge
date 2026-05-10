ALTER TABLE {{schema}}.document_snapshots
    ALTER COLUMN snapshot_v1 DROP NOT NULL;

ALTER TABLE {{schema}}.document_update_logs
    ALTER COLUMN update_v1 DROP NOT NULL;

UPDATE {{schema}}.document_snapshots
SET snapshot_v1 = NULL
WHERE snapshot_v2 IS NOT NULL;

UPDATE {{schema}}.document_update_logs
SET update_v1 = NULL
WHERE update_v2 IS NOT NULL;

ALTER TABLE {{schema}}.document_snapshots
    DROP CONSTRAINT IF EXISTS document_snapshots_payload_present;

ALTER TABLE {{schema}}.document_snapshots
    ADD CONSTRAINT document_snapshots_payload_present
    CHECK (snapshot_v1 IS NOT NULL OR snapshot_v2 IS NOT NULL);

ALTER TABLE {{schema}}.document_update_logs
    DROP CONSTRAINT IF EXISTS document_update_logs_payload_present;

ALTER TABLE {{schema}}.document_update_logs
    ADD CONSTRAINT document_update_logs_payload_present
    CHECK (update_v1 IS NOT NULL OR update_v2 IS NOT NULL);
