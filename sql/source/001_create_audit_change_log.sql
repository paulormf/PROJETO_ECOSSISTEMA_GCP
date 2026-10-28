-- Change log da origem PostgreSQL.
-- Esta migration deve ser revisada e executada no banco de origem,
-- nunca em um ambiente GCP, depois que a conexão e o plano de rollback
-- forem validados.

BEGIN;

CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS audit.change_log (
    change_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    operation CHAR(1) NOT NULL CHECK (operation IN ('I', 'U', 'D')),
    source_schema NAME NOT NULL,
    source_table NAME NOT NULL,
    primary_key JSONB NOT NULL,
    old_row JSONB,
    new_row JSONB,
    source_user NAME NOT NULL DEFAULT session_user,
    transaction_id BIGINT NOT NULL DEFAULT txid_current(),
    request_id TEXT,
    processed_at TIMESTAMPTZ,
    CONSTRAINT change_log_row_state_check CHECK (
        (operation = 'I' AND old_row IS NULL AND new_row IS NOT NULL)
        OR (operation = 'U' AND old_row IS NOT NULL AND new_row IS NOT NULL)
        OR (operation = 'D' AND old_row IS NOT NULL AND new_row IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS ix_change_log_source_position
    ON audit.change_log (source_schema, source_table, change_id);

CREATE INDEX IF NOT EXISTS ix_change_log_occurred_at
    ON audit.change_log (occurred_at);

CREATE INDEX IF NOT EXISTS ix_change_log_unprocessed
    ON audit.change_log (change_id)
    WHERE processed_at IS NULL;

COMMENT ON TABLE audit.change_log IS
    'Auditoria de alterações das tabelas de origem para ingestão incremental.';

COMMENT ON COLUMN audit.change_log.change_id IS
    'Watermark global crescente usado pelos consumidores do change log.';

COMMENT ON COLUMN audit.change_log.primary_key IS
    'Chave primária da linha de origem representada como objeto JSONB.';

COMMIT;
