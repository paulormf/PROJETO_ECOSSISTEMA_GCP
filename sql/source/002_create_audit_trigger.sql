-- Função e triggers do change log da origem PostgreSQL.
-- Executar somente depois de validar a migration 001 no ambiente de desenvolvimento.

BEGIN;

CREATE OR REPLACE FUNCTION audit.capture_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    row_data JSONB;
    old_data JSONB;
    new_data JSONB;
    primary_key_data JSONB;
BEGIN
    old_data := CASE
        WHEN TG_OP IN ('UPDATE', 'DELETE') THEN to_jsonb(OLD)
        ELSE NULL
    END;

    new_data := CASE
        WHEN TG_OP IN ('INSERT', 'UPDATE') THEN to_jsonb(NEW)
        ELSE NULL
    END;

    row_data := COALESCE(new_data, old_data);

    SELECT COALESCE(
        jsonb_object_agg(attribute.attname, row_data -> attribute.attname),
        '{}'::JSONB
    )
    INTO primary_key_data
    FROM pg_index AS index_definition
    CROSS JOIN LATERAL unnest(index_definition.indkey)
        WITH ORDINALITY AS key_columns(attnum, key_ordinal)
    JOIN pg_attribute AS attribute
        ON attribute.attrelid = index_definition.indrelid
       AND attribute.attnum = key_columns.attnum
    WHERE index_definition.indrelid = TG_RELID
      AND index_definition.indisprimary;

    INSERT INTO audit.change_log (
        operation,
        source_schema,
        source_table,
        primary_key,
        old_row,
        new_row,
        request_id
    )
    VALUES (
        CASE TG_OP
            WHEN 'INSERT' THEN 'I'
            WHEN 'UPDATE' THEN 'U'
            WHEN 'DELETE' THEN 'D'
        END,
        TG_TABLE_SCHEMA,
        TG_TABLE_NAME,
        primary_key_data,
        old_data,
        new_data,
        current_setting('app.request_id', true)
    );

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;

    RETURN NEW;
END;
$$;

COMMENT ON FUNCTION audit.capture_change() IS
    'Registra INSERT, UPDATE e DELETE das tabelas monitoradas no audit.change_log.';

DO $$
DECLARE
    monitored_table TEXT;
BEGIN
    FOREACH monitored_table IN ARRAY ARRAY[
        'armazens',
        'categorias',
        'clientes',
        'enderecos_clientes',
        'envios',
        'estoque',
        'eventos_clientes',
        'itens_pedido',
        'marcas',
        'pagamentos',
        'pedidos',
        'produtos',
        'variantes_produtos'
    ]
    LOOP
        EXECUTE format(
            'DROP TRIGGER IF EXISTS %I ON %I.%I',
            'trg_audit_' || monitored_table,
            'ecommerce',
            monitored_table
        );

        EXECUTE format(
            'CREATE TRIGGER %I
             AFTER INSERT OR UPDATE OR DELETE ON %I.%I
             FOR EACH ROW
             EXECUTE FUNCTION audit.capture_change()',
            'trg_audit_' || monitored_table,
            'ecommerce',
            monitored_table
        );
    END LOOP;
END;
$$;

COMMIT;
