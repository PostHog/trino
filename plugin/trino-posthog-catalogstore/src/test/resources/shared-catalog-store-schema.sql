-- Canonical schema of the shared catalog store.
--
-- The coordinator never runs this: a managed reader holds a read-only role and creates nothing.
-- The controller that publishes the catalogs owns these tables, and this file is the single
-- definition both sides test against, so that the reader's expectations and the publisher's DDL
-- cannot drift apart in wording, types or constraints.
--
-- Statements are separated by a line containing only a semicolon.

CREATE TABLE IF NOT EXISTS trino_catalogs (
    cell_id         varchar     NOT NULL,
    catalog_name    varchar     NOT NULL,
    connector_name  varchar     NOT NULL,
    catalog_version varchar     NOT NULL,
    properties      text        NOT NULL,
    updated_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (cell_id, catalog_name)
)
;
CREATE TABLE IF NOT EXISTS trino_catalog_writer_state (
    cell_id         varchar     NOT NULL,
    revision        bigint      NOT NULL,
    writer_epoch    bigint      NOT NULL,
    writer_identity varchar     NOT NULL,
    catalog_count   integer     NOT NULL,
    updated_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (cell_id)
)
;
CREATE TABLE IF NOT EXISTS trino_catalog_journal (
    cell_id         varchar     NOT NULL,
    revision        bigint      NOT NULL,
    operation_id    varchar     NOT NULL,
    operation       varchar     NOT NULL,
    catalog_name    varchar     NOT NULL,
    catalog_version varchar,
    payload_hash    varchar     NOT NULL,
    writer_epoch    bigint      NOT NULL,
    committed_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (cell_id, revision)
)
;
CREATE UNIQUE INDEX IF NOT EXISTS trino_catalog_journal_operation
    ON trino_catalog_journal (cell_id, operation_id)
;
