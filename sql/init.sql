-- Runs once when the Postgres volume is first created.
CREATE DATABASE airflow;

CREATE TABLE IF NOT EXISTS daily_summary (
    txn_date      DATE        NOT NULL,
    txn_type      TEXT        NOT NULL,
    txn_count     BIGINT      NOT NULL,
    total_amount  NUMERIC(18,2) NOT NULL,
    fraud_count   BIGINT      NOT NULL,
    fraud_rate    DOUBLE PRECISION NOT NULL,
    loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (txn_date, txn_type)
);

CREATE TABLE IF NOT EXISTS dq_results (
    txn_date    DATE NOT NULL,
    check_name  TEXT NOT NULL,
    failed_rows BIGINT NOT NULL,
    passed      BOOLEAN NOT NULL,
    checked_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (txn_date, check_name)
);

CREATE TABLE IF NOT EXISTS alerts (
    txn_id      BIGINT      NOT NULL,
    event_ts    TIMESTAMP   NOT NULL,
    rule        TEXT        NOT NULL,
    txn_type    TEXT        NOT NULL,
    account_id  TEXT,
    dest_id     TEXT,
    amount      NUMERIC(18,2),
    is_fraud    INT,          -- ground-truth label, used only to score rule precision/recall
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS stream_metrics (
    window_start TIMESTAMP NOT NULL,
    window_end   TIMESTAMP NOT NULL,
    txn_type     TEXT      NOT NULL,
    txn_count    BIGINT    NOT NULL,
    total_amount NUMERIC(18,2),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
