-- T3: tabel stream.* (idempoten: aman dijalankan berulang).
CREATE SCHEMA IF NOT EXISTS stream;

-- Log event: satu baris per event UNIK. PK event_id = "<order_id>:<status>" → duplikat otomatis diabaikan.
CREATE TABLE IF NOT EXISTS stream.order_events (
    event_id        text PRIMARY KEY,
    order_id        text        NOT NULL,
    status          text        NOT NULL,
    status_rank     smallint    NOT NULL,
    event_ts        timestamp   NOT NULL,           -- waktu kejadian (waktu lokal Brasil, dari data)
    produced_at     timestamptz NOT NULL,           -- stempel producer saat kirim
    consumed_at     timestamptz NOT NULL,           -- stempel consumer saat batch diterima
    ts_imputed      boolean     NOT NULL DEFAULT false,
    kafka_partition integer     NOT NULL,
    kafka_offset    bigint      NOT NULL
);
CREATE INDEX IF NOT EXISTS order_events_order_idx ON stream.order_events (order_id);

-- Status terkini: satu baris per order; status_rank hanya boleh naik (dijaga oleh WHERE di upsert).
CREATE TABLE IF NOT EXISTS stream.order_current (
    order_id      text PRIMARY KEY,
    status        text        NOT NULL,
    status_rank   smallint    NOT NULL,
    last_event_ts timestamp   NOT NULL,
    updated_at    timestamptz NOT NULL DEFAULT now()
);

-- Pesan rusak. UNIQUE (topic, partition, offset) membuat penulisan ulang setelah crash idempoten.
CREATE TABLE IF NOT EXISTS stream.dlq (
    id              bigserial PRIMARY KEY,
    topic           text        NOT NULL,
    kafka_partition integer     NOT NULL,
    kafka_offset    bigint      NOT NULL,
    reason          text        NOT NULL,
    msg_key         text,
    payload         text,
    consumed_at     timestamptz NOT NULL,
    UNIQUE (topic, kafka_partition, kafka_offset)
);

-- Metrik per batch consumer (sumber laporan freshness dan mart T5).
CREATE TABLE IF NOT EXISTS stream.batch_metrics (
    run_id         text        NOT NULL,
    batch_no       integer     NOT NULL,
    consumed_at    timestamptz NOT NULL,
    n_messages     integer     NOT NULL,
    n_new          integer     NOT NULL,
    n_duplicate    integer     NOT NULL,
    n_out_of_order integer     NOT NULL,
    n_dlq          integer     NOT NULL,
    fresh_p50_ms   double precision,
    fresh_p95_ms   double precision,
    PRIMARY KEY (run_id, batch_no)
);
