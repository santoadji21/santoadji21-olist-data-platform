-- Porting dari T0–T6 sql/mart/60_stream_freshness.sql. Sumber: tabel stream.batch_metrics (ditulis consumer Kafka).
{{ config(post_hook=["alter table {{ this }} add primary key (run_id, batch_no)"]) }}

-- Freshness per batch consumer (untuk grafik).
SELECT run_id, batch_no, consumed_at, n_messages, n_new, n_duplicate, n_out_of_order, n_dlq,
       round(fresh_p50_ms::numeric, 1) AS fresh_p50_ms, round(fresh_p95_ms::numeric, 1) AS fresh_p95_ms
FROM {{ source('stream', 'batch_metrics') }}
