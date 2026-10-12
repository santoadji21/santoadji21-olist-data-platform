-- Freshness stream (T3): agregasi dari stream.batch_metrics. Per batch (untuk grafik) dan per menit (untuk tabel).
-- p50 per menit = rata-rata p50 batch berbobot jumlah event baru; p95 per menit = maksimum p95 batch (batas atas).
CREATE TABLE mart.stream_freshness_batch AS
SELECT run_id, batch_no, consumed_at, n_messages, n_new, n_duplicate, n_out_of_order, n_dlq,
       round(fresh_p50_ms::numeric, 1) AS fresh_p50_ms, round(fresh_p95_ms::numeric, 1) AS fresh_p95_ms
FROM stream.batch_metrics;

ALTER TABLE mart.stream_freshness_batch ADD PRIMARY KEY (run_id, batch_no);

CREATE TABLE mart.stream_freshness AS
SELECT date_trunc('minute', consumed_at)                    AS minute,
       count(*)                                             AS batches,
       sum(n_messages)                                      AS messages,
       sum(n_new)                                           AS new_events,
       sum(n_duplicate)                                     AS duplicates,
       sum(n_out_of_order)                                  AS out_of_order,
       sum(n_dlq)                                           AS dlq,
       round((sum(fresh_p50_ms * n_new) / nullif(sum(n_new) FILTER (WHERE fresh_p50_ms IS NOT NULL), 0))::numeric, 1) AS fresh_p50_ms,
       round(max(fresh_p95_ms)::numeric, 1)                 AS fresh_p95_ms_max
FROM stream.batch_metrics
GROUP BY 1;

ALTER TABLE mart.stream_freshness ADD PRIMARY KEY (minute);
