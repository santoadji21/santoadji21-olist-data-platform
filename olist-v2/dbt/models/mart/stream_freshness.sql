-- Porting dari T0–T6 sql/mart/60_stream_freshness.sql.
{{ config(post_hook=["alter table {{ this }} add primary key (minute)"]) }}

-- Freshness per menit. p50 per menit = rata-rata p50 batch berbobot jumlah event baru; p95 = maksimum p95 batch (batas atas).
SELECT date_trunc('minute', consumed_at)                    AS minute,
       count(*)                                             AS batches,
       sum(n_messages)                                      AS messages,
       sum(n_new)                                           AS new_events,
       sum(n_duplicate)                                     AS duplicates,
       sum(n_out_of_order)                                  AS out_of_order,
       sum(n_dlq)                                           AS dlq,
       round((sum(fresh_p50_ms * n_new) / nullif(sum(n_new) FILTER (WHERE fresh_p50_ms IS NOT NULL), 0))::numeric, 1) AS fresh_p50_ms,
       round(max(fresh_p95_ms)::numeric, 1)                 AS fresh_p95_ms_max
FROM {{ source('stream', 'batch_metrics') }}
GROUP BY 1
