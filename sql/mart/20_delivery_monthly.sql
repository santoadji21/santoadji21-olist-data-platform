-- Q2 — performa pengiriman: bulan beli × negara bagian pelanggan.
-- PENYEBUT JUJUR: late_rate = terlambat / order dengan is_late IS NOT NULL (delivered + tanggal tiba terisi).
-- Order yang belum/tidak terkirim tidak ikut dihitung "tepat waktu".
CREATE TABLE mart.delivery_monthly AS
SELECT d.year_month,
       d.is_complete_month,
       o.customer_state,
       count(*)                                          AS delivered_orders,
       count(*) FILTER (WHERE o.is_late)                 AS late_orders,
       round(100.0 * count(*) FILTER (WHERE o.is_late) / count(*), 2) AS late_rate_pct,
       round(avg(o.delivery_days), 2)                    AS avg_delivery_days,
       round(avg(o.days_vs_estimate), 2)                 AS avg_days_vs_estimate,
       count(r.order_id)                                 AS reviewed_orders,
       round(avg(r.review_score), 3)                     AS avg_review_score,
       count(r.order_id) FILTER (WHERE NOT o.is_late)    AS reviewed_on_time,
       count(r.order_id) FILTER (WHERE o.is_late)        AS reviewed_late,
       round(avg(r.review_score) FILTER (WHERE NOT o.is_late), 3) AS avg_review_on_time,
       round(avg(r.review_score) FILTER (WHERE o.is_late), 3)     AS avg_review_late
FROM dw.fact_order o
JOIN dw.dim_date d ON d.date_key = o.purchase_date_key
LEFT JOIN dw.fact_review r USING (order_id)
WHERE o.is_late IS NOT NULL
GROUP BY d.year_month, d.is_complete_month, o.customer_state;

ALTER TABLE mart.delivery_monthly ADD PRIMARY KEY (year_month, customer_state);
