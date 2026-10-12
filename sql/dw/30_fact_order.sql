-- fact_order: grain = satu baris per ORDER (header), semua 99.441 order termasuk tanpa item / tanpa pembayaran.
-- is_late: NULL (bukan false) bila order tidak/belum terkirim atau tanggal tiba kosong; dibandingkan per TANGGAL.
CREATE TABLE dw.fact_order AS
WITH items AS (
    SELECT order_id, count(*) AS item_count, sum(price) AS items_value, sum(freight_value) AS freight_value
    FROM raw.order_items GROUP BY order_id
), pays AS (
    SELECT order_id, count(*) AS payment_count, sum(payment_value) AS payment_value
    FROM raw.order_payments GROUP BY order_id
)
SELECT o.order_id,
       o.customer_id,
       c.customer_unique_id,
       o.order_status,
       o.order_purchase_timestamp      AS purchase_ts,
       o.order_approved_at             AS approved_ts,
       o.order_delivered_carrier_date  AS carrier_ts,
       o.order_delivered_customer_date AS delivered_ts,
       o.order_estimated_delivery_date AS estimated_ts,
       to_char(o.order_purchase_timestamp, 'YYYYMMDD')::int                 AS purchase_date_key,
       to_char(o.order_delivered_customer_date, 'YYYYMMDD')::int            AS delivered_date_key,
       to_char(o.order_estimated_delivery_date, 'YYYYMMDD')::int            AS estimated_date_key,
       c.customer_state,
       (i.order_id IS NOT NULL)                       AS has_items,
       (p.order_id IS NOT NULL)                       AS has_payment,
       coalesce(i.item_count, 0)                      AS item_count,
       coalesce(i.items_value, 0)::numeric(12,2)      AS items_value,
       coalesce(i.freight_value, 0)::numeric(12,2)    AS freight_value,
       coalesce(p.payment_count, 0)                   AS payment_count,
       coalesce(p.payment_value, 0)::numeric(12,2)    AS payment_value,
       CASE WHEN o.order_status = 'delivered' AND o.order_delivered_customer_date IS NOT NULL
            THEN o.order_delivered_customer_date::date > o.order_estimated_delivery_date::date
       END                                            AS is_late,
       CASE WHEN o.order_status = 'delivered' AND o.order_delivered_customer_date IS NOT NULL
            THEN o.order_delivered_customer_date::date - o.order_purchase_timestamp::date
       END                                            AS delivery_days,
       CASE WHEN o.order_status = 'delivered' AND o.order_delivered_customer_date IS NOT NULL
            THEN o.order_delivered_customer_date::date - o.order_estimated_delivery_date::date
       END                                            AS days_vs_estimate
FROM raw.orders o
JOIN raw.customers c USING (customer_id)
LEFT JOIN items i USING (order_id)
LEFT JOIN pays  p USING (order_id);

ALTER TABLE dw.fact_order ADD PRIMARY KEY (order_id);
