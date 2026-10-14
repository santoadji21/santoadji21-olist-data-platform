-- Porting dari T0–T6 sql/dw/30_fact_order.sql (@ cf8b911). PK dan FK dipasang sebagai constraint Postgres lewat post_hook.
-- depends_on: {{ ref('dim_customer') }}
-- depends_on: {{ ref('dim_date') }}
{{ config(
    post_hook=[
        "alter table {{ this }} add primary key (order_id)",
        "alter table {{ this }} add foreign key (customer_unique_id) references {{ ref('dim_customer') }} (customer_unique_id)",
        "alter table {{ this }} add foreign key (purchase_date_key) references {{ ref('dim_date') }} (date_key)",
        "alter table {{ this }} add foreign key (delivered_date_key) references {{ ref('dim_date') }} (date_key)",
        "alter table {{ this }} add foreign key (estimated_date_key) references {{ ref('dim_date') }} (date_key)"
    ]
) }}

-- fact_order: grain = satu baris per ORDER (header), semua 99.441 order termasuk tanpa item / tanpa pembayaran.
-- is_late: NULL (bukan false) bila order tidak/belum terkirim atau tanggal tiba kosong; dibandingkan per TANGGAL.
WITH items AS (
    SELECT order_id, count(*) AS item_count, sum(price) AS items_value, sum(freight_value) AS freight_value
    FROM {{ ref('stg_order_items') }} GROUP BY order_id
), pays AS (
    SELECT order_id, count(*) AS payment_count, sum(payment_value) AS payment_value
    FROM {{ ref('stg_order_payments') }} GROUP BY order_id
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
FROM {{ ref('stg_orders') }} o
JOIN {{ ref('stg_customers') }} c USING (customer_id)
LEFT JOIN items i USING (order_id)
LEFT JOIN pays  p USING (order_id)
