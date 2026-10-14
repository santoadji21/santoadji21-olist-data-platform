-- Porting dari T0–T6 sql/dw/20_dim_customer.sql (@ cf8b911). PK dan FK dipasang sebagai constraint Postgres lewat post_hook.
-- depends_on: {{ ref('dim_geo') }}
{{ config(
    post_hook=[
        "alter table {{ this }} add primary key (customer_unique_id)",
        "alter table {{ this }} add foreign key (geo_key) references {{ ref('dim_geo') }} (geo_key)"
    ]
) }}

-- dim_customer: satu baris per ORANG (customer_unique_id), bukan per order (customer_id).
-- Atribut yang berubah antar order (zip, kota): ambil yang TERBARU (SCD tipe 1).
-- Yang hilang: riwayat pindah alamat; pelanggan dinilai di lokasi pembelian terakhirnya.
WITH ranked AS (
    SELECT c.customer_unique_id, c.customer_zip_code_prefix, c.customer_city, c.customer_state,
           o.order_purchase_timestamp,
           row_number() OVER (PARTITION BY c.customer_unique_id
                              ORDER BY o.order_purchase_timestamp DESC, c.customer_id DESC) AS rn,
           count(*)  OVER (PARTITION BY c.customer_unique_id)                                AS n_orders,
           min(o.order_purchase_timestamp) OVER (PARTITION BY c.customer_unique_id)          AS first_order_ts,
           max(o.order_purchase_timestamp) OVER (PARTITION BY c.customer_unique_id)          AS last_order_ts
    FROM {{ ref('stg_customers') }} c
    JOIN {{ ref('stg_orders') }} o USING (customer_id)
)
SELECT r.customer_unique_id,
       CASE WHEN g.geo_key IS NULL THEN 'unknown' ELSE r.customer_zip_code_prefix::text END AS geo_key,
       r.customer_zip_code_prefix  AS zip_code_prefix,
       r.customer_city             AS city,
       r.customer_state            AS state,
       r.n_orders, r.first_order_ts, r.last_order_ts
FROM ranked r
LEFT JOIN {{ ref('dim_geo') }} g ON g.geo_key = r.customer_zip_code_prefix::text
WHERE r.rn = 1
