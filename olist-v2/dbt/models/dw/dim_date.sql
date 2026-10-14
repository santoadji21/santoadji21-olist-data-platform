-- Porting dari T0–T6 sql/dw/05_dim_date.sql (@ cf8b911). PK dan FK dipasang sebagai constraint Postgres lewat post_hook.
{{ config(
    post_hook=[
        "alter table {{ this }} add primary key (date_key)"
    ]
) }}

-- dim_date: satu baris per tanggal kalender dari tanggal terawal sampai terakhir di semua kolom tanggal fakta.
-- is_complete_month: false bila jumlah order bulan itu < 10% median bulanan (bulan sparse awal/akhir dataset;
-- TIDAK dibuang, hanya diberi flag agar mart dan dashboard bisa memilih). Bulan tanpa order sama sekali
-- (mis. 2016-11) juga false.
WITH bounds AS (
    SELECT min(d)::date AS d0, max(d)::date AS d1
    FROM {{ ref('stg_orders') }} o,
         LATERAL unnest(ARRAY[o.order_purchase_timestamp, o.order_approved_at, o.order_delivered_carrier_date,
                              o.order_delivered_customer_date, o.order_estimated_delivery_date]) AS d
), monthly AS (
    SELECT date_trunc('month', order_purchase_timestamp)::date AS m, count(*) AS n FROM {{ ref('stg_orders') }} GROUP BY 1
), med AS (
    SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY n) AS median_n FROM monthly
)
SELECT to_char(g, 'YYYYMMDD')::int          AS date_key,
       g::date                              AS date,
       extract(year FROM g)::int            AS year,
       extract(quarter FROM g)::int         AS quarter,
       extract(month FROM g)::int           AS month,
       to_char(g, 'YYYY-MM')                AS year_month,
       to_char(g, 'FMMonth')                AS month_name,
       extract(day FROM g)::int             AS day,
       extract(isodow FROM g)::int          AS iso_dow,
       extract(isodow FROM g) IN (6, 7)     AS is_weekend,
       coalesce(mo.n, 0) >= 0.10 * med.median_n AS is_complete_month
FROM bounds, med,
     generate_series(bounds.d0, bounds.d1, interval '1 day') AS g
LEFT JOIN monthly mo ON mo.m = date_trunc('month', g)::date
