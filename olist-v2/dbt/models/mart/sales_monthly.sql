-- Porting dari T0–T6 sql/mart/10_sales_monthly.sql. PK dipasang lewat post_hook.
{{ config(post_hook=["alter table {{ this }} add primary key (year_month, category, customer_state)"]) }}

-- Q1 — penjualan bulanan: bulan beli × kategori (Inggris) × negara bagian pelanggan.
-- DEFINISI PENDAPATAN: Σ price item pada order dengan status BUKAN 'canceled'/'unavailable'
-- (order yang tidak pernah terkirim bukan pendapatan). Ongkir (freight) dipisah, bukan bagian dari pendapatan.
-- Bukan payment_value: itu termasuk bunga cicilan/voucher dan order tanpa item (lihat mart.reconciliation).
SELECT d.year_month,
       d.is_complete_month,
       p.category_en                     AS category,
       i.customer_state,
       count(DISTINCT i.order_id)        AS orders,
       count(*)                          AS items,
       sum(i.price)::numeric(14,2)       AS revenue,
       sum(i.freight_value)::numeric(14,2) AS freight,
       round(avg(i.price), 2)            AS avg_item_price
FROM {{ ref('fact_order_item') }} i
JOIN {{ ref('dim_date') }}    d ON d.date_key = i.purchase_date_key
JOIN {{ ref('dim_product') }} p USING (product_id)
WHERE i.order_status NOT IN ('canceled', 'unavailable')
GROUP BY d.year_month, d.is_complete_month, p.category_en, i.customer_state
