-- Q1 — penjualan bulanan: bulan beli × kategori (Inggris) × negara bagian pelanggan.
-- DEFINISI PENDAPATAN: Σ price item pada order dengan status BUKAN 'canceled'/'unavailable'
-- (order yang tidak pernah terkirim bukan pendapatan). Ongkir (freight) dipisah, bukan bagian dari pendapatan.
-- Bukan payment_value: itu termasuk bunga cicilan/voucher dan order tanpa item (lihat mart.reconciliation).
CREATE TABLE mart.sales_monthly AS
SELECT d.year_month,
       d.is_complete_month,
       p.category_en                     AS category,
       i.customer_state,
       count(DISTINCT i.order_id)        AS orders,
       count(*)                          AS items,
       sum(i.price)::numeric(14,2)       AS revenue,
       sum(i.freight_value)::numeric(14,2) AS freight,
       round(avg(i.price), 2)            AS avg_item_price
FROM dw.fact_order_item i
JOIN dw.dim_date    d ON d.date_key = i.purchase_date_key
JOIN dw.dim_product p USING (product_id)
WHERE i.order_status NOT IN ('canceled', 'unavailable')
GROUP BY d.year_month, d.is_complete_month, p.category_en, i.customer_state;

ALTER TABLE mart.sales_monthly ADD PRIMARY KEY (year_month, category, customer_state);

-- Q1 (bagian "cocok dengan uang yang dibayar?"): nilai pesanan vs terbayar per bulan,
-- hanya order yang punya item DAN pembayaran (perbandingan sebanding).
CREATE TABLE mart.revenue_vs_paid_monthly AS
SELECT d.year_month,
       d.is_complete_month,
       count(*)                                         AS orders_compared,
       sum(o.items_value + o.freight_value)::numeric(14,2) AS ordered_value,
       sum(o.payment_value)::numeric(14,2)              AS paid_value,
       (sum(o.payment_value) - sum(o.items_value + o.freight_value))::numeric(14,2) AS paid_minus_ordered
FROM dw.fact_order o
JOIN dw.dim_date d ON d.date_key = o.purchase_date_key
WHERE o.has_items AND o.has_payment AND o.order_status NOT IN ('canceled', 'unavailable')
GROUP BY d.year_month, d.is_complete_month;

ALTER TABLE mart.revenue_vs_paid_monthly ADD PRIMARY KEY (year_month);
