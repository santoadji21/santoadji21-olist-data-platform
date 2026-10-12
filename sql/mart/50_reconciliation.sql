-- Rekonsiliasi per order: nilai pesanan (Σ price + Σ freight) vs terbayar (Σ payment_value).
-- TOLERANSI: |selisih| <= 0,01 BRL dianggap cocok (pembulatan sen).
-- Kelas (urutan prioritas, tiap order tepat satu): tanpa_item → tanpa_pembayaran → cocok → voucher →
-- cicilan (dibayar LEBIH dan ada kartu kredit dengan cicilan > 1: bunga cicilan) → tidak_dijelaskan.
CREATE TABLE mart.reconciliation AS
WITH pay AS (
    SELECT order_id,
           bool_or(payment_type = 'voucher')                                AS has_voucher,
           coalesce(max(payment_installments) FILTER (WHERE payment_type = 'credit_card'), 0) AS max_cc_installments
    FROM dw.fact_payment GROUP BY order_id
), base AS (
    SELECT o.order_id, o.order_status, o.has_items, o.has_payment,
           (o.items_value + o.freight_value)::numeric(14,2) AS ordered_value,
           o.payment_value::numeric(14,2)                  AS paid_value,
           (o.payment_value - (o.items_value + o.freight_value))::numeric(14,2) AS diff,
           coalesce(p.has_voucher, false)                  AS has_voucher,
           coalesce(p.max_cc_installments, 0)              AS max_cc_installments
    FROM dw.fact_order o LEFT JOIN pay p USING (order_id)
)
SELECT b.*,
       CASE
         WHEN NOT has_items                          THEN 'tanpa_item'
         WHEN NOT has_payment                        THEN 'tanpa_pembayaran'
         WHEN abs(diff) <= 0.01                      THEN 'cocok'
         WHEN has_voucher                            THEN 'voucher'
         WHEN diff > 0 AND max_cc_installments > 1   THEN 'cicilan'
         ELSE 'tidak_dijelaskan'
       END AS class
FROM base b;

ALTER TABLE mart.reconciliation ADD PRIMARY KEY (order_id);
CREATE INDEX ON mart.reconciliation (class);
