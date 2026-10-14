-- Porting dari T0–T6 sql/mart/50_reconciliation.sql. PK dipasang lewat post_hook.
{{ config(post_hook=["alter table {{ this }} add primary key (order_id)", "create index on {{ this }} (class)"]) }}

-- Rekonsiliasi per order: nilai pesanan (Σ price + Σ freight) vs terbayar (Σ payment_value).
-- TOLERANSI: |selisih| <= 0,01 BRL dianggap cocok (pembulatan sen).
-- Kelas (urutan prioritas, tiap order tepat satu): tanpa_item → tanpa_pembayaran → cocok → voucher →
-- cicilan (dibayar LEBIH dan ada kartu kredit dengan cicilan > 1: bunga cicilan) → tidak_dijelaskan.
WITH pay AS (
    SELECT order_id,
           bool_or(payment_type = 'voucher')                                AS has_voucher,
           coalesce(max(payment_installments) FILTER (WHERE payment_type = 'credit_card'), 0) AS max_cc_installments
    FROM {{ ref('fact_payment') }} GROUP BY order_id
), base AS (
    SELECT o.order_id, o.order_status, o.has_items, o.has_payment,
           (o.items_value + o.freight_value)::numeric(14,2) AS ordered_value,
           o.payment_value::numeric(14,2)                  AS paid_value,
           (o.payment_value - (o.items_value + o.freight_value))::numeric(14,2) AS diff,
           coalesce(p.has_voucher, false)                  AS has_voucher,
           coalesce(p.max_cc_installments, 0)              AS max_cc_installments
    FROM {{ ref('fact_order') }} o LEFT JOIN pay p USING (order_id)
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
FROM base b
