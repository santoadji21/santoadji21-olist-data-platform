-- Porting dari T0–T6 sql/mart/10_sales_monthly.sql. PK dipasang lewat post_hook.
{{ config(post_hook=["alter table {{ this }} add primary key (year_month)"]) }}

-- Q1 (bagian "cocok dengan uang yang dibayar?"): nilai pesanan vs terbayar per bulan,
-- hanya order yang punya item DAN pembayaran (perbandingan sebanding).
SELECT d.year_month,
       d.is_complete_month,
       count(*)                                         AS orders_compared,
       sum(o.items_value + o.freight_value)::numeric(14,2) AS ordered_value,
       sum(o.payment_value)::numeric(14,2)              AS paid_value,
       (sum(o.payment_value) - sum(o.items_value + o.freight_value))::numeric(14,2) AS paid_minus_ordered
FROM {{ ref('fact_order') }} o
JOIN {{ ref('dim_date') }} d ON d.date_key = o.purchase_date_key
WHERE o.has_items AND o.has_payment AND o.order_status NOT IN ('canceled', 'unavailable')
GROUP BY d.year_month, d.is_complete_month
