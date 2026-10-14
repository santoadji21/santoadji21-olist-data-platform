-- Porting 16 pemeriksaan mart T5 (tanpa 'stream_freshness_*': menunggu Kafka). Lulus bila tidak ada baris bad_rows <> 0.
select * from (
-- Setiap baris = satu pemeriksaan mart; bad_rows HARUS 0. Menelusuri tiap mart kembali ke fakta di dw.
SELECT 'sales_revenue_equals_fact_items' AS check_name,
       (abs((SELECT sum(revenue) FROM {{ ref('sales_monthly') }})
          - (SELECT sum(price) FROM {{ ref('fact_order_item') }} WHERE order_status NOT IN ('canceled','unavailable'))) > 0.005)::int AS bad_rows
UNION ALL SELECT 'sales_items_equals_fact_items',
       abs((SELECT sum(items) FROM {{ ref('sales_monthly') }}) - (SELECT count(*) FROM {{ ref('fact_order_item') }} WHERE order_status NOT IN ('canceled','unavailable')))
UNION ALL SELECT 'revenue_vs_paid_paid_equals_fact',
       (abs((SELECT sum(paid_value) FROM {{ ref('revenue_vs_paid_monthly') }})
          - (SELECT sum(payment_value) FROM {{ ref('fact_order') }} WHERE has_items AND has_payment AND order_status NOT IN ('canceled','unavailable'))) > 0.005)::int
UNION ALL SELECT 'delivery_denominator_equals_fact_not_null',
       abs((SELECT sum(delivered_orders) FROM {{ ref('delivery_monthly') }}) - (SELECT count(*) FROM {{ ref('fact_order') }} WHERE is_late IS NOT NULL))
UNION ALL SELECT 'delivery_late_equals_fact_late',
       abs((SELECT sum(late_orders) FROM {{ ref('delivery_monthly') }}) - (SELECT count(*) FROM {{ ref('fact_order') }} WHERE is_late))
UNION ALL SELECT 'delivery_no_rate_above_100', count(*) FROM {{ ref('delivery_monthly') }} WHERE late_rate_pct > 100 OR late_rate_pct < 0
UNION ALL SELECT 'seller_count_equals_sellers_with_items',
       abs((SELECT count(*) FROM {{ ref('seller_scorecard') }}) - (SELECT count(DISTINCT seller_id) FROM {{ ref('fact_order_item') }}))
UNION ALL SELECT 'seller_unrated_never_flagged', count(*) FROM {{ ref('seller_scorecard') }} WHERE NOT is_rated AND needs_action
UNION ALL SELECT 'rfm_customers_equals_people_with_valid_orders',
       abs((SELECT count(*) FROM {{ ref('customer_rfm') }}) - (SELECT count(DISTINCT customer_unique_id) FROM {{ ref('fact_order') }} WHERE has_items AND order_status NOT IN ('canceled','unavailable')))
UNION ALL SELECT 'rfm_frequency_sums_to_valid_orders',
       abs((SELECT sum(frequency) FROM {{ ref('customer_rfm') }}) - (SELECT count(*) FROM {{ ref('fact_order') }} WHERE has_items AND order_status NOT IN ('canceled','unavailable')))
UNION ALL SELECT 'rfm_scores_in_1_5', count(*) FROM {{ ref('customer_rfm') }} WHERE r_score NOT BETWEEN 1 AND 5 OR f_score NOT BETWEEN 1 AND 5 OR m_score NOT BETWEEN 1 AND 5
UNION ALL SELECT 'rfm_every_customer_has_a_segment', count(*) FROM {{ ref('customer_rfm') }} WHERE segment IS NULL
UNION ALL SELECT 'reconciliation_one_row_per_order', abs((SELECT count(*) FROM {{ ref('reconciliation') }}) - (SELECT count(*) FROM {{ ref('fact_order') }}))
UNION ALL SELECT 'reconciliation_every_order_has_a_class', count(*) FROM {{ ref('reconciliation') }} WHERE class IS NULL
UNION ALL SELECT 'reconciliation_matched_share_at_least_98pct',
       ((SELECT 100.0 * count(*) FILTER (WHERE class = 'cocok') / count(*) FROM {{ ref('reconciliation') }}) < 98)::int
UNION ALL SELECT 'reconciliation_paid_equals_fact', (abs((SELECT sum(paid_value) FROM {{ ref('reconciliation') }}) - (SELECT sum(payment_value) FROM {{ ref('fact_order') }})) > 0.005)::int
) t where bad_rows <> 0
