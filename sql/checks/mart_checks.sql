-- Setiap baris = satu pemeriksaan mart; bad_rows HARUS 0. Menelusuri tiap mart kembali ke fakta di dw.
SELECT 'sales_revenue_equals_fact_items' AS check_name,
       (abs((SELECT sum(revenue) FROM mart.sales_monthly)
          - (SELECT sum(price) FROM dw.fact_order_item WHERE order_status NOT IN ('canceled','unavailable'))) > 0.005)::int AS bad_rows
UNION ALL SELECT 'sales_items_equals_fact_items',
       abs((SELECT sum(items) FROM mart.sales_monthly) - (SELECT count(*) FROM dw.fact_order_item WHERE order_status NOT IN ('canceled','unavailable')))
UNION ALL SELECT 'revenue_vs_paid_paid_equals_fact',
       (abs((SELECT sum(paid_value) FROM mart.revenue_vs_paid_monthly)
          - (SELECT sum(payment_value) FROM dw.fact_order WHERE has_items AND has_payment AND order_status NOT IN ('canceled','unavailable'))) > 0.005)::int
UNION ALL SELECT 'delivery_denominator_equals_fact_not_null',
       abs((SELECT sum(delivered_orders) FROM mart.delivery_monthly) - (SELECT count(*) FROM dw.fact_order WHERE is_late IS NOT NULL))
UNION ALL SELECT 'delivery_late_equals_fact_late',
       abs((SELECT sum(late_orders) FROM mart.delivery_monthly) - (SELECT count(*) FROM dw.fact_order WHERE is_late))
UNION ALL SELECT 'delivery_no_rate_above_100', count(*) FROM mart.delivery_monthly WHERE late_rate_pct > 100 OR late_rate_pct < 0
UNION ALL SELECT 'seller_count_equals_sellers_with_items',
       abs((SELECT count(*) FROM mart.seller_scorecard) - (SELECT count(DISTINCT seller_id) FROM dw.fact_order_item))
UNION ALL SELECT 'seller_unrated_never_flagged', count(*) FROM mart.seller_scorecard WHERE NOT is_rated AND needs_action
UNION ALL SELECT 'rfm_customers_equals_people_with_valid_orders',
       abs((SELECT count(*) FROM mart.customer_rfm) - (SELECT count(DISTINCT customer_unique_id) FROM dw.fact_order WHERE has_items AND order_status NOT IN ('canceled','unavailable')))
UNION ALL SELECT 'rfm_frequency_sums_to_valid_orders',
       abs((SELECT sum(frequency) FROM mart.customer_rfm) - (SELECT count(*) FROM dw.fact_order WHERE has_items AND order_status NOT IN ('canceled','unavailable')))
UNION ALL SELECT 'rfm_scores_in_1_5', count(*) FROM mart.customer_rfm WHERE r_score NOT BETWEEN 1 AND 5 OR f_score NOT BETWEEN 1 AND 5 OR m_score NOT BETWEEN 1 AND 5
UNION ALL SELECT 'rfm_every_customer_has_a_segment', count(*) FROM mart.customer_rfm WHERE segment IS NULL
UNION ALL SELECT 'reconciliation_one_row_per_order', abs((SELECT count(*) FROM mart.reconciliation) - (SELECT count(*) FROM dw.fact_order))
UNION ALL SELECT 'reconciliation_every_order_has_a_class', count(*) FROM mart.reconciliation WHERE class IS NULL
UNION ALL SELECT 'reconciliation_matched_share_at_least_98pct',
       ((SELECT 100.0 * count(*) FILTER (WHERE class = 'cocok') / count(*) FROM mart.reconciliation) < 98)::int
UNION ALL SELECT 'reconciliation_paid_equals_fact', (abs((SELECT sum(paid_value) FROM mart.reconciliation) - (SELECT sum(payment_value) FROM dw.fact_order)) > 0.005)::int
UNION ALL SELECT 'stream_freshness_batches_equal_source', abs((SELECT sum(batches) FROM mart.stream_freshness) - (SELECT count(*) FROM stream.batch_metrics))
