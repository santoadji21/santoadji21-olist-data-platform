-- Setiap baris = satu pemeriksaan; bad_rows HARUS 0. Dipakai oleh `pipeline.py model` (sebelum commit) dan tests/test_dw.py.
SELECT 'fk_dim_customer_geo' AS check_name, count(*) AS bad_rows FROM dw.dim_customer c LEFT JOIN dw.dim_geo g USING (geo_key) WHERE g.geo_key IS NULL
UNION ALL SELECT 'fk_dim_seller_geo', count(*) FROM dw.dim_seller s LEFT JOIN dw.dim_geo g USING (geo_key) WHERE g.geo_key IS NULL
UNION ALL SELECT 'fk_fact_order_customer', count(*) FROM dw.fact_order f LEFT JOIN dw.dim_customer c USING (customer_unique_id) WHERE c.customer_unique_id IS NULL
UNION ALL SELECT 'fk_fact_order_purchase_date', count(*) FROM dw.fact_order f LEFT JOIN dw.dim_date d ON d.date_key = f.purchase_date_key WHERE d.date_key IS NULL
UNION ALL SELECT 'fk_fact_order_delivered_date', count(*) FROM dw.fact_order f LEFT JOIN dw.dim_date d ON d.date_key = f.delivered_date_key WHERE f.delivered_date_key IS NOT NULL AND d.date_key IS NULL
UNION ALL SELECT 'fk_fact_order_estimated_date', count(*) FROM dw.fact_order f LEFT JOIN dw.dim_date d ON d.date_key = f.estimated_date_key WHERE d.date_key IS NULL
UNION ALL SELECT 'fk_item_order', count(*) FROM dw.fact_order_item i LEFT JOIN dw.fact_order o USING (order_id) WHERE o.order_id IS NULL
UNION ALL SELECT 'fk_item_product', count(*) FROM dw.fact_order_item i LEFT JOIN dw.dim_product p USING (product_id) WHERE p.product_id IS NULL
UNION ALL SELECT 'fk_item_seller', count(*) FROM dw.fact_order_item i LEFT JOIN dw.dim_seller s USING (seller_id) WHERE s.seller_id IS NULL
UNION ALL SELECT 'fk_payment_order', count(*) FROM dw.fact_payment p LEFT JOIN dw.fact_order o USING (order_id) WHERE o.order_id IS NULL
UNION ALL SELECT 'fk_review_order', count(*) FROM dw.fact_review r LEFT JOIN dw.fact_order o USING (order_id) WHERE o.order_id IS NULL
-- grain: jumlah baris = jumlah kunci unik di sumber
UNION ALL SELECT 'grain_dim_customer_vs_unique_people', abs((SELECT count(*) FROM dw.dim_customer) - (SELECT count(DISTINCT customer_unique_id) FROM raw.customers))
UNION ALL SELECT 'grain_fact_order_vs_raw_orders', abs((SELECT count(*) FROM dw.fact_order) - (SELECT count(*) FROM raw.orders))
UNION ALL SELECT 'grain_fact_order_item_vs_raw', abs((SELECT count(*) FROM dw.fact_order_item) - (SELECT count(*) FROM raw.order_items))
UNION ALL SELECT 'grain_fact_payment_vs_raw', abs((SELECT count(*) FROM dw.fact_payment) - (SELECT count(*) FROM raw.order_payments))
UNION ALL SELECT 'grain_fact_review_vs_orders_with_review', abs((SELECT count(*) FROM dw.fact_review) - (SELECT count(DISTINCT order_id) FROM raw.order_reviews))
UNION ALL SELECT 'grain_dim_geo_vs_distinct_zip', abs((SELECT count(*) - 1 FROM dw.dim_geo) - (SELECT count(DISTINCT geolocation_zip_code_prefix) FROM raw.geolocation WHERE NOT is_outside_brazil))
UNION ALL SELECT 'grain_dim_product_vs_raw', abs((SELECT count(*) FROM dw.dim_product) - (SELECT count(*) FROM raw.products))
-- isi: uang tidak bocor / tidak berlipat
UNION ALL SELECT 'sum_item_price_matches_raw', (abs((SELECT sum(price) FROM dw.fact_order_item) - (SELECT sum(price) FROM raw.order_items)) > 0.005)::int
UNION ALL SELECT 'sum_payment_matches_raw', (abs((SELECT sum(payment_value) FROM dw.fact_payment) - (SELECT sum(payment_value) FROM raw.order_payments)) > 0.005)::int
UNION ALL SELECT 'fact_order_item_count_matches_header', count(*) FROM dw.fact_order o WHERE o.item_count <> (SELECT count(*) FROM dw.fact_order_item i WHERE i.order_id = o.order_id)
-- aturan bisnis
UNION ALL SELECT 'is_late_not_null_for_non_delivered', count(*) FROM dw.fact_order WHERE is_late IS NOT NULL AND (order_status <> 'delivered' OR delivered_ts IS NULL)
UNION ALL SELECT 'is_late_null_for_delivered_with_date', count(*) FROM dw.fact_order WHERE order_status = 'delivered' AND delivered_ts IS NOT NULL AND is_late IS NULL
UNION ALL SELECT 'has_items_flag_consistent', count(*) FROM dw.fact_order WHERE has_items <> (item_count > 0)
UNION ALL SELECT 'review_count_ge_1', count(*) FROM dw.fact_review WHERE review_count < 1
UNION ALL SELECT 'dim_product_no_null_category_en', count(*) FROM dw.dim_product WHERE category_en IS NULL
UNION ALL SELECT 'dim_geo_no_point_outside_brazil', count(*) FROM dw.dim_geo WHERE geo_key <> 'unknown' AND (lat NOT BETWEEN -34 AND 5 OR lng NOT BETWEEN -74 AND -35)
UNION ALL SELECT 'dim_geo_single_unknown_row', abs((SELECT count(*) FROM dw.dim_geo WHERE geo_key = 'unknown') - 1)
