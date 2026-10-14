-- Porting dari T0–T6 sql/checks/dw_integrity.sql (28 pemeriksaan). Test dbt LULUS bila query ini mengembalikan 0 baris,
-- yaitu bila tidak ada pemeriksaan dengan bad_rows <> 0.
select * from (
SELECT 'fk_dim_customer_geo' AS check_name, count(*) AS bad_rows FROM {{ ref('dim_customer') }} c LEFT JOIN {{ ref('dim_geo') }} g USING (geo_key) WHERE g.geo_key IS NULL
UNION ALL SELECT 'fk_dim_seller_geo', count(*) FROM {{ ref('dim_seller') }} s LEFT JOIN {{ ref('dim_geo') }} g USING (geo_key) WHERE g.geo_key IS NULL
UNION ALL SELECT 'fk_fact_order_customer', count(*) FROM {{ ref('fact_order') }} f LEFT JOIN {{ ref('dim_customer') }} c USING (customer_unique_id) WHERE c.customer_unique_id IS NULL
UNION ALL SELECT 'fk_fact_order_purchase_date', count(*) FROM {{ ref('fact_order') }} f LEFT JOIN {{ ref('dim_date') }} d ON d.date_key = f.purchase_date_key WHERE d.date_key IS NULL
UNION ALL SELECT 'fk_fact_order_delivered_date', count(*) FROM {{ ref('fact_order') }} f LEFT JOIN {{ ref('dim_date') }} d ON d.date_key = f.delivered_date_key WHERE f.delivered_date_key IS NOT NULL AND d.date_key IS NULL
UNION ALL SELECT 'fk_fact_order_estimated_date', count(*) FROM {{ ref('fact_order') }} f LEFT JOIN {{ ref('dim_date') }} d ON d.date_key = f.estimated_date_key WHERE d.date_key IS NULL
UNION ALL SELECT 'fk_item_order', count(*) FROM {{ ref('fact_order_item') }} i LEFT JOIN {{ ref('fact_order') }} o USING (order_id) WHERE o.order_id IS NULL
UNION ALL SELECT 'fk_item_product', count(*) FROM {{ ref('fact_order_item') }} i LEFT JOIN {{ ref('dim_product') }} p USING (product_id) WHERE p.product_id IS NULL
UNION ALL SELECT 'fk_item_seller', count(*) FROM {{ ref('fact_order_item') }} i LEFT JOIN {{ ref('dim_seller') }} s USING (seller_id) WHERE s.seller_id IS NULL
UNION ALL SELECT 'fk_payment_order', count(*) FROM {{ ref('fact_payment') }} p LEFT JOIN {{ ref('fact_order') }} o USING (order_id) WHERE o.order_id IS NULL
UNION ALL SELECT 'fk_review_order', count(*) FROM {{ ref('fact_review') }} r LEFT JOIN {{ ref('fact_order') }} o USING (order_id) WHERE o.order_id IS NULL
-- grain: jumlah baris = jumlah kunci unik di sumber
UNION ALL SELECT 'grain_dim_customer_vs_unique_people', abs((SELECT count(*) FROM {{ ref('dim_customer') }}) - (SELECT count(DISTINCT customer_unique_id) FROM {{ source('raw', 'customers') }}))
UNION ALL SELECT 'grain_fact_order_vs_raw_orders', abs((SELECT count(*) FROM {{ ref('fact_order') }}) - (SELECT count(*) FROM {{ source('raw', 'orders') }}))
UNION ALL SELECT 'grain_fact_order_item_vs_raw', abs((SELECT count(*) FROM {{ ref('fact_order_item') }}) - (SELECT count(*) FROM {{ source('raw', 'order_items') }}))
UNION ALL SELECT 'grain_fact_payment_vs_raw', abs((SELECT count(*) FROM {{ ref('fact_payment') }}) - (SELECT count(*) FROM {{ source('raw', 'order_payments') }}))
UNION ALL SELECT 'grain_fact_review_vs_orders_with_review', abs((SELECT count(*) FROM {{ ref('fact_review') }}) - (SELECT count(DISTINCT order_id) FROM {{ source('raw', 'order_reviews') }}))
UNION ALL SELECT 'grain_dim_geo_vs_distinct_zip', abs((SELECT count(*) - 1 FROM {{ ref('dim_geo') }}) - (SELECT count(DISTINCT geolocation_zip_code_prefix) FROM {{ source('raw', 'geolocation') }} WHERE NOT is_outside_brazil))
UNION ALL SELECT 'grain_dim_product_vs_raw', abs((SELECT count(*) FROM {{ ref('dim_product') }}) - (SELECT count(*) FROM {{ source('raw', 'products') }}))
-- isi: uang tidak bocor / tidak berlipat
UNION ALL SELECT 'sum_item_price_matches_raw', (abs((SELECT sum(price) FROM {{ ref('fact_order_item') }}) - (SELECT sum(price) FROM {{ source('raw', 'order_items') }})) > 0.005)::int
UNION ALL SELECT 'sum_payment_matches_raw', (abs((SELECT sum(payment_value) FROM {{ ref('fact_payment') }}) - (SELECT sum(payment_value) FROM {{ source('raw', 'order_payments') }})) > 0.005)::int
UNION ALL SELECT 'fact_order_item_count_matches_header', count(*) FROM {{ ref('fact_order') }} o WHERE o.item_count <> (SELECT count(*) FROM {{ ref('fact_order_item') }} i WHERE i.order_id = o.order_id)
-- aturan bisnis
UNION ALL SELECT 'is_late_not_null_for_non_delivered', count(*) FROM {{ ref('fact_order') }} WHERE is_late IS NOT NULL AND (order_status <> 'delivered' OR delivered_ts IS NULL)
UNION ALL SELECT 'is_late_null_for_delivered_with_date', count(*) FROM {{ ref('fact_order') }} WHERE order_status = 'delivered' AND delivered_ts IS NOT NULL AND is_late IS NULL
UNION ALL SELECT 'has_items_flag_consistent', count(*) FROM {{ ref('fact_order') }} WHERE has_items <> (item_count > 0)
UNION ALL SELECT 'review_count_ge_1', count(*) FROM {{ ref('fact_review') }} WHERE review_count < 1
UNION ALL SELECT 'dim_product_no_null_category_en', count(*) FROM {{ ref('dim_product') }} WHERE category_en IS NULL
UNION ALL SELECT 'dim_geo_no_point_outside_brazil', count(*) FROM {{ ref('dim_geo') }} WHERE geo_key <> 'unknown' AND (lat NOT BETWEEN -34 AND 5 OR lng NOT BETWEEN -74 AND -35)
UNION ALL SELECT 'dim_geo_single_unknown_row', abs((SELECT count(*) FROM {{ ref('dim_geo') }} WHERE geo_key = 'unknown') - 1)
) c
where bad_rows <> 0
