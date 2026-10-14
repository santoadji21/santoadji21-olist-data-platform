-- Porting dari T0–T6 sql/mart/30_seller_scorecard.sql. PK dipasang lewat post_hook.
{{ config(post_hook=["alter table {{ this }} add primary key (seller_id)"]) }}

-- Q3 — scorecard seller. Satu baris per seller yang punya item.
-- Atribusi: order dihitung untuk SETIAP seller yang ada di order itu (order multi-seller terhitung ke tiap seller).
-- JUMLAH ORDER MINIMAL: seller dengan < 30 order tidak "dinilai" (is_rated = false): dengan 2 order dan 1 terlambat,
-- "50 % terlambat" tidak bermakna. 30 order → 634 dari 3.095 seller dinilai, tetapi mencakup sebagian besar order.
-- AMBANG TINDAKAN (hanya untuk seller yang dinilai): late_rate >= 15 % (≈ 2× rata-rata platform),
-- canceled_rate >= 5 %, atau rata-rata skor <= 3,5 (rata-rata platform ≈ 4,1).
WITH so AS (   -- satu baris per (seller, order)
    SELECT i.seller_id, i.order_id, sum(i.price) AS price
    FROM {{ ref('fact_order_item') }} i GROUP BY i.seller_id, i.order_id
), agg AS (
    SELECT so.seller_id,
           count(*)                                              AS orders,
           sum(so.price) FILTER (WHERE o.order_status NOT IN ('canceled','unavailable')) AS revenue,
           count(*) FILTER (WHERE o.is_late IS NOT NULL)         AS delivered_orders,
           count(*) FILTER (WHERE o.is_late)                     AS late_orders,
           count(*) FILTER (WHERE o.order_status = 'canceled')   AS canceled_orders,
           count(r.order_id)                                     AS reviewed_orders,
           avg(r.review_score)                                   AS avg_review_score
    FROM so
    JOIN {{ ref('fact_order') }} o USING (order_id)
    LEFT JOIN {{ ref('fact_review') }} r USING (order_id)
    GROUP BY so.seller_id
), scored AS (
    SELECT a.seller_id, s.state, s.city, a.orders,
           coalesce(a.revenue, 0)::numeric(14,2) AS revenue,
           a.delivered_orders, a.late_orders,
           round(100.0 * a.late_orders / nullif(a.delivered_orders, 0), 2)     AS late_rate_pct,
           a.canceled_orders,
           round(100.0 * a.canceled_orders / a.orders, 2)                      AS canceled_rate_pct,
           a.reviewed_orders, round(a.avg_review_score, 3)                     AS avg_review_score,
           (a.orders >= 30)                                                    AS is_rated
    FROM agg a JOIN {{ ref('dim_seller') }} s USING (seller_id)
)
SELECT sc.*,
       (is_rated AND late_rate_pct >= 15)       AS flag_late,
       (is_rated AND canceled_rate_pct >= 5)    AS flag_canceled,
       (is_rated AND avg_review_score <= 3.5)   AS flag_review,
       (is_rated AND (late_rate_pct >= 15 OR canceled_rate_pct >= 5 OR avg_review_score <= 3.5)) AS needs_action
FROM scored sc
