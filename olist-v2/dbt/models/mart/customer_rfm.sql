-- Porting dari T0–T6 sql/mart/40_customer_rfm.sql. PK dipasang lewat post_hook.
{{ config(post_hook=["alter table {{ this }} add primary key (customer_unique_id)"]) }}

-- Q4 — RFM per ORANG (customer_unique_id). Titik acuan recency = tanggal beli terakhir di data (bukan hari ini).
-- Hanya order bernilai: status bukan canceled/unavailable dan punya item. Monetary = Σ price (definisi pendapatan Q1).
-- R dan M: kuintil (ntile 5; seri dipecah deterministik oleh customer_unique_id). 5 = paling baru / paling besar.
-- F BUKAN kuintil mentah: ±96 % orang hanya belanja sekali, jadi ntile(5) memecah ribuan nilai yang sama (F = 1)
-- ke lima skor secara acak. F memakai pita tetap: 1 order → 1, 2 → 2, 3 → 3, 4 → 4, ≥ 5 → 5.
WITH ref AS (SELECT max(purchase_ts)::date AS ref_date FROM {{ ref('fact_order') }}),
base AS (
    SELECT c.customer_unique_id, c.state,
           count(*)                                    AS frequency,
           max(o.purchase_ts)::date                    AS last_order_date,
           sum(o.items_value)::numeric(14,2)           AS monetary
    FROM {{ ref('fact_order') }} o
    JOIN {{ ref('dim_customer') }} c USING (customer_unique_id)
    WHERE o.has_items AND o.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY c.customer_unique_id, c.state
), scored AS (
    SELECT b.*, (ref.ref_date - b.last_order_date) AS recency_days,
           ntile(5) OVER (ORDER BY (ref.ref_date - b.last_order_date) DESC, b.customer_unique_id) AS r_score,
           least(b.frequency, 5)::int                                                              AS f_score,
           ntile(5) OVER (ORDER BY b.monetary, b.customer_unique_id)                               AS m_score
    FROM base b CROSS JOIN ref
)
SELECT s.*,
       CASE
         WHEN f_score >= 2 AND r_score >= 3 THEN 'Pelanggan setia'
         WHEN f_score >= 2 AND r_score <= 2 THEN 'Langganan berisiko hilang'
         WHEN f_score = 1  AND m_score >= 4 AND r_score <= 2 THEN 'Bernilai tinggi, lama tidak kembali'
         WHEN f_score = 1  AND r_score >= 4 THEN 'Baru'
         WHEN f_score = 1  AND m_score >= 4 THEN 'Bernilai tinggi, belum ulang'
         ELSE 'Sekali beli, tidak aktif'
       END AS segment
FROM scored s
