-- Porting dari T0–T6 sql/dw/33_fact_review.sql (@ cf8b911). PK dan FK dipasang sebagai constraint Postgres lewat post_hook.
-- depends_on: {{ ref('fact_order') }}
{{ config(
    post_hook=[
        "alter table {{ this }} add primary key (order_id)",
        "alter table {{ this }} add foreign key (order_id) references {{ ref('fact_order') }} (order_id)"
    ]
) }}

-- fact_review: grain = satu baris per ORDER yang punya ulasan (98.673).
-- Kebijakan untuk order dengan > 1 ulasan (547 order): ambil ulasan DENGAN review_answer_timestamp TERBARU;
-- seri → review_creation_date terbaru → review_id terkecil (deterministik). review_count menyimpan jumlah asli.
-- Alternatif yang ditolak: rata-rata skor semua ulasan (menyembunyikan perubahan pendapat pelanggan dan
-- membuat skor tidak lagi bilangan bulat 1–5).
SELECT DISTINCT ON (order_id)
       order_id, review_id, review_score, review_creation_date, review_answer_timestamp,
       (review_comment_title   IS NOT NULL) AS has_comment_title,
       (review_comment_message IS NOT NULL) AS has_comment_message,
       count(*) OVER (PARTITION BY order_id) AS review_count
FROM {{ ref('stg_order_reviews') }}
ORDER BY order_id, review_answer_timestamp DESC, review_creation_date DESC, review_id
