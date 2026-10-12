-- fact_review: grain = satu baris per ORDER yang punya ulasan (98.673).
-- Kebijakan untuk order dengan > 1 ulasan (547 order): ambil ulasan DENGAN review_answer_timestamp TERBARU;
-- seri → review_creation_date terbaru → review_id terkecil (deterministik). review_count menyimpan jumlah asli.
-- Alternatif yang ditolak: rata-rata skor semua ulasan (menyembunyikan perubahan pendapat pelanggan dan
-- membuat skor tidak lagi bilangan bulat 1–5).
CREATE TABLE dw.fact_review AS
SELECT DISTINCT ON (order_id)
       order_id, review_id, review_score, review_creation_date, review_answer_timestamp,
       (review_comment_title   IS NOT NULL) AS has_comment_title,
       (review_comment_message IS NOT NULL) AS has_comment_message,
       count(*) OVER (PARTITION BY order_id) AS review_count
FROM raw.order_reviews
ORDER BY order_id, review_answer_timestamp DESC, review_creation_date DESC, review_id;

ALTER TABLE dw.fact_review ADD PRIMARY KEY (order_id);
