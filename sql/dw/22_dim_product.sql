-- dim_product: typo kolom sumber diperbaiki di sini (lenght → length). Kategori Inggris dengan fallback:
-- null → 'unknown'; tanpa terjemahan → nama Portugis apa adanya (13 produk, mis. pc_gamer).
CREATE TABLE dw.dim_product AS
SELECT p.product_id,
       p.product_category_name                                                   AS category_pt,
       coalesce(t.product_category_name_english, p.product_category_name, 'unknown') AS category_en,
       p.product_name_lenght        AS name_length,
       p.product_description_lenght AS description_length,
       p.product_photos_qty         AS photos_qty,
       p.product_weight_g           AS weight_g,
       p.product_length_cm          AS length_cm,
       p.product_height_cm          AS height_cm,
       p.product_width_cm           AS width_cm
FROM raw.products p
LEFT JOIN raw.product_category_translation t USING (product_category_name);

ALTER TABLE dw.dim_product ADD PRIMARY KEY (product_id);
