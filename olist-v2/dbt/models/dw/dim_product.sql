-- Porting dari T0–T6 sql/dw/22_dim_product.sql (@ cf8b911). PK dan FK dipasang sebagai constraint Postgres lewat post_hook.
{{ config(
    post_hook=[
        "alter table {{ this }} add primary key (product_id)"
    ]
) }}

-- dim_product: typo kolom sumber diperbaiki di sini (lenght → length). Kategori Inggris dengan fallback:
-- null → 'unknown'; tanpa terjemahan → nama Portugis apa adanya (13 produk, mis. pc_gamer).
SELECT p.product_id,
       p.product_category_name                                                   AS category_pt,
       coalesce(t.product_category_name_english, p.product_category_name, 'unknown') AS category_en,
       p.product_name_length        AS name_length,
       p.product_description_length AS description_length,
       p.product_photos_qty         AS photos_qty,
       p.product_weight_g           AS weight_g,
       p.product_length_cm          AS length_cm,
       p.product_height_cm          AS height_cm,
       p.product_width_cm           AS width_cm
FROM {{ ref('stg_products') }} p
LEFT JOIN {{ ref('stg_product_category_translation') }} t USING (product_category_name)
