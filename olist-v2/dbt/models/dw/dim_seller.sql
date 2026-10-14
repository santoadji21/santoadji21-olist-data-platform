-- Porting dari T0–T6 sql/dw/21_dim_seller.sql (@ cf8b911). PK dan FK dipasang sebagai constraint Postgres lewat post_hook.
-- depends_on: {{ ref('dim_geo') }}
{{ config(
    post_hook=[
        "alter table {{ this }} add primary key (seller_id)",
        "alter table {{ this }} add foreign key (geo_key) references {{ ref('dim_geo') }} (geo_key)"
    ]
) }}


SELECT s.seller_id,
       CASE WHEN g.geo_key IS NULL THEN 'unknown' ELSE s.seller_zip_code_prefix::text END AS geo_key,
       s.seller_zip_code_prefix AS zip_code_prefix,
       s.seller_city            AS city,
       s.seller_state           AS state
FROM {{ ref('stg_sellers') }} s
LEFT JOIN {{ ref('dim_geo') }} g ON g.geo_key = s.seller_zip_code_prefix::text
