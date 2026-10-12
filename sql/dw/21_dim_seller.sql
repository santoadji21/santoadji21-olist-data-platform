CREATE TABLE dw.dim_seller AS
SELECT s.seller_id,
       CASE WHEN g.geo_key IS NULL THEN 'unknown' ELSE s.seller_zip_code_prefix::text END AS geo_key,
       s.seller_zip_code_prefix AS zip_code_prefix,
       s.seller_city            AS city,
       s.seller_state           AS state
FROM raw.sellers s
LEFT JOIN dw.dim_geo g ON g.geo_key = s.seller_zip_code_prefix::text;

ALTER TABLE dw.dim_seller ADD PRIMARY KEY (seller_id);
