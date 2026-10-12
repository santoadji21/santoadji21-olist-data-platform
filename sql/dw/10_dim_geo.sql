-- dim_geo: satu baris per zip prefix. Titik di luar Brasil (raw flag is_outside_brazil) tidak ikut.
-- avg() untuk koordinat, mode() untuk kota/negara bagian. Plus baris 'unknown' agar FK customer/seller selalu valid.
CREATE TABLE dw.dim_geo AS
SELECT geolocation_zip_code_prefix::text                        AS geo_key,   -- text: muat 'unknown'
       avg(geolocation_lat)                                     AS lat,
       avg(geolocation_lng)                                     AS lng,
       mode() WITHIN GROUP (ORDER BY geolocation_city)          AS city,
       mode() WITHIN GROUP (ORDER BY geolocation_state)         AS state,
       count(*)                                                 AS n_points
FROM raw.geolocation
WHERE NOT is_outside_brazil
GROUP BY geolocation_zip_code_prefix;

INSERT INTO dw.dim_geo (geo_key, lat, lng, city, state, n_points)
VALUES ('unknown', NULL, NULL, 'unknown', 'XX', 0);

ALTER TABLE dw.dim_geo ADD PRIMARY KEY (geo_key);   -- gagal bila grain salah
