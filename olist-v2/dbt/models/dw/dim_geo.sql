-- Porting dari T0–T6 sql/dw/10_dim_geo.sql (@ cf8b911). PK dan FK dipasang sebagai constraint Postgres lewat post_hook.
{{ config(
    post_hook=[
        "alter table {{ this }} add primary key (geo_key)"
    ]
) }}

-- dim_geo: satu baris per zip prefix. Titik di luar Brasil (raw flag is_outside_brazil) tidak ikut.
-- avg() untuk koordinat, mode() untuk kota/negara bagian. Plus baris 'unknown' agar FK customer/seller selalu valid.
with geo as (
SELECT geolocation_zip_code_prefix::text                        AS geo_key,   -- text: muat 'unknown'
       avg(geolocation_lat)                                     AS lat,
       avg(geolocation_lng)                                     AS lng,
       mode() WITHIN GROUP (ORDER BY geolocation_city)          AS city,
       mode() WITHIN GROUP (ORDER BY geolocation_state)         AS state,
       count(*)                                                 AS n_points
FROM {{ ref('stg_geolocation') }}
WHERE NOT is_outside_brazil
GROUP BY geolocation_zip_code_prefix
)
select * from geo
union all
select 'unknown'::text, null::double precision, null::double precision, 'unknown'::text, 'XX'::text, 0::bigint
