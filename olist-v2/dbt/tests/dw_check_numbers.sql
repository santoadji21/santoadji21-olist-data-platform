-- Angka cek dari Tugas Besar (§8.2). Lulus bila tidak ada selisih.
with n as (
    select 'dim_customer (orang unik)'            as pemeriksaan, (select count(*) from {{ ref('dim_customer') }})                                    as aktual, 96096  as diharapkan
    union all select 'fact_order_item',                         (select count(*) from {{ ref('fact_order_item') }}),                                  112650
    union all select 'fact_order',                              (select count(*) from {{ ref('fact_order') }}),                                       99441
    union all select 'order tanpa item',                        (select count(*) from {{ ref('fact_order') }} where not has_items),                   775
    union all select 'order tanpa pembayaran',                  (select count(*) from {{ ref('fact_order') }} where not has_payment),                 1
    union all select 'order dengan >= 1 review (fact_review)',  (select count(*) from {{ ref('fact_review') }}),                                      98673
    union all select 'order delivered',                         (select count(*) from {{ ref('fact_order') }} where order_status = 'delivered'),      96478
    union all select 'bulan lengkap',                           (select count(distinct year_month) from {{ ref('dim_date') }} where is_complete_month), 20
)
select * from n where aktual <> diharapkan
