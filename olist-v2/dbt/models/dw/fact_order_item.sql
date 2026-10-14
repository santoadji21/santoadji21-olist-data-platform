-- Porting dari T0–T6 sql/dw/31_fact_order_item.sql (@ cf8b911). PK dan FK dipasang sebagai constraint Postgres lewat post_hook.
-- depends_on: {{ ref('dim_customer') }}
-- depends_on: {{ ref('dim_date') }}
-- depends_on: {{ ref('dim_product') }}
-- depends_on: {{ ref('dim_seller') }}
-- depends_on: {{ ref('fact_order') }}
{{ config(
    post_hook=[
        "alter table {{ this }} add primary key (order_id, order_item_id)",
        "alter table {{ this }} add foreign key (order_id) references {{ ref('fact_order') }} (order_id)",
        "alter table {{ this }} add foreign key (product_id) references {{ ref('dim_product') }} (product_id)",
        "alter table {{ this }} add foreign key (seller_id) references {{ ref('dim_seller') }} (seller_id)",
        "alter table {{ this }} add foreign key (customer_unique_id) references {{ ref('dim_customer') }} (customer_unique_id)",
        "alter table {{ this }} add foreign key (purchase_date_key) references {{ ref('dim_date') }} (date_key)"
    ]
) }}

-- fact_order_item: grain = satu baris per UNIT barang, PK (order_id, order_item_id). Harus 112.650 baris.
-- order_status / purchase_date_key / state didenormalisasi seperlunya agar mart tidak selalu join 4 tabel.
SELECT i.order_id, i.order_item_id, i.product_id, i.seller_id,
       i.shipping_limit_date, i.price, i.freight_value,
       fo.order_status, fo.purchase_date_key, fo.customer_unique_id,
       fo.customer_state, s.seller_state AS seller_state
FROM {{ ref('stg_order_items') }} i
JOIN {{ ref('fact_order') }} fo USING (order_id)
JOIN {{ ref('stg_sellers') }} s USING (seller_id)
