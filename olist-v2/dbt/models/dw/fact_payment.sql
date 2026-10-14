-- Porting dari T0–T6 sql/dw/32_fact_payment.sql (@ cf8b911). PK dan FK dipasang sebagai constraint Postgres lewat post_hook.
-- depends_on: {{ ref('fact_order') }}
{{ config(
    post_hook=[
        "alter table {{ this }} add primary key (order_id, payment_sequential)",
        "alter table {{ this }} add foreign key (order_id) references {{ ref('fact_order') }} (order_id)"
    ]
) }}

-- fact_payment: grain = satu baris per PEMBAYARAN, PK (order_id, payment_sequential).
SELECT order_id, payment_sequential, payment_type, payment_installments, payment_value
FROM {{ ref('stg_order_payments') }}
