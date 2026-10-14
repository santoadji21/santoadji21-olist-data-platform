select * from {{ source('raw', 'order_items') }}
