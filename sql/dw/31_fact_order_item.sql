-- fact_order_item: grain = satu baris per UNIT barang, PK (order_id, order_item_id). Harus 112.650 baris.
-- order_status / purchase_date_key / state didenormalisasi seperlunya agar mart tidak selalu join 4 tabel.
CREATE TABLE dw.fact_order_item AS
SELECT i.order_id, i.order_item_id, i.product_id, i.seller_id,
       i.shipping_limit_date, i.price, i.freight_value,
       fo.order_status, fo.purchase_date_key, fo.customer_unique_id,
       fo.customer_state, s.seller_state AS seller_state
FROM raw.order_items i
JOIN dw.fact_order fo USING (order_id)
JOIN raw.sellers s USING (seller_id);

ALTER TABLE dw.fact_order_item ADD PRIMARY KEY (order_id, order_item_id);
