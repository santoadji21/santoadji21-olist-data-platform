-- FK dipasang (bukan hanya didokumentasikan). Bila ada baris yatim, ALTER ini GAGAL dan seluruh rebuild rollback.
ALTER TABLE dw.dim_customer     ADD FOREIGN KEY (geo_key)            REFERENCES dw.dim_geo (geo_key);
ALTER TABLE dw.dim_seller       ADD FOREIGN KEY (geo_key)            REFERENCES dw.dim_geo (geo_key);
ALTER TABLE dw.fact_order       ADD FOREIGN KEY (customer_unique_id) REFERENCES dw.dim_customer (customer_unique_id);
ALTER TABLE dw.fact_order       ADD FOREIGN KEY (purchase_date_key)  REFERENCES dw.dim_date (date_key);
ALTER TABLE dw.fact_order       ADD FOREIGN KEY (delivered_date_key) REFERENCES dw.dim_date (date_key);
ALTER TABLE dw.fact_order       ADD FOREIGN KEY (estimated_date_key) REFERENCES dw.dim_date (date_key);
ALTER TABLE dw.fact_order_item  ADD FOREIGN KEY (order_id)           REFERENCES dw.fact_order (order_id);
ALTER TABLE dw.fact_order_item  ADD FOREIGN KEY (product_id)         REFERENCES dw.dim_product (product_id);
ALTER TABLE dw.fact_order_item  ADD FOREIGN KEY (seller_id)          REFERENCES dw.dim_seller (seller_id);
ALTER TABLE dw.fact_order_item  ADD FOREIGN KEY (customer_unique_id) REFERENCES dw.dim_customer (customer_unique_id);
ALTER TABLE dw.fact_order_item  ADD FOREIGN KEY (purchase_date_key)  REFERENCES dw.dim_date (date_key);
ALTER TABLE dw.fact_payment     ADD FOREIGN KEY (order_id)           REFERENCES dw.fact_order (order_id);
ALTER TABLE dw.fact_review      ADD FOREIGN KEY (order_id)           REFERENCES dw.fact_order (order_id);
