-- fact_payment: grain = satu baris per PEMBAYARAN, PK (order_id, payment_sequential).
CREATE TABLE dw.fact_payment AS
SELECT order_id, payment_sequential, payment_type, payment_installments, payment_value
FROM raw.order_payments;

ALTER TABLE dw.fact_payment ADD PRIMARY KEY (order_id, payment_sequential);
