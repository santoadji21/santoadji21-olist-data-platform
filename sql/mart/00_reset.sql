-- Rebuild penuh mart dalam satu transaksi (mart murni turunan dw + stream). Gagal → rollback, mart lama utuh.
DROP SCHEMA IF EXISTS mart CASCADE;
CREATE SCHEMA mart;
