-- Rebuild penuh (bukan inkremental): dw sepenuhnya diturunkan dari raw.*, jadi DROP + CTAS lebih sederhana dan
-- tidak bisa melenceng dari raw. Dijalankan dalam SATU transaksi oleh `pipeline.py model` — gagal di tengah
-- (mis. grain salah → ADD PRIMARY KEY gagal) berarti rollback dan dw lama tetap utuh.
DROP SCHEMA IF EXISTS dw CASCADE;
CREATE SCHEMA dw;
