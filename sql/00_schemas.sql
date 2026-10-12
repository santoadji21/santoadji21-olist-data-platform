-- Skema platform. Buat idempoten (aman dijalankan berulang) dan panggil dari `pipeline.py init`.
-- Tabel stream.* (T3), dw.* (T4), mart.* (T5) kamu yang rancang — ini hanya pembaginya.
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS stream;
CREATE SCHEMA IF NOT EXISTS dw;
CREATE SCHEMA IF NOT EXISTS mart;
