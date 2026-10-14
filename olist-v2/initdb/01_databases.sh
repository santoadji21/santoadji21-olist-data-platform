#!/bin/bash
# Dijalankan sekali saat volume Postgres pertama kali dibuat. Satu server, tiga database.
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<-EOSQL
    CREATE DATABASE airflow;
    CREATE DATABASE olist_dw;      -- warehouse: dbt menulis ke sini, Metabase membaca dari sini
    CREATE DATABASE metabase;      -- database aplikasi Metabase
EOSQL
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname olist_dw <<-EOSQL
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE SCHEMA IF NOT EXISTS staging;
    CREATE SCHEMA IF NOT EXISTS dw;
    CREATE SCHEMA IF NOT EXISTS mart;
EOSQL
