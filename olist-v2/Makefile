# olist-v2 — pembungkus docker compose. Jalankan dari folder olist-v2/.
# `-p olist-v2` MENGALAHKAN COMPOSE_PROJECT_NAME yang mungkin diekspor shell (proyek lain memakai `olist`).
COMPOSE ?= docker compose -p olist-v2
P       ?=                       # profil tambahan, mis. make up P="spark stream"
PROFILES = $(foreach p,$(P),--profile $(p))
ALL      = --profile spark --profile stream --profile bi --profile lab --profile contract

.DEFAULT_GOAL := help

VENV ?= .venv
.PHONY: benchmark all report smoke inject restore stream stream-small stream-report stream-reset marts dashboard dbt-build dbt-docs dag-test task-test reset-layers reset-all build-core download bronze silver gate load test venv help init build up up-all down reset ps logs psql minio-ls kafka-topics notebook-token config

help: ## daftar target
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-13s %s\n", $$1, $$2}'
	@echo; echo "  profil: spark | stream (kafka) | bi (metabase) | lab (jupyter) | contract (editor ODCS)"

download: ## unduh dataset dari Kaggle ke data/raw (perlu .kaggle/kaggle.json)
	@test -f .kaggle/kaggle.json || { echo "taruh kaggle.json di olist-v2/.kaggle/ dulu (salin dari ~/.kaggle/kaggle.json)"; exit 1; }
	$(COMPOSE) exec -T airflow-scheduler python -m olist_v2.download

bronze: ## CSV -> MinIO bronze + manifest (perintah mandiri, tanpa UI Airflow)
	$(COMPOSE) exec -T airflow-scheduler python -m olist_v2.bronze

silver: ## bronze -> silver (Spark lokal) + gate + karantina -> outputs/dq_report.md
	$(COMPOSE) exec -T airflow-scheduler python -m olist_v2.silver

gate: ## datacontract test terhadap silver di MinIO -> outputs/contract_gate.md
	$(COMPOSE) exec -T airflow-scheduler python -m olist_v2.gate

load: ## silver -> raw.* di Postgres (TRUNCATE+COPY, satu transaksi) -> outputs/load_report.md
	$(COMPOSE) exec -T airflow-scheduler python -m olist_v2.load

dbt-build: ## raw -> staging -> dw -> mart (dbt build: model + 65 uji) dari laptop (venv lokal; Postgres di localhost:15432)
	@$(COMPOSE) exec -T airflow-scheduler python -c "from olist_v2.stream_cli import connect; connect().close()"   # skema stream.* (sumber mart freshness) harus ada
	@set -a; . ./.env; set +a; DBT_PG_PASSWORD=$$V2_PG_PASSWORD DBT_PG_USER=$$V2_PG_USER DBT_PG_PORT=$$V2_PG_PORT $(VENV)/bin/dbt build --project-dir dbt --profiles-dir dbt

all: ## SATU PERINTAH dari nol: [reset (YES=1)] -> up -> download -> bronze -> silver -> gate -> load -> stream -> dbt-build -> marts -> dashboard; waktu per tahap -> outputs/run_all.json
	$(VENV)/bin/python -m olist_v2.run_all $(if $(filter 1,$(YES)),--fresh,)

benchmark: ## Spark vs DuckDB pada silver, skala 1x (3 ulangan), 5x, 10x -> outputs/benchmark_report.md (beberapa menit; Spark bisa gagal di 10x = hasil)
	$(COMPOSE) exec -T airflow-scheduler python -m olist_v2.bench suite $(if $(SCALES),--scales $(SCALES),)

report: ## EVALUATION_EVIDENCE.md dibangkitkan dari outputs/*.json (tanpa angka ketikan tangan)
	$(VENV)/bin/python -m olist_v2.evidence

smoke: ## pemeriksaan menyeluruh: service sehat, tiap lapisan konsisten, stream benar (exit != 0 bila ada gagal)
	$(VENV)/bin/python -m olist_v2.failures smoke

inject: ## failure injection T6 (6 kasus, perusak sungguhan) lalu pulihkan stack -> outputs/failure_tests.md (±10 menit; ONLY=a,c untuk sebagian)
	$(VENV)/bin/python -m olist_v2.failures inject $(if $(ONLY),--only $(ONLY),)

restore: ## pulihkan stack setelah inject: replay stream penuh + dbt + marts + smoke
	$(VENV)/bin/python -m olist_v2.failures restore

stream: ## replay penuh 99.441 order: reset, producer + consumer, laporan (butuh `make up P=stream` + load)
	$(COMPOSE) exec -T airflow-scheduler python -m olist_v2.stream_cli run

stream-small: ## replay 5.000 order (cepat, untuk uji)
	$(COMPOSE) exec -T airflow-scheduler python -m olist_v2.stream_cli run --limit 5000

stream-report: ## laporan producer vs consumer dari kondisi sekarang
	$(COMPOSE) exec -T airflow-scheduler python -m olist_v2.stream_cli report

stream-reset: ## kosongkan topic, consumer group, dan tabel stream.*
	$(COMPOSE) exec -T airflow-scheduler python -m olist_v2.stream_cli reset

marts: ## 17 pemeriksaan mart + ekspor gold + outputs/marts_report.md (jalan di container; butuh dbt-build dulu)
	$(COMPOSE) exec -T airflow-scheduler python -m olist_v2.marts

dashboard: ## siapkan Metabase (admin, koneksi, 8 kartu + insight, dashboard) dari olist_v2/questions.py; butuh `make up P=bi` dan mart
	@set -a; . ./.env; set +a; WAREHOUSE_DSN="postgresql://$$V2_PG_USER:$$V2_PG_PASSWORD@localhost:$$V2_PG_PORT/olist_dw" METABASE_URL="http://localhost:$$V2_METABASE_PORT" $(VENV)/bin/python -m olist_v2.metabase

dbt-docs: ## dokumentasi dan lineage dbt -> dbt/target/ (buka dbt/target/index.html)
	@set -a; . ./.env; set +a; DBT_PG_PASSWORD=$$V2_PG_PASSWORD DBT_PG_USER=$$V2_PG_USER DBT_PG_PORT=$$V2_PG_PORT $(VENV)/bin/dbt docs generate --project-dir dbt --profiles-dir dbt

dag-test: ## jalankan SELURUH DAG di proses sendiri, log tercetak di layar (tidak bergantung pada file log Airflow)
	$(COMPOSE) exec -T airflow-scheduler airflow dags test olist_v2_pipeline

task-test: ## satu task di layar: make task-test T=contract_gate
	@test -n "$(T)" || { echo "pakai: make task-test T=<task_id>"; exit 2; }
	$(COMPOSE) exec -T airflow-scheduler airflow tasks test olist_v2_pipeline $(T) $$(date -u +%Y-%m-%d)

test: ## pytest tanpa Docker (venv lokal)
	$(VENV)/bin/python -m pytest tests -q

venv: ## venv lokal untuk IDE/plugin dbt (versi sama dengan image Airflow)
	uv venv $(VENV) --python 3.11 && uv pip install --python $(VENV)/bin/python -r requirements-dev.txt
	@$(VENV)/bin/dbt --version | head -3

init: ## buat .env dari .env.example
	@test -f .env || { cp .env.example .env; echo ".env dibuat"; }

config: init ## validasi compose (semua profil), tanpa menyalakan apa pun
	$(COMPOSE) $(ALL) config -q && echo "compose valid"

build-core: init ## build HANYA image Airflow (cukup untuk fase inti; Spark/Jupyter belum perlu)
	$(COMPOSE) build airflow-webserver

build: init ## build SEMUA image kustom (airflow, spark, jupyter) — pertama kali beberapa menit
	$(COMPOSE) $(ALL) build

up: init ## nyalakan inti (+ profil di P=), TANPA --wait; lihat make ps
	$(COMPOSE) $(PROFILES) up -d

up-all: init ## nyalakan SEMUA profil (butuh RAM besar; Docker saat ini 7,8 GB)
	$(COMPOSE) $(ALL) up -d

down: ## matikan (volume tetap)
	$(COMPOSE) $(ALL) down

# ---- reset bertingkat (semua meminta konfirmasi; lewati dengan YES=1) --------------------------------------------
define CONFIRM
	@if [ "$(YES)" != "1" ]; then printf "$(1) Lanjut? [y/N] "; read a; [ "$$a" = "y" ] || { echo "dibatalkan"; exit 1; }; fi
endef

reset-layers: ## [ringan] kosongkan bucket bronze/silver/gold + schema raw/staging/dw/mart; stack TETAP hidup
	$(call CONFIRM,Mengosongkan SEMUA objek di bucket bronze/silver/gold dan schema raw/staging/dw/mart di olist_dw.)
	@$(COMPOSE) exec -T minio sh -c 'mc alias set local http://localhost:9000 "$$MINIO_ROOT_USER" "$$MINIO_ROOT_PASSWORD" >/dev/null && for b in bronze silver gold; do mc rm --recursive --force local/$$b >/dev/null 2>&1 || true; done'
	@$(COMPOSE) exec -T postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$$POSTGRES_USER" -d olist_dw -c "DROP SCHEMA IF EXISTS raw CASCADE; DROP SCHEMA IF EXISTS staging CASCADE; DROP SCHEMA IF EXISTS dw CASCADE; DROP SCHEMA IF EXISTS mart CASCADE; CREATE SCHEMA raw; CREATE SCHEMA staging; CREATE SCHEMA dw; CREATE SCHEMA mart;"'
	@echo "layer dikosongkan. Hilang: objek bronze (beserta riwayat versi manifest), silver, gold, dan isi raw/staging/dw/mart. Tetap: CSV di data/raw, riwayat run Airflow, kode, .env."

reset: ## [sedang] matikan DAN hapus volume olist-v2-* (Postgres, MinIO, Kafka) dan isi airflow/logs/; CSV data/raw tetap
	$(call CONFIRM,Menghapus SEMUA volume olist-v2 (Postgres termasuk database airflow/metabase, MinIO, Kafka) dan isi airflow/logs/.)
	$(COMPOSE) $(ALL) down -v
	rm -rf airflow/logs/*
	@echo "volume dan log Airflow (airflow/logs/) dihapus. Tetap: CSV di data/raw, kode, .env, outputs/. Mulai lagi: make up"

reset-all: ## [penuh] reset + hapus CSV unduhan di data/raw (unduh ulang dari Kaggle saat run berikutnya)
	$(call CONFIRM,Menghapus semua volume olist-v2, isi airflow/logs/, DAN CSV hasil unduhan di data/raw.)
	$(COMPOSE) $(ALL) down -v
	rm -rf airflow/logs/*
	rm -f data/raw/*.csv data/raw/*.zip
	rm -rf data/raw/.dl-*
	@echo "semuanya dihapus. Tetap: kode, .env, .kaggle/, outputs/. Mulai lagi: make up lalu jalankan DAG (akan mengunduh dari Kaggle)."

ps: ## status
	$(COMPOSE) $(ALL) ps -a

logs: ## log 50 baris terakhir; contoh: make logs S=airflow-scheduler
	$(COMPOSE) $(ALL) logs --tail 50 $(S)

psql: ## psql ke warehouse olist_dw
	$(COMPOSE) exec postgres sh -c 'psql -U "$$POSTGRES_USER" -d olist_dw'

minio-ls: ## isi bucket lewat mc (web console: http://localhost:19001)
	@$(COMPOSE) exec minio sh -c 'mc alias set local http://localhost:9000 "$$MINIO_ROOT_USER" "$$MINIO_ROOT_PASSWORD" >/dev/null && mc ls --recursive local'

kafka-topics: ## daftar topic Kafka (profil stream)
	$(COMPOSE) --profile stream exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

notebook-token: ## token Jupyter dari .env
	@grep '^V2_JUPYTER_TOKEN=' .env | cut -d= -f2
