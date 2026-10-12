# Pintu tambahan di atas `python pipeline.py` (pipeline.py tetap satu-satunya entry point logika).
# Pakai: make help
PY      ?= .venv/bin/python
COMPOSE ?= docker compose
SVC     ?=

.DEFAULT_GOAL := help
.PHONY: help init up build down reset ps logs restart shell-pg check profile contract contract-doc bronze silver load test minio-ls minio-cat stream stream-small stream-report model marts all smoke inject report

help: ## tampilkan daftar target
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-14s %s\n", $$1, $$2}'

init: ## buat .env dari .env.example, cek port & data mentah
	$(PY) pipeline.py init

up: ## nyalakan stack lalu tunggu healthy (build image jupyter otomatis bila belum ada)
	$(PY) pipeline.py up

build: ## nyalakan stack dengan build image (jupyter)
	$(PY) pipeline.py up --build

down: ## matikan stack (data volume tetap ada)
	$(PY) pipeline.py down

reset: ## matikan stack DAN hapus volume (Postgres/Kafka/MinIO kosong lagi)
	$(PY) pipeline.py down -v

ps: ## status container
	$(COMPOSE) ps

logs: ## log 50 baris terakhir; contoh: make logs SVC=kafka
	$(COMPOSE) logs --tail 50 $(SVC)

restart: ## restart service; contoh: make restart SVC=postgres
	$(COMPOSE) restart $(SVC)

shell-pg: ## psql ke Postgres di container
	docker exec -it olist-postgres sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

check: ## cek 9 CSV (kolom, jumlah baris, md5)
	$(PY) pipeline.py check

profile: ## outputs/profile.md + profile.json
	$(PY) pipeline.py profile

contract: ## tulis kontrak ODCS + datacontract lint + test
	$(PY) pipeline.py contract

contract-doc: ## bangkitkan DATA_CONTRACT.md
	$(PY) pipeline.py contract-doc

MC = docker exec olist-minio sh -c 'mc alias set local http://localhost:9000 "$$MINIO_ROOT_USER" "$$MINIO_ROOT_PASSWORD" >/dev/null &&

minio-ls: ## daftar semua objek di bucket bronze/silver/gold (tanpa web console)
	@$(MC) mc ls --recursive local/bronze local/silver local/gold'

minio-cat: ## isi satu objek; contoh: make minio-cat OBJ=bronze/olist/_manifest/sellers.json
	@test -n "$(OBJ)" || { echo "pakai: make minio-cat OBJ=bucket/key"; exit 2; }
	@$(MC) mc cat local/$(OBJ)'

bronze: ## CSV -> MinIO bronze + manifest (idempoten)
	$(PY) pipeline.py bronze

silver: ## bronze -> silver Parquet + gate kualitas + karantina
	$(PY) pipeline.py silver

load: ## silver -> raw.* di Postgres (TRUNCATE+COPY, satu transaksi)
	$(PY) pipeline.py load

test: ## pytest (test yang butuh Docker di-skip bila stack mati)
	$(PY) -m pytest tests -q

stream: ## replay penuh 99.441 order: reset, producer + consumer, laporan
	$(PY) pipeline.py stream run

stream-small: ## replay 5.000 order (cepat, untuk uji)
	$(PY) pipeline.py stream run --limit 5000

stream-report: ## laporan producer vs consumer dari kondisi sekarang
	$(PY) pipeline.py stream report

model: ## raw.* -> star schema dw.* (rebuild penuh, satu transaksi)
	$(PY) pipeline.py model

marts: ## dw/stream -> mart.* + ekspor gold + laporan + grafik (outputs/dashboard/*.png)
	$(PY) pipeline.py marts

all: ## seluruh jalur dari .env sampai dashboard (durasi -> outputs/run_all.json)
	$(PY) pipeline.py all

smoke: ## pemeriksaan kesehatan seluruh lapisan
	$(PY) pipeline.py smoke

inject: ## failure injection otomatis (±6 menit) -> outputs/failure_tests.md
	$(PY) pipeline.py inject

report: ## EVALUATION_EVIDENCE.md dari outputs/*.json
	$(PY) pipeline.py report
