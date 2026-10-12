# Pintu tambahan di atas `python pipeline.py` (pipeline.py tetap satu-satunya entry point logika).
# Pakai: make help
PY      ?= .venv/bin/python
COMPOSE ?= docker compose
SVC     ?=

.DEFAULT_GOAL := help
.PHONY: help init up build down reset ps logs restart shell-pg check profile contract contract-doc

help: ## tampilkan daftar target
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-14s %s\n", $$1, $$2}'

init: ## buat .env dari .env.example, cek port & data mentah
	$(PY) pipeline.py init

up: ## nyalakan stack lalu tunggu healthy (build image jupyter otomatis bila belum ada)
	@if docker image inspect olist/jupyter:local >/dev/null 2>&1; then \
		$(PY) pipeline.py up; \
	else \
		echo "image olist/jupyter:local belum ada -> build dulu (pertama kali, beberapa menit)"; \
		$(PY) pipeline.py up --build; \
	fi

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
