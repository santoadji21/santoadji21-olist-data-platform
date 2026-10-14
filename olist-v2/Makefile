# olist-v2 — pembungkus docker compose. Jalankan dari folder olist-v2/.
# `-p olist-v2` MENGALAHKAN COMPOSE_PROJECT_NAME yang mungkin diekspor shell (proyek lain memakai `olist`).
COMPOSE ?= docker compose -p olist-v2
P       ?=                       # profil tambahan, mis. make up P="spark stream"
PROFILES = $(foreach p,$(P),--profile $(p))
ALL      = --profile spark --profile stream --profile bi --profile lab --profile contract

.DEFAULT_GOAL := help

VENV ?= .venv
.PHONY: build-core download bronze test venv help init build up up-all down reset ps logs psql minio-ls kafka-topics notebook-token config

help: ## daftar target
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-13s %s\n", $$1, $$2}'
	@echo; echo "  profil: spark | stream (kafka) | bi (metabase) | lab (jupyter) | contract (editor ODCS)"

download: ## unduh dataset dari Kaggle ke data/raw (perlu .kaggle/kaggle.json)
	@test -f .kaggle/kaggle.json || { echo "taruh kaggle.json di olist-v2/.kaggle/ dulu (salin dari ~/.kaggle/kaggle.json)"; exit 1; }
	$(COMPOSE) exec -T airflow-scheduler python -m olist_v2.download

bronze: ## CSV -> MinIO bronze + manifest (perintah mandiri, tanpa UI Airflow)
	$(COMPOSE) exec -T airflow-scheduler python -m olist_v2.bronze

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

reset: ## matikan DAN hapus volume olist-v2 saja (olist-v2-*); proyek lain tidak tersentuh
	$(COMPOSE) $(ALL) down -v

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
