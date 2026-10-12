# RUNBOOK — Olist Data Platform

Ditulis untuk orang yang baru pertama kali membuka repo ini (atau kamu sendiri jam 2 pagi).

## 1. Menyalakan dari nol
```
git clone … && cd …
pip install -r requirements.txt
python pipeline.py init          # .env, cek port, skema, topic, bucket
python pipeline.py up --build    # pertama kali ±3 menit (build image jupyter)
python scripts/check_dataset.py  # dataset di data/raw
python pipeline.py all           # atau tahap per tahap: check → … → report
```
Waktu normal tiap tahap di laptop …: profile … s, bronze … s, silver … s, load … s, stream … s, model … s, marts … s.

## 2. Memeriksa tiap lapisan
| Lapisan | Perintah | Yang harus terlihat |
|---|---|---|
| bronze | `docker compose exec minio mc ls lab/bronze/olist/orders/` | … |
| raw | `docker compose exec postgres psql -U … -c "select count(*) from raw.orders"` | 99441 |
| stream | Kafka UI `localhost:8080` → topic … | … |
| dw / mart | `pytest tests/test_dw.py -q` | … passed |

## 3. Symptom → pemeriksaan termurah → perbaikan
| Symptom | Periksa dulu | Perbaikan |
|---|---|---|
| `NoBrokersAvailable` | `docker compose ps kafka`; alamat di `.env` (laptop 29092, container 9092) | … |
| `password authentication failed` | … | … |
| gate silver `FAIL` | `outputs/dq_report.md` → rule mana | … |
| stream_report: status tidak cocok | … | … |
| … (minimal 6) | | |

## 4. Reset
- `python pipeline.py down` → data tetap.
- `python pipeline.py down -v` → **hilang**: … ; jalankan ulang dari: ….
- reset stream saja: ….
