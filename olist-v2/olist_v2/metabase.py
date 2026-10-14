"""Dashboard sebagai kode: siapkan Metabase lewat REST API (idempoten) dari olist_v2.questions.

  * setup admin pertama kali, lalu login; sambungkan database `olist_dw` (host `postgres` di jaringan Docker)
  * satu kartu SQL per pertanyaan + satu dashboard "Olist v2"; di bawah tiap grafik ada kartu teks berisi
    kalimat insight berangka yang dihitung dari query yang SAMA (psycopg2 ke Postgres)
  * dijalankan ulang = kartu/dashboard diperbarui, bukan digandakan
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request

from .questions import QUESTIONS

COLLECTION = "Olist v2"
DASHBOARD = "Olist v2 — penjualan, pengiriman, seller, pelanggan"
DB_NAME = "olist_dw"


class MetabaseError(RuntimeError):
    pass


class Client:
    def __init__(self, base: str):
        self.base, self.token = base.rstrip("/"), None

    def call(self, method: str, path: str, body=None):
        req = urllib.request.Request(self.base + path, method=method, data=None if body is None else json.dumps(body).encode(),
                                     headers={"Content-Type": "application/json", **({"X-Metabase-Session": self.token} if self.token else {})})
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                raw = r.read()
                return json.loads(raw) if raw else None
        except urllib.error.HTTPError as e:
            raise MetabaseError(f"{method} {path} → {e.code}: {e.read().decode()[:400]}") from e

    def wait(self, seconds: int = 180):
        end = time.time() + seconds
        while time.time() < end:
            try:
                if self.call("GET", "/api/health").get("status") == "ok":
                    return
            except Exception:
                pass
            time.sleep(3)
        raise MetabaseError(f"Metabase tidak sehat di {self.base} — jalankan `make up P=bi` lalu `make ps`")


def db_details(env) -> dict:
    return {"host": "postgres", "port": 5432, "dbname": DB_NAME, "user": env["V2_PG_USER"], "password": env["V2_PG_PASSWORD"], "ssl": False}


def login(c: Client, env) -> None:
    props = c.call("GET", "/api/session/properties")
    email, pw = env["V2_METABASE_ADMIN_EMAIL"], env["V2_METABASE_ADMIN_PASSWORD"]
    if not props.get("has-user-setup"):
        c.call("POST", "/api/setup", {"token": props["setup-token"], "user": {"email": email, "password": pw, "first_name": "Olist", "last_name": "Admin", "site_name": "Olist v2"},
                                      "prefs": {"site_name": "Olist v2", "allow_tracking": False}})
    c.token = c.call("POST", "/api/session", {"username": email, "password": pw})["id"]


def ensure_database(c: Client, env) -> int:
    dbs = c.call("GET", "/api/database")
    dbs = dbs["data"] if isinstance(dbs, dict) else dbs
    found = next((d for d in dbs if d["name"] == DB_NAME), None)
    body = {"name": DB_NAME, "engine": "postgres", "details": db_details(env), "is_full_sync": True}
    if found:
        c.call("PUT", f"/api/database/{found['id']}", body)
        return found["id"]
    return c.call("POST", "/api/database", body)["id"]


def ensure_collection(c: Client) -> int:
    cols = c.call("GET", "/api/collection")
    found = next((x for x in cols if x["name"] == COLLECTION and not x.get("archived")), None)
    return found["id"] if found else c.call("POST", "/api/collection", {"name": COLLECTION})["id"]


def fetch(conn, sql: str) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(sql)
        names = [d[0] for d in cur.description]
        return [dict(zip(names, (float(v) if hasattr(v, "as_tuple") else v for v in row))) for row in cur.fetchall()]


def provision(c: Client, conn, env) -> dict:
    login(c, env)
    db_id, col_id = ensure_database(c, env), ensure_collection(c)
    existing = {x["name"]: x for x in c.call("GET", f"/api/collection/{col_id}/items?models=card")["data"]}
    insights, card_ids = {}, {}
    for q in QUESTIONS:
        insights[q.key] = q.insight(fetch(conn, q.sql))
        body = {"name": q.title, "collection_id": col_id, "display": q.display, "visualization_settings": q.viz or {},
                "dataset_query": {"type": "native", "database": db_id, "native": {"query": q.sql.strip()}}}
        card = c.call("PUT", f"/api/card/{existing[q.title]['id']}", body) if q.title in existing else c.call("POST", "/api/card", body)
        card_ids[q.key] = card["id"]
    insights["q3_sellers"] = insights["q3_summary"]
    # tata letak: grafik 12 kolom lebar di kiri, kalimat insight di kanan (6 kolom); grid Metabase = 24 kolom
    dashcards, row, nid = [], 0, -1
    for q in QUESTIONS:
        if q.key == "q3_summary":
            continue
        dashcards.append({"id": nid, "card_id": card_ids[q.key], "row": row, "col": 0, "size_x": 15, "size_y": 7, "visualization_settings": {}, "parameter_mappings": []})
        dashcards.append({"id": nid - 1, "card_id": None, "row": row, "col": 15, "size_x": 9, "size_y": 7, "parameter_mappings": [],
                          "visualization_settings": {"virtual_card": {"name": None, "display": "text", "archived": False, "visualization_settings": {}, "dataset_query": {}},
                                                     "text": f"### Insight\n\n{insights[q.key]}"}})
        row, nid = row + 7, nid - 2
    dashes = c.call("GET", f"/api/collection/{col_id}/items?models=dashboard")["data"]
    dash = next((d for d in dashes if d["name"] == DASHBOARD), None)
    did = dash["id"] if dash else c.call("POST", "/api/dashboard", {"name": DASHBOARD, "collection_id": col_id,
                                                                    "description": "Sumber: mart.* (dbt). Satu kalimat insight berangka per pertanyaan."})["id"]
    c.call("PUT", f"/api/dashboard/{did}", {"dashcards": dashcards})
    return {"dashboard_id": did, "url": f"{c.base}/dashboard/{did}", "cards": card_ids, "insights": insights}


def main(env=os.environ) -> int:
    import psycopg2

    for k in ("V2_PG_USER", "V2_PG_PASSWORD", "V2_METABASE_ADMIN_EMAIL", "V2_METABASE_ADMIN_PASSWORD", "WAREHOUSE_DSN"):
        if not env.get(k):
            print(f"variabel {k} belum diatur (lihat .env.example)", file=sys.stderr)
            return 2
    c = Client(env.get("METABASE_URL", "http://localhost:13000"))
    try:
        c.wait()
        conn = psycopg2.connect(env["WAREHOUSE_DSN"], connect_timeout=5)
        try:
            out = provision(c, conn, env)
        finally:
            conn.close()
    except MetabaseError as e:
        print(f"metabase: {e}", file=sys.stderr)
        return 1
    for k, v in out["insights"].items():
        if v:
            print(f"  {k:13s} {v}")
    print(f"dashboard: {out['url']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
