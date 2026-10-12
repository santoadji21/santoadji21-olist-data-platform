import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="session")
def db():
    """Koneksi ke Postgres stack; test yang memakainya di-skip kalau stack tidak jalan (bukan gagal)."""
    import psycopg

    from olist_platform.config  # ganti dengan nama paketmu import settings

    try:
        conn = psycopg.connect(settings().pg_dsn, autocommit=True)
    except psycopg.OperationalError as e:
        pytest.skip(f"Postgres tidak bisa dihubungi: {e}")
    yield conn
    conn.close()


def q1(db, sql):
    with db.cursor() as cur:
        cur.execute(sql)
        return cur.fetchone()[0]
