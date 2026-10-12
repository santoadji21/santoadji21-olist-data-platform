import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="session")
def db():
    """Koneksi ke Postgres stack; test yang memakainya di-skip kalau stack tidak jalan (bukan gagal)."""
    import psycopg

    from pipeline import read_env

    e = read_env()
    dsn = (f"host=localhost port={e.get('POSTGRES_PORT', '5432')} dbname={e.get('POSTGRES_DB', 'olist')} "
           f"user={e.get('POSTGRES_USER', 'olist')} password={e.get('POSTGRES_PASSWORD', '')}")
    try:
        conn = psycopg.connect(dsn, autocommit=True, connect_timeout=3)
    except psycopg.OperationalError as e:
        pytest.skip(f"Postgres tidak bisa dihubungi: {e}")
    yield conn
    conn.close()


def q1(db, sql):
    with db.cursor() as cur:
        cur.execute(sql)
        return cur.fetchone()[0]
