import itertools
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from olist_v2.consumer import process_batch as _process
from test_load import _dsn

SQL = (Path(__file__).resolve().parent.parent / "olist_v2" / "sql" / "10_stream.sql").read_text()
NOW = datetime(2026, 10, 12, 10, 0, 1, tzinfo=timezone.utc)


@pytest.fixture
def conn():
    import psycopg2

    try:
        c = psycopg2.connect(_dsn(), connect_timeout=3)
    except psycopg2.OperationalError as e:
        pytest.skip(f"Postgres v2 tidak bisa dihubungi: {e}")
    with c, c.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS zz_t3 CASCADE")
        cur.execute(SQL.replace("stream", "zz_t3"))   # schema uji sendiri: tabel stream.* asli tidak tersentuh
    yield c
    c.rollback()
    with c, c.cursor() as cur:
        cur.execute("DROP SCHEMA zz_t3 CASCADE")
    c.close()


def one(c, sql):
    with c.cursor() as cur:
        cur.execute(sql)
        return cur.fetchone()


_batch_no = itertools.count(1)


def process_batch(conn, records, now):
    return _process(conn, records, now, run_id="t", batch_no=next(_batch_no), schema="zz_t3")


def rec(off, **over):
    m = {"event_id": "o1:created", "order_id": "o1", "status": "created", "event_ts": "2017-01-01 00:00:00",
         "produced_at": "2026-10-12T10:00:00+00:00"}
    m.update(over)
    return {"topic": "t", "partition": 0, "offset": off, "key": b"o1", "value": json.dumps(m).encode()}


def ev(off, status):
    return rec(off, event_id=f"o1:{status}", status=status)


def status(c):
    return one(c, "SELECT status FROM zz_t3.order_current WHERE order_id='o1'")[0]


def test_duplicate_is_ignored_and_counted(conn):
    m = process_batch(conn, [ev(0, "created"), ev(1, "created")], NOW)
    assert (m["n_new"], m["n_duplicate"]) == (1, 1)
    m2 = process_batch(conn, [ev(2, "created")], NOW)           # duplikat lintas batch
    assert (m2["n_new"], m2["n_duplicate"]) == (0, 1)
    assert one(conn, "SELECT count(*) FROM zz_t3.order_events")[0] == 1


def test_late_event_is_logged_but_does_not_lower_status(conn):
    process_batch(conn, [ev(0, "delivered")], NOW)
    m = process_batch(conn, [ev(1, "shipped")], NOW)
    assert m["n_out_of_order"] == 1 and status(conn) == "delivered"
    assert one(conn, "SELECT count(*) FROM zz_t3.order_events")[0] == 2


def test_out_of_order_inside_one_batch_still_ends_at_highest(conn):
    process_batch(conn, [ev(0, "delivered"), ev(1, "created"), ev(2, "shipped")], NOW)
    assert status(conn) == "delivered"


def test_canceled_is_not_overwritten_by_late_delivered(conn):
    process_batch(conn, [ev(0, "canceled"), ev(1, "delivered")], NOW)
    assert status(conn) == "canceled"


def test_bad_messages_go_to_dlq_and_do_not_crash_the_batch(conn):
    bad = {"topic": "t", "partition": 0, "offset": 5, "key": None, "value": b"<<garbage>>"}
    m = process_batch(conn, [ev(0, "created"), bad], NOW)
    assert (m["n_new"], m["n_dlq"]) == (1, 1)
    process_batch(conn, [bad], NOW)                              # redelivery setelah crash → baris DLQ tidak dobel
    assert one(conn, "SELECT count(*), min(reason) FROM zz_t3.dlq") == (1, "not_json")


def test_redelivered_batch_gives_same_final_state(conn):
    batch = [ev(0, "created"), ev(1, "approved"), ev(2, "shipped")]
    process_batch(conn, batch, NOW)
    process_batch(conn, batch, NOW)                              # simulasi crash sebelum commit offset
    assert one(conn, "SELECT count(*) FROM zz_t3.order_events")[0] == 3
    assert status(conn) == "shipped"
