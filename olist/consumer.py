"""T3 — Consumer: validasi → DLQ / log event / status terkini, semuanya dalam satu transaksi per batch.

Urutan commit (inti jaminan): (1) simpan ke Postgres + commit transaksi, (2) kirim DLQ ke topic + flush,
(3) baru commit offset Kafka. Mati di antara (1) dan (3) → pesan dikirim ulang saat restart → event sudah ada
(PK event_id) → diabaikan; baris DLQ sudah ada (UNIQUE topic/partition/offset) → diabaikan. Hasil akhir sama.
Yang TIDAK idempoten: publikasi ke TOPIC DLQ (at-least-once; bisa dobel setelah crash).
"""
from __future__ import annotations

import os
import statistics
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from .stream import Invalid, apply_order_current, validate

UPSERT_CURRENT = """
INSERT INTO {s}.order_current (order_id, status, status_rank, last_event_ts)
SELECT * FROM unnest(%s::text[], %s::text[], %s::smallint[], %s::timestamp[])
ON CONFLICT (order_id) DO UPDATE
   SET status = EXCLUDED.status, status_rank = EXCLUDED.status_rank,
       last_event_ts = EXCLUDED.last_event_ts, updated_at = now()
 WHERE {s}.order_current.status_rank < EXCLUDED.status_rank
"""

INSERT_EVENTS = """
INSERT INTO {s}.order_events
  (event_id, order_id, status, status_rank, event_ts, produced_at, consumed_at, ts_imputed, kafka_partition, kafka_offset)
SELECT * FROM unnest(%s::text[], %s::text[], %s::text[], %s::smallint[], %s::timestamp[], %s::timestamptz[],
                     %s::timestamptz[], %s::boolean[], %s::int[], %s::bigint[])
ON CONFLICT (event_id) DO NOTHING
RETURNING event_id
"""

INSERT_DLQ = """
INSERT INTO {s}.dlq (topic, kafka_partition, kafka_offset, reason, msg_key, payload, consumed_at)
SELECT * FROM unnest(%s::text[], %s::int[], %s::bigint[], %s::text[], %s::text[], %s::text[], %s::timestamptz[])
ON CONFLICT (topic, kafka_partition, kafka_offset) DO NOTHING
"""


def process_batch(conn, records: list[dict], consumed_at: datetime, run_id: str = "adhoc", batch_no: int = 0,
                  schema: str = "stream") -> dict:
    """records: [{topic, partition, offset, key, value(bytes)}]. Semua tulis Postgres dalam SATU transaksi.
    `schema` hanya diubah oleh test agar tabel stream.* asli tidak tersentuh."""
    SQL = {k: v.replace("{s}", schema) for k, v in
           (("events", INSERT_EVENTS), ("current", UPSERT_CURRENT), ("dlq", INSERT_DLQ))}
    valid: list[dict] = []
    bad: list[tuple[dict, str]] = []
    for r in records:
        try:
            ev = validate(r["value"])
            ev["partition"], ev["offset"] = r["partition"], r["offset"]
            valid.append(ev)
        except Invalid as e:
            bad.append((r, e.reason))

    seen: set[str] = set()
    first: list[dict] = []          # kemunculan pertama tiap event_id dalam batch, urutan tiba
    for ev in valid:
        if ev["event_id"] not in seen:
            seen.add(ev["event_id"])
            first.append(ev)

    with conn.transaction(), conn.cursor() as cur:
        new_ids: set[str] = set()
        if first:
            cols = list(zip(*[(e["event_id"], e["order_id"], e["status"], e["rank"], e["event_ts"], e["produced_at"],
                               consumed_at, e["ts_imputed"], e["partition"], e["offset"]) for e in first]))
            cur.execute(SQL["events"], [list(c) for c in cols])
            new_ids = {row[0] for row in cur.fetchall()}
        new = [e for e in first if e["event_id"] in new_ids]

        out_of_order = 0
        best: dict[str, dict] = {}
        if new:
            order_ids = list({e["order_id"] for e in new})
            cur.execute(f"SELECT order_id, status_rank FROM {schema}.order_current WHERE order_id = ANY(%s)", (order_ids,))
            rank_now: dict[str, int] = dict(cur.fetchall())
            for e in new:   # urutan tiba: out-of-order = event yang peringkatnya di bawah status yang sudah tersimpan
                cur_rank = rank_now.get(e["order_id"])
                if cur_rank is not None and e["rank"] < cur_rank:
                    out_of_order += 1
                if apply_order_current(cur_rank, e["rank"]):
                    rank_now[e["order_id"]] = e["rank"]
                    best[e["order_id"]] = e
            if best:
                b = list(best.values())
                cur.execute(SQL["current"], [[x["order_id"] for x in b], [x["status"] for x in b],
                                             [x["rank"] for x in b], [x["event_ts"] for x in b]])
        if bad:
            cur.execute(SQL["dlq"], [
                [r["topic"] for r, _ in bad], [r["partition"] for r, _ in bad], [r["offset"] for r, _ in bad],
                [why for _, why in bad],
                [r["key"].decode("utf-8", "replace") if r.get("key") else None for r, _ in bad],
                [(r["value"] or b"").decode("utf-8", "replace") for r, _ in bad],
                [consumed_at] * len(bad)])
        fresh = [(consumed_at - e["produced_at"].replace(tzinfo=e["produced_at"].tzinfo or timezone.utc)).total_seconds() * 1000
                 for e in new]
        p50 = statistics.median(fresh) if fresh else None
        p95 = (statistics.quantiles(fresh, n=100, method="inclusive")[94] if len(fresh) >= 2 else (fresh[0] if fresh else None))
        m = {"run_id": run_id, "batch_no": batch_no, "n_messages": len(records), "n_new": len(new),
             "n_duplicate": len(valid) - len(new), "n_out_of_order": out_of_order, "n_dlq": len(bad),
             "fresh_p50_ms": p50, "fresh_p95_ms": p95}
        cur.execute(
            f"INSERT INTO {schema}.batch_metrics VALUES (%(run_id)s,%(batch_no)s,%(c)s,%(n_messages)s,%(n_new)s,"
            "%(n_duplicate)s,%(n_out_of_order)s,%(n_dlq)s,%(fresh_p50_ms)s,%(fresh_p95_ms)s)",
            {**m, "c": consumed_at})
    m["dlq_records"] = [(r, why) for r, why in bad]
    return m


def run_consumer(env: dict, conn, *, idle_exit: float = 10.0, start_grace: float = 90.0, batch_size: int = 2000,
                 crash_at_batch: int | None = None, max_batches: int | None = None, log=print) -> dict:
    """Konsumsi sampai tak ada pesan selama `idle_exit` detik (setelah pesan pertama, atau `start_grace` bila belum ada)."""
    from kafka import KafkaConsumer, KafkaProducer

    from .producer import bootstrap

    topic, dlq_topic = env.get("KAFKA_TOPIC", "olist.order_events"), env.get("KAFKA_DLQ_TOPIC", "olist.order_events.dlq")
    consumer = KafkaConsumer(
        topic, bootstrap_servers=bootstrap(env), group_id=env.get("KAFKA_GROUP", "olist-order-consumer"),
        enable_auto_commit=False, auto_offset_reset="earliest", max_poll_records=batch_size,
        session_timeout_ms=10_000, heartbeat_interval_ms=3_000,
    )
    dlq_producer = KafkaProducer(bootstrap_servers=bootstrap(env), acks="all", retries=5)
    run_id = uuid.uuid4().hex[:8]
    totals = {"run_id": run_id, "batches": 0, "messages": 0, "new": 0, "duplicate": 0, "out_of_order": 0, "dlq": 0}
    t_start = last_msg = time.time()
    got_any = False
    try:
        while True:
            polled = consumer.poll(timeout_ms=1000, max_records=batch_size)
            records = [{"topic": r.topic, "partition": r.partition, "offset": r.offset, "key": r.key, "value": r.value}
                       for rs in polled.values() for r in rs]
            if not records:
                limit = idle_exit if got_any else start_grace
                if time.time() - (last_msg if got_any else t_start) > limit:
                    break
                continue
            got_any, last_msg = True, time.time()
            consumed_at = datetime.now(timezone.utc)
            totals["batches"] += 1
            m = process_batch(conn, records, consumed_at, run_id, totals["batches"])          # (1) Postgres
            futs = [dlq_producer.send(dlq_topic, key=r["key"], value=r["value"], headers=[("reason", why.encode())])
                    for r, why in m["dlq_records"]]
            dlq_producer.flush(timeout=30)                                                    # (2) topic DLQ
            for f in futs:
                f.get(timeout=1)    # gagal kirim DLQ → exception → offset TIDAK di-commit
            if crash_at_batch and totals["batches"] == crash_at_batch:
                log(f"[consumer] SIMULASI CRASH setelah DB commit, sebelum commit offset (batch {crash_at_batch})", flush=True)
                os._exit(137)
            consumer.commit()                                                                 # (3) offset
            for k, src in (("messages", "n_messages"), ("new", "n_new"), ("duplicate", "n_duplicate"),
                           ("out_of_order", "n_out_of_order"), ("dlq", "n_dlq")):
                totals[k] += m[src]
            if totals["batches"] % 20 == 0:
                log(f"[consumer] batch {totals['batches']}: {totals['messages']:,} pesan, {totals['new']:,} baru")
            if max_batches and totals["batches"] >= max_batches:
                break
    finally:
        consumer.close()
        dlq_producer.close()
    totals["seconds"] = round(time.time() - t_start, 1)
    return totals
