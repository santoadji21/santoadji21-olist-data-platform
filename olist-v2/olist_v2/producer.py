"""T3 — Producer: raw.orders → event per perubahan status → Kafka, dengan gangguan terkontrol.

`KafkaProducer.send()` hanya menaruh pesan di buffer dan mengembalikan future. Kalau broker mati cukup lama,
future itu gagal diam-diam. Karena itu tiap future diberi errback yang menghitung kegagalan, `flush()` dipanggil,
dan run dinyatakan GAGAL (exception) bila ada pesan yang tidak terkirim — bukan sukses palsu.
"""
from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from .stream import RANK, Disturbance, build_plan, order_events, render

ORDER_COLS = ("order_id", "order_status", "order_purchase_timestamp", "order_approved_at",
              "order_delivered_carrier_date", "order_delivered_customer_date")
PARTITIONS = 3


class SendFailure(RuntimeError):
    pass


def bootstrap(env: dict) -> str:
    return env.get("KAFKA_BOOTSTRAP", f"localhost:{env.get('V2_KAFKA_PORT', '19092')}")   # container: kafka:9092 (dari compose)


def ensure_topics(env: dict, partitions: int = PARTITIONS) -> None:
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    admin = KafkaAdminClient(bootstrap_servers=bootstrap(env))
    try:
        for name in (env.get("KAFKA_TOPIC", "olist.order_events"), env.get("KAFKA_DLQ_TOPIC", "olist.order_events.dlq")):
            try:
                admin.create_topics([NewTopic(name, partitions, 1)])
            except TopicAlreadyExistsError:
                pass
    finally:
        admin.close()


def reset_kafka(env: dict, timeout: float = 30.0) -> None:
    """Hapus kedua topic + consumer group, tunggu sampai bersih, buat ulang (replay dari nol)."""
    from kafka.admin import KafkaAdminClient
    from kafka.errors import GroupIdNotFoundError, UnknownTopicOrPartitionError

    admin = KafkaAdminClient(bootstrap_servers=bootstrap(env))
    names = [env.get("KAFKA_TOPIC", "olist.order_events"), env.get("KAFKA_DLQ_TOPIC", "olist.order_events.dlq")]
    try:
        for n in names:
            try:
                admin.delete_topics([n])
            except UnknownTopicOrPartitionError:
                pass
        try:
            admin.delete_consumer_groups([env.get("KAFKA_GROUP", "olist-order-consumer")])
        except (GroupIdNotFoundError, Exception):
            pass
        t0 = time.time()
        while time.time() - t0 < timeout and set(names) & set(admin.list_topics()):
            time.sleep(0.5)
    finally:
        admin.close()
    ensure_topics(env)


def load_orders(conn, limit: int | None) -> list[dict]:
    sql = f"SELECT {', '.join(ORDER_COLS)} FROM raw.orders ORDER BY order_purchase_timestamp, order_id"
    if limit:
        sql += f" LIMIT {int(limit)}"
    with conn.cursor() as cur:
        cur.execute(sql)
        return [dict(zip(ORDER_COLS, r)) for r in cur.fetchall()]


def all_events(orders: list[dict]) -> list[dict]:
    evs = [e for o in orders for e in order_events(o)]
    evs.sort(key=lambda e: (e["event_ts"], RANK[e["status"]], e["order_id"]))   # urutan waktu kejadian global
    return evs


def run_producer(env: dict, conn, *, limit: int | None = None, speed: float = 0.0, seed: int = 42,
                 disturb: Disturbance | None = None, truth_path: Path | None = None, unchecked: bool = False,
                 log=print) -> dict:
    from kafka import KafkaProducer

    ensure_topics(env)
    topic = env.get("KAFKA_TOPIC", "olist.order_events")
    d = disturb or Disturbance(seed=seed)
    orders = load_orders(conn, limit)
    events = all_events(orders)
    plan = build_plan(events, d)
    log(f"[producer] {len(orders):,} order → {len(events):,} event unik → {plan.counts['messages_total']:,} pesan "
        f"(dobel {plan.counts['duplicates']:,}, terlambat {plan.counts['late']:,}, rusak {sum(plan.counts['corrupt'].values()):,})")

    failures: list[str] = []
    producer = KafkaProducer(bootstrap_servers=bootstrap(env), acks="all", retries=5, linger_ms=20,
                             batch_size=64 * 1024, request_timeout_ms=30_000, max_block_ms=30_000)
    run_id = uuid.uuid4().hex[:8]
    started = datetime.now(timezone.utc)
    t0 = time.time()
    try:
        for i, item in enumerate(plan.items, 1):
            payload, key = render(item, datetime.now(timezone.utc).isoformat())
            fut = producer.send(topic, key=key.encode() if key else None, value=payload)
            if not unchecked:   # mode 'unchecked' meniru producer naif: future tak diperiksa (hanya untuk eksperimen T6)
                fut.add_errback(lambda exc: failures.append(repr(exc)))
            if speed and i % 100 == 0:
                lag = i / speed - (time.time() - t0)
                if lag > 0:
                    time.sleep(lag)
            if i % 100_000 == 0:
                log(f"[producer] {i:,}/{len(plan.items):,} pesan")
        try:
            producer.flush(timeout=60)
        except Exception:
            if not unchecked:
                raise
    finally:
        producer.close(timeout=10)
    seconds = round(time.time() - t0, 1)
    run = {"run_id": run_id, "started_at": started.isoformat(timespec="seconds"), "seconds": seconds,
           "seed": d.seed, "limit": limit, "orders": len(orders),
           "rates": {"duplicate": d.dup_rate, "late": d.late_rate, "corrupt": d.corrupt_rate},
           "counts": plan.counts, "send_failures": len(failures), "unchecked": unchecked}
    if truth_path:
        write_truth(truth_path, run)
    if failures:
        raise SendFailure(f"{len(failures):,} pesan gagal terkirim (contoh: {failures[0]})")
    return run


def write_truth(path: Path, run: dict) -> None:
    """Kunci jawaban producer. Append: tiap run producer tercatat (berguna untuk uji 'producer dua kali')."""
    data = json.loads(path.read_text()) if path.exists() else {"runs": []}
    data["runs"].append(run)
    path.parent.mkdir(exist_ok=True)
    path.write_text(json.dumps(data, indent=2))
