"""T3 — Laporan: angka producer (kunci jawaban) vs consumer, dan status terkini vs raw.orders."""
from __future__ import annotations

import json
from pathlib import Path


def q(conn, sql, *a):
    with conn.cursor() as cur:
        cur.execute(sql, a)
        return cur.fetchone()


def topic_total(env: dict, topic: str) -> int:
    """Jumlah pesan di topic (end − beginning offset, semua partisi)."""
    from kafka import KafkaConsumer, TopicPartition

    from .producer import bootstrap
    c = KafkaConsumer(bootstrap_servers=bootstrap(env))
    try:
        parts = [TopicPartition(topic, p) for p in c.partitions_for_topic(topic) or []]
        end, beg = c.end_offsets(parts), c.beginning_offsets(parts)
        return sum(end[p] - beg[p] for p in parts)
    finally:
        c.close()


def collect(env: dict, conn, truth_path: Path) -> dict:
    truth = json.loads(truth_path.read_text())["runs"] if truth_path.exists() else []
    topic, dlq_topic = env.get("KAFKA_TOPIC", "olist.order_events"), env.get("KAFKA_DLQ_TOPIC", "olist.order_events.dlq")
    sent = {
        "runs": len(truth),
        "orders": max((r["orders"] for r in truth), default=0),
        "events_unique": max((r["counts"]["events_unique"] for r in truth), default=0),
        "messages_total": sum(r["counts"]["messages_total"] for r in truth),
        "duplicates": sum(r["counts"]["duplicates"] for r in truth),
        "late": sum(r["counts"]["late"] for r in truth),
        "corrupt": sum(sum(r["counts"]["corrupt"].values()) for r in truth),
        "send_failures": sum(r["send_failures"] for r in truth),
        "producer_seconds": round(sum(r["seconds"] for r in truth), 1),
    }
    on_topic = topic_total(env, topic)
    stored = q(conn, "SELECT count(*), count(DISTINCT event_id) FROM stream.order_events")
    dlq_rows = q(conn, "SELECT count(*) FROM stream.dlq")[0]
    cur_rows = q(conn, "SELECT count(*) FROM stream.order_current")[0]
    match = q(conn, "SELECT count(*) FILTER (WHERE c.status = o.order_status), count(*) FROM stream.order_current c "
                    "JOIN raw.orders o USING (order_id)")
    ooo, nbatch = q(conn, "SELECT coalesce(sum(n_out_of_order),0), count(*) FROM stream.batch_metrics")
    fresh = q(conn, "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM consumed_at - produced_at) * 1000), "
                    "percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM consumed_at - produced_at) * 1000), "
                    "max(extract(epoch FROM consumed_at - produced_at) * 1000) FROM stream.order_events")
    span = q(conn, "SELECT extract(epoch FROM max(consumed_at) - min(produced_at)) FROM stream.order_events")[0]
    mismatch = []
    with conn.cursor() as cur:
        cur.execute("SELECT c.order_id, c.status, o.order_status FROM stream.order_current c "
                    "JOIN raw.orders o USING (order_id) WHERE c.status <> o.order_status LIMIT 5")
        mismatch = cur.fetchall()
    derived_dup = on_topic - stored[0] - dlq_rows
    return {
        "sent": sent,
        "consumer": {
            "messages_on_topic": on_topic, "events_stored": stored[0], "events_distinct": stored[1],
            "dlq_rows": dlq_rows, "duplicates_derived": derived_dup, "out_of_order": int(ooo), "batches": nbatch,
            "order_current_rows": cur_rows, "status_match": match[0], "status_total": match[1],
            "status_mismatch_examples": mismatch,
            "fresh_p50_ms": round(fresh[0], 1) if fresh[0] is not None else None,
            "fresh_p95_ms": round(fresh[1], 1) if fresh[1] is not None else None,
            "fresh_max_ms": round(float(fresh[2]), 1) if fresh[2] is not None else None,
            "end_to_end_seconds": round(float(span), 1) if span is not None else None,
        },
        "checks": {
            "unique_events_equal": stored[0] == sent["events_unique"] and stored[1] == stored[0],
            "duplicates_equal": derived_dup == sent["duplicates"],
            "dlq_equal": dlq_rows == sent["corrupt"],
            "current_status_all_match": match[1] > 0 and match[0] == match[1] and cur_rows == sent["orders"],
        },
    }


def render_md(r: dict) -> str:
    s, c, k = r["sent"], r["consumer"], r["checks"]
    ok = lambda b: "✅ cocok" if b else "❌ BEDA"
    o = ["# Laporan Stream (T3)\n",
         f"Producer: {s['runs']} run, {s['producer_seconds']} s · consumer: {c['batches']} batch · "
         f"ujung ke ujung {c['end_to_end_seconds']} s\n",
         "## Producer (kunci jawaban) vs consumer\n",
         "| Besaran | Producer | Consumer | Hasil |", "|---|---:|---:|---|",
         f"| Event unik | {s['events_unique']:,} | {c['events_stored']:,} (distinct {c['events_distinct']:,}) | {ok(k['unique_events_equal'])} |",
         f"| Duplikat diabaikan | {s['duplicates']:,} | {c['duplicates_derived']:,} (total pesan − unik − DLQ) | {ok(k['duplicates_equal'])} |",
         f"| Pesan rusak → DLQ | {s['corrupt']:,} | {c['dlq_rows']:,} | {ok(k['dlq_equal'])} |",
         f"| Order dengan status terkini | {s['orders']:,} | {c['order_current_rows']:,} | {ok(c['order_current_rows'] == s['orders'])} |",
         f"| Total pesan di topic | {s['messages_total']:,} | {c['messages_on_topic']:,} | {ok(s['messages_total'] == c['messages_on_topic'])} |",
         f"| Pesan gagal terkirim | {s['send_failures']:,} | — | {ok(s['send_failures'] == 0)} |",
         "\n## Status terkini vs `raw.orders.order_status`\n",
         f"{c['status_match']:,} dari {c['status_total']:,} order cocok → {ok(k['current_status_all_match'])}",
         ]
    if c["status_mismatch_examples"]:
        o.append("\nContoh beda: " + "; ".join(f"{a} stream={b} raw={d}" for a, b, d in c["status_mismatch_examples"]))
    o += ["\n## Terlambat / tidak urut\n",
          f"- Event ditahan producer (terlambat): {s['late']:,}",
          f"- Event baru yang tiba di bawah status yang sudah tersimpan (out-of-order, dihitung consumer): {c['out_of_order']:,}. "
          "Angka ini tidak sama dengan jumlah event terlambat: event yang ditahan hanya out-of-order bila status berikutnya sudah tiba, "
          "dan data asli juga menghasilkan out-of-order alami (tanggal tiba lebih awal dari tanggal kurir).",
          "- Event out-of-order tetap tercatat di `stream.order_events`, tetapi tidak menurunkan `stream.order_current` "
          "(status hanya naik).",
          "\n## Freshness (`consumed_at − produced_at`, event baru)\n",
          f"p50 = {c['fresh_p50_ms']} ms · p95 = {c['fresh_p95_ms']} ms · maks = {c['fresh_max_ms']} ms",
          ]
    return "\n".join(o) + "\n"


def write_outputs(root: Path, env: dict, conn) -> tuple[Path, dict]:
    r = collect(env, conn, root / "outputs" / "stream_truth.json")
    out = root / "outputs"
    (out / "stream.json").write_text(json.dumps(r, indent=2, default=str))
    md = out / "stream_report.md"
    md.write_text(render_md(r), encoding="utf-8")
    return md, r
