"""T3 — Logika murni streaming (tanpa Kafka/Postgres): event, peringkat status, validasi, gangguan.

Keputusan desain yang dijaga di sini:
  * event_id deterministik = "<order_id>:<status>" → duplikat dikenali tanpa state tambahan, dan dua replay
    yang berbeda menghasilkan id yang sama (idempoten antar-replay: fitur, bukan bug).
  * Status hanya boleh NAIK menurut peringkat. `canceled`/`unavailable` adalah terminal (peringkat tertinggi):
    event `delivered` yang terlambat tidak bisa menimpa `canceled`, dan sebaliknya tidak ada yang menurunkannya.
  * Gangguan "rusak" adalah salinan TAMBAHAN yang dirusak (event aslinya tetap terkirim), sehingga
    event unik tersimpan harus tepat sama dengan event unik asli.
"""
from __future__ import annotations

import heapq
import json
import random
from dataclasses import dataclass, field
from datetime import datetime

# Peringkat status (naik-saja). invoiced/processing hanya ada sebagai status akhir sejumlah kecil order.
RANK = {
    "created": 1, "approved": 2, "invoiced": 3, "processing": 4, "shipped": 5, "delivered": 6,
    "canceled": 9, "unavailable": 9,   # terminal
}
TERMINAL = {"canceled", "unavailable"}
TS_FMT = "%Y-%m-%d %H:%M:%S"

# (status, kolom waktu di raw.orders) — riwayat yang bisa direkonstruksi dari header order.
HISTORY = (
    ("created", "order_purchase_timestamp"),
    ("approved", "order_approved_at"),
    ("shipped", "order_delivered_carrier_date"),
    ("delivered", "order_delivered_customer_date"),
)
REQUIRED = ("event_id", "order_id", "status", "event_ts", "produced_at")


def event_id(order_id: str, status: str) -> str:
    return f"{order_id}:{status}"


def order_events(order: dict) -> list[dict]:
    """Pecah satu baris raw.orders menjadi event per perubahan status.

    Aturan: pakai riwayat yang punya waktu nyata; untuk status non-terminal, hentikan pada status akhir order;
    pastikan event status akhir selalu ada (waktu diisi = waktu terakhir yang diketahui, `ts_imputed=True`).
    Order `delivered` dengan tanggal tiba lebih awal dari tanggal kurir menghasilkan event yang urutan
    waktunya terbalik — out-of-order alami yang harus ditangani consumer.
    """
    final = order["order_status"]
    final_rank = RANK[final]
    evs: list[dict] = []
    for status, col in HISTORY:
        ts = order.get(col)
        if ts is None or (final not in TERMINAL and RANK[status] > final_rank):
            continue
        evs.append({"status": status, "ts": ts, "imputed": False})
    if not evs:  # tidak mungkin (purchase NOT NULL), tapi jaga-jaga
        raise ValueError(f"order {order['order_id']} tanpa waktu apa pun")
    if final not in {e["status"] for e in evs}:
        evs.append({"status": final, "ts": max(e["ts"] for e in evs), "imputed": True})
    return [
        {"event_id": event_id(order["order_id"], e["status"]), "order_id": order["order_id"],
         "status": e["status"], "event_ts": e["ts"].strftime(TS_FMT), "ts_imputed": e["imputed"]}
        for e in sorted(evs, key=lambda e: (e["ts"], RANK[e["status"]]))
    ]


# ---- validasi sisi consumer ---------------------------------------------------------------------
class Invalid(Exception):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def _parse_ts(v, field_name: str) -> datetime:
    try:
        return datetime.fromisoformat(str(v)) if field_name == "produced_at" else datetime.strptime(str(v), TS_FMT)
    except (ValueError, TypeError):
        raise Invalid(f"bad_timestamp:{field_name}") from None


def validate(raw: bytes | None) -> dict:
    """Bytes pesan → event tervalidasi, atau Invalid(reason). Tidak pernah melempar exception lain."""
    if raw is None:
        raise Invalid("empty_message")
    try:
        msg = json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        raise Invalid("not_json") from None
    if not isinstance(msg, dict):
        raise Invalid("not_object")
    for f in REQUIRED:
        if msg.get(f) in (None, ""):
            raise Invalid(f"missing_field:{f}")
    if msg["status"] not in RANK:
        raise Invalid(f"unknown_status:{msg['status']}")
    return {
        "event_id": str(msg["event_id"]), "order_id": str(msg["order_id"]), "status": msg["status"],
        "rank": RANK[msg["status"]], "event_ts": _parse_ts(msg["event_ts"], "event_ts"),
        "produced_at": _parse_ts(msg["produced_at"], "produced_at"), "ts_imputed": bool(msg.get("ts_imputed", False)),
    }


def apply_order_current(current_rank: int | None, rank: int) -> bool:
    """True bila event ini boleh mengubah status terkini (status hanya naik)."""
    return current_rank is None or rank > current_rank


# ---- gangguan terkontrol di producer ------------------------------------------------------------
@dataclass
class Disturbance:
    dup_rate: float = 0.03
    late_rate: float = 0.02
    corrupt_rate: float = 0.005
    late_min: int = 500        # ditahan minimal sekian pesan berikutnya...
    late_max: int = 20_000     # ...dan maksimal sekian
    seed: int = 42


@dataclass
class Item:
    """Satu pesan yang akan dikirim. `corrupt` = None (valid) atau spesifikasi kerusakan yang sudah ditentukan seed."""
    ev: dict
    corrupt: tuple | None = None

    @property
    def key(self) -> str | None:
        return None if self.corrupt and self.corrupt[0] == "missing_field" and self.corrupt[1] == "order_id" else self.ev["order_id"]


@dataclass
class Plan:
    """Urutan kirim + hitungan gangguan (kunci jawaban). Payload dirender SAAT kirim agar produced_at jujur."""
    items: list[Item] = field(default_factory=list)
    counts: dict = field(default_factory=dict)


CORRUPT_KINDS = ("missing_field", "bad_timestamp", "not_json")
_DROPPABLE = ("order_id", "status", "event_id", "event_ts")
_BAD_TS = ("31/12/2017 25:61", "not-a-date", "2017-13-45 99:99:99")


def render(item: Item, produced_at: str) -> tuple[bytes, str | None]:
    msg = {**item.ev, "produced_at": produced_at}
    if item.corrupt is None:
        return json.dumps(msg).encode(), item.key
    kind, arg = item.corrupt
    if kind == "not_json":
        return b"<<garbage:" + str(arg).encode() + b">>", item.key
    if kind == "missing_field":
        msg.pop(arg)
    else:
        msg["event_ts"] = arg
    return json.dumps(msg).encode(), item.key


def build_plan(events: list[dict], d: Disturbance) -> Plan:
    """Susun urutan kirim: waktu kejadian + duplikat + terlambat + rusak (deterministik oleh seed)."""
    rng = random.Random(d.seed)
    counts = {"events_unique": len(events), "duplicates": 0, "late": 0,
              "corrupt": {k: 0 for k in CORRUPT_KINDS}, "messages_total": 0}
    heap: list[tuple[int, int, Item]] = []   # (posisi rilis, urut, item)
    out: list[Item] = []
    seq = 0

    def release(upto: int):
        while heap and heap[0][0] <= upto:
            out.append(heapq.heappop(heap)[2])

    for pos, ev in enumerate(events):
        release(pos)
        item = Item(ev)
        if rng.random() < d.late_rate:       # tahan, kirim jauh sesudahnya
            seq += 1
            heapq.heappush(heap, (pos + rng.randint(d.late_min, d.late_max), seq, item))
            counts["late"] += 1
        else:
            out.append(item)
        if rng.random() < d.dup_rate:        # kirim lagi (sedikit tertunda)
            seq += 1
            heapq.heappush(heap, (pos + rng.randint(0, 200), seq, item))
            counts["duplicates"] += 1
        if rng.random() < d.corrupt_rate:    # salinan rusak TAMBAHAN
            kind = rng.choice(CORRUPT_KINDS)
            arg = rng.choice(_DROPPABLE) if kind == "missing_field" else (
                rng.choice(_BAD_TS) if kind == "bad_timestamp" else rng.random())
            counts["corrupt"][kind] += 1
            out.append(Item(ev, (kind, arg)))
    release(10**18)                          # sisa yang ditahan dikirim di akhir
    counts["messages_total"] = len(out)
    return Plan(out, counts)
