"""Test logika stream tanpa Docker: dekomposisi event, status naik-saja, validasi, gangguan deterministik."""
import json
from datetime import datetime as D

import pytest

from olist.stream import (RANK, Disturbance, Invalid, apply_order_current, build_plan, event_id, order_events,
                          render, validate)


def order(status, **ts):
    base = {"order_id": "o1", "order_status": status, "order_purchase_timestamp": D(2017, 1, 1),
            "order_approved_at": None, "order_delivered_carrier_date": None, "order_delivered_customer_date": None}
    base.update(ts)
    return base


def final_status(evs):
    """Terapkan aturan 'status hanya naik' dalam urutan tiba → status akhir."""
    cur = None
    for e in evs:
        if apply_order_current(RANK.get(cur), RANK[e["status"]]):
            cur = e["status"]
    return cur


CASES = {
    "delivered_normal": order("delivered", order_approved_at=D(2017, 1, 1, 1), order_delivered_carrier_date=D(2017, 1, 3),
                              order_delivered_customer_date=D(2017, 1, 9)),
    "delivered_missing_date": order("delivered", order_approved_at=D(2017, 1, 1, 1), order_delivered_carrier_date=D(2017, 1, 3)),
    "delivered_before_carrier": order("delivered", order_approved_at=D(2017, 1, 1, 1), order_delivered_carrier_date=D(2017, 1, 5),
                                      order_delivered_customer_date=D(2017, 1, 4)),
    "shipped": order("shipped", order_approved_at=D(2017, 1, 1, 1), order_delivered_carrier_date=D(2017, 1, 3)),
    "canceled_but_has_delivery": order("canceled", order_approved_at=D(2017, 1, 1, 1), order_delivered_carrier_date=D(2017, 1, 3),
                                       order_delivered_customer_date=D(2017, 1, 9)),
    "unavailable": order("unavailable"),
    "invoiced": order("invoiced", order_approved_at=D(2017, 1, 1, 1)),
    "created": order("created"),
}


@pytest.mark.parametrize("name", CASES)
def test_events_end_at_raw_status_even_when_replayed_in_any_order(name):
    o = CASES[name]
    evs = order_events(o)
    assert final_status(evs) == o["order_status"]
    assert final_status(list(reversed(evs))) == o["order_status"]   # tiba terbalik: hasil sama


def test_event_ids_are_deterministic_and_unique_per_order():
    ids = [e["event_id"] for e in order_events(CASES["delivered_normal"])]
    assert ids == [event_id("o1", s) for s in ("created", "approved", "shipped", "delivered")]
    assert order_events(CASES["delivered_normal"]) == order_events(CASES["delivered_normal"])


def test_missing_delivery_date_gets_imputed_final_event():
    last = order_events(CASES["delivered_missing_date"])[-1]
    assert last["status"] == "delivered" and last["ts_imputed"] is True


def test_natural_out_of_order_when_delivered_precedes_carrier():
    assert [e["status"] for e in order_events(CASES["delivered_before_carrier"])][-2:] == ["delivered", "shipped"]


def test_late_event_never_lowers_status_and_canceled_is_terminal():
    assert not apply_order_current(RANK["delivered"], RANK["shipped"])
    assert not apply_order_current(RANK["canceled"], RANK["delivered"])
    assert apply_order_current(None, RANK["created"])


def msg(**over):
    m = {"event_id": "o1:created", "order_id": "o1", "status": "created", "event_ts": "2017-01-01 00:00:00",
         "produced_at": "2026-10-12T10:00:00+00:00"}
    m.update(over)
    return json.dumps(m).encode()


@pytest.mark.parametrize("raw,reason", [
    (b"<<garbage>>", "not_json"),
    (b"[1,2]", "not_object"),
    (None, "empty_message"),
    (msg(status=None), "missing_field:status"),
    (msg(event_ts="not-a-date"), "bad_timestamp:event_ts"),
    (msg(status="teleported"), "unknown_status:teleported"),
    (msg(produced_at="kemarin"), "bad_timestamp:produced_at"),
    (b"\xff\xfe\x00", "not_json"),
])
def test_validate_rejects_with_reason_and_never_raises_other_errors(raw, reason):
    with pytest.raises(Invalid) as e:
        validate(raw)
    assert e.value.reason == reason


def test_validate_accepts_good_message():
    ev = validate(msg())
    assert ev["rank"] == 1 and ev["event_ts"] == D(2017, 1, 1)


def events(n):
    return [{"event_id": f"o{i}:created", "order_id": f"o{i}", "status": "created",
             "event_ts": "2017-01-01 00:00:00", "ts_imputed": False} for i in range(n)]


def test_plan_is_deterministic_and_counts_are_the_answer_key():
    a, b = build_plan(events(20_000), Disturbance(seed=7)), build_plan(events(20_000), Disturbance(seed=7))
    assert a.counts == b.counts
    c = a.counts
    assert abs(c["duplicates"] / 20_000 - 0.03) < 0.01 and abs(c["late"] / 20_000 - 0.02) < 0.01
    assert c["messages_total"] == 20_000 + c["duplicates"] + sum(c["corrupt"].values())  # late hanya menggeser, tidak menambah


def test_every_unique_event_is_sent_at_least_once_and_corrupt_copies_are_extra():
    plan = build_plan(events(5_000), Disturbance(seed=1))
    good = set()
    bad = 0
    for it in plan.items:
        payload, _ = render(it, "2026-10-12T10:00:00+00:00")
        try:
            good.add(validate(payload)["event_id"])
        except Invalid:
            bad += 1
    assert len(good) == 5_000                       # tak ada event unik yang hilang
    assert bad == sum(plan.counts["corrupt"].values())
