from datetime import datetime

from pyspark.sql import Row

from fooddelivery.quality import ORDER_EVENT_RULES, all_rules_pass, quarantine
from fooddelivery.transforms import (
    attach_restaurant_as_of,
    build_order_lifecycle,
    city_daily_kpis,
    dedupe_events,
    demand_supply_signals,
    latest_per_key,
    standardize_order_events,
    zone_demand_supply,
)


def raw_event(event_id, order_id, status, ts, value=400.0, rider=None, **kw):
    base = dict(
        event_id=event_id,
        order_id=order_id,
        customer_id="C1",
        restaurant_id="REST1",
        city="hyderabad ",
        zone="Gachibowli",
        rider_id=rider,
        status=status,
        event_ts=ts,
        order_value=value,
        delivery_fee=25.0,
        payment_mode="upi",
        cancel_reason=None,
    )
    base.update(kw)
    return base


RAW_SCHEMA = (
    "event_id string, order_id string, customer_id string, restaurant_id string, city string, zone string, "
    "rider_id string, status string, event_ts string, order_value double, delivery_fee double, "
    "payment_mode string, cancel_reason string"
)


def events_df(spark, rows):
    cols = [c.split()[0] for c in RAW_SCHEMA.split(", ")]
    return standardize_order_events(spark.createDataFrame([tuple(r[c] for c in cols) for r in rows], RAW_SCHEMA))


def test_standardize_fixes_case_types_and_missing_tip(spark):
    df = events_df(spark, [raw_event("e1", "O1", " placed", "2026-10-01T07:00:00.000Z")])
    r = df.collect()[0]
    assert r.status == "PLACED" and r.status_rank == 1
    assert r.city == "Hyderabad" and r.payment_mode == "UPI"
    assert r.event_ts == datetime(2026, 10, 1, 7, 0)
    assert float(r.tip_amount) == 0.0  # column did not exist in the source


def test_quality_rules_and_quarantine_split_every_row_exactly_once(spark):
    rows = [
        raw_event("e1", "O1", "PLACED", "2026-10-01T07:00:00.000Z"),
        raw_event("e2", None, "PLACED", "2026-10-01T07:00:00.000Z"),
        raw_event("e3", "O3", "PLACED", "2026-10-01T07:00:00.000Z", value=-10.0),
        raw_event("e4", "O4", "REFUNDED_PENDING", "2026-10-01T07:00:00.000Z"),
        raw_event("e5", "O5", "PLACED", "2026-10-01T07:00:00.000Z", restaurant_id=None),
    ]
    df = events_df(spark, rows)
    good = df.filter(all_rules_pass(ORDER_EVENT_RULES))
    bad = quarantine(df, ORDER_EVENT_RULES)
    assert [r.event_id for r in good.collect()] == ["e1"]
    failed = {r.event_id: r._failed_rules for r in bad.collect()}
    assert failed == {
        "e2": ["valid_order_id"],
        "e3": ["valid_order_value"],
        "e4": ["valid_status"],
        "e5": ["valid_restaurant"],
    }


def test_dedupe_drops_client_retries(spark):
    rows = [raw_event("e1", "O1", "PLACED", "2026-10-01T07:00:00.000Z")] * 3
    out = dedupe_events(events_df(spark, rows), "event_id", "event_ts", "30 minutes")
    assert out.count() == 1


def test_dedupe_on_a_real_stream(spark, tmp_path):
    rows = [raw_event("e1", "O1", "PLACED", "2026-10-01T07:00:00.000Z")] * 3 + [
        raw_event("e2", "O1", "ACCEPTED", "2026-10-01T07:02:00.000Z", rider="R1")
    ]
    spark.createDataFrame(
        [tuple(r[c.split()[0]] for c in RAW_SCHEMA.split(", ")) for r in rows], RAW_SCHEMA
    ).write.json(str(tmp_path / "in"))
    stream = standardize_order_events(spark.readStream.schema(RAW_SCHEMA).json(str(tmp_path / "in")))
    q = (
        dedupe_events(stream, "event_id", "event_ts", "30 minutes")
        .writeStream.format("memory")
        .queryName("dedup_out")
        .option("checkpointLocation", str(tmp_path / "chk"))
        .trigger(availableNow=True)
        .start()
    )
    q.awaitTermination()
    assert sorted(r.event_id for r in spark.table("dedup_out").collect()) == ["e1", "e2"]


def test_order_lifecycle_durations_and_sla(spark):
    rows = [
        raw_event("a1", "FAST", "PLACED", "2026-10-01T07:00:00.000Z"),
        raw_event("a2", "FAST", "ACCEPTED", "2026-10-01T07:02:00.000Z", rider="R1"),
        raw_event("a3", "FAST", "PICKED_UP", "2026-10-01T07:14:00.000Z", rider="R1"),
        raw_event("a4", "FAST", "DELIVERED", "2026-10-01T07:30:00.000Z", rider="R1"),
        raw_event("b1", "SLOW", "PLACED", "2026-10-01T07:00:00.000Z"),
        raw_event("b2", "SLOW", "DELIVERED", "2026-10-01T07:55:00.000Z", rider="R2"),
        raw_event("c1", "CXL", "PLACED", "2026-10-01T07:00:00.000Z"),
        raw_event("c2", "CXL", "CANCELLED", "2026-10-01T07:03:00.000Z", cancel_reason="CUSTOMER_CANCELLED"),
    ]
    out = {r.order_id: r for r in build_order_lifecycle(events_df(spark, rows), sla_minutes=40).collect()}
    fast = out["FAST"]
    assert (fast.accept_minutes, fast.prep_minutes, fast.last_mile_minutes, fast.delivery_minutes) == (2, 12, 16, 30)
    assert fast.order_status == "DELIVERED" and fast.sla_breached is False and fast.rider_id == "R1"
    assert out["SLOW"].sla_breached is True
    assert out["CXL"].order_status == "CANCELLED" and out["CXL"].sla_breached is None
    assert out["CXL"].cancel_reason == "CUSTOMER_CANCELLED"


def test_point_in_time_join_uses_commission_valid_at_order_time(spark):
    events = events_df(
        spark,
        [
            raw_event("x1", "OLD", "PLACED", "2026-10-01T07:00:00.000Z"),
            raw_event("y1", "NEW", "PLACED", "2026-10-01T09:00:00.000Z"),
        ],
    )
    dim = spark.createDataFrame(
        [
            ("REST1", "Spice Kitchen", "Biryani", 18.0, datetime(2026, 10, 1, 0, 0), datetime(2026, 10, 1, 8, 0)),
            ("REST1", "Spice Kitchen", "Biryani", 22.0, datetime(2026, 10, 1, 8, 0), None),
        ],
        "restaurant_id string, restaurant_name string, cuisine string, commission_pct double, "
        "__START_AT timestamp, __END_AT timestamp",
    )
    out = {r.order_id: r for r in attach_restaurant_as_of(build_order_lifecycle(events), dim).collect()}
    assert out["OLD"].commission_pct == 18.0 and float(out["OLD"].platform_commission) == 72.0
    assert out["NEW"].commission_pct == 22.0 and float(out["NEW"].platform_commission) == 88.0


def test_city_kpis(spark):
    rows = [
        raw_event("a1", "O1", "PLACED", "2026-10-01T07:00:00.000Z", value=100.0),
        raw_event("a2", "O1", "DELIVERED", "2026-10-01T07:30:00.000Z", value=100.0, rider="R1"),
        raw_event("b1", "O2", "PLACED", "2026-10-01T07:00:00.000Z", value=300.0),
        raw_event("b2", "O2", "DELIVERED", "2026-10-01T07:50:00.000Z", value=300.0, rider="R2"),
        raw_event("c1", "O3", "PLACED", "2026-10-01T07:00:00.000Z"),
        raw_event("c2", "O3", "CANCELLED", "2026-10-01T07:02:00.000Z"),
        raw_event("d1", "O4", "PLACED", "2026-10-01T07:00:00.000Z"),
        raw_event("d2", "O4", "CANCELLED", "2026-10-01T07:02:00.000Z"),
    ]
    lifecycle = build_order_lifecycle(events_df(spark, rows)).withColumn("platform_commission", lifecycle_zero())
    k = city_daily_kpis(lifecycle).collect()[0]
    assert (k.total_orders, k.delivered_orders, k.cancelled_orders) == (4, 2, 2)
    assert float(k.gmv) == 400.0 and k.cancellation_pct == 50.0
    assert k.avg_delivery_minutes == 40.0 and k.sla_breach_pct == 50.0


def lifecycle_zero():
    from pyspark.sql import functions as F

    return F.lit(0)


def test_zone_demand_supply_counts_orders_and_idle_riders(spark):
    orders = events_df(spark, [raw_event(f"o{i}", f"O{i}", "PLACED", f"2026-10-01T07:0{i}:00.000Z") for i in range(4)])
    pings = spark.createDataFrame(
        [
            Row(
                rider_id="R1",
                city="Hyderabad",
                zone="Gachibowli",
                rider_status="IDLE",
                ping_ts=datetime(2026, 10, 1, 7, 1),
            ),
            Row(
                rider_id="R1",
                city="Hyderabad",
                zone="Gachibowli",
                rider_status="IDLE",
                ping_ts=datetime(2026, 10, 1, 7, 3),
            ),
            Row(
                rider_id="R2",
                city="Hyderabad",
                zone="Gachibowli",
                rider_status="ON_TRIP",
                ping_ts=datetime(2026, 10, 1, 7, 2),
            ),
        ]
    )
    r = zone_demand_supply(demand_supply_signals(orders, pings), "5 minutes", "30 minutes").collect()
    assert len(r) == 1
    w = r[0]
    assert (w.orders_placed, w.idle_riders, w.active_riders) == (4, 1, 2)
    assert w.demand_supply_ratio == 4.0 and w.surge_flag == "HIGH"


def test_latest_per_key_keeps_newest_row(spark):
    df = spark.createDataFrame(
        [
            Row(rider_id="R1", ping_ts=datetime(2026, 10, 1, 7, 1), lat=1.0),
            Row(rider_id="R1", ping_ts=datetime(2026, 10, 1, 7, 5), lat=2.0),
            Row(rider_id="R2", ping_ts=datetime(2026, 10, 1, 7, 2), lat=3.0),
        ]
    )
    out = {r.rider_id: r.lat for r in latest_per_key(df, "rider_id", "ping_ts").collect()}
    assert out == {"R1": 2.0, "R2": 3.0}
