"""Pure PySpark transformations for the Silver and Gold layers.

These functions take DataFrames and return DataFrames. They never read or
write tables, which keeps them easy to unit test locally and lets the same
code run in batch (materialized views) or streaming (streaming tables).
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from fooddelivery.config import DELIVERY_SLA_MINUTES, STATUS_RANK

# --------------------------------------------------------------------------- #
# Silver: standardisation
# --------------------------------------------------------------------------- #


def standardize_order_events(df: DataFrame) -> DataFrame:
    """Clean types and values coming from the raw JSON order events.

    - trims / upper-cases categorical fields (the mobile apps send mixed case)
    - parses ISO-8601 timestamps
    - casts money columns to decimal so sums are exact
    - adds `status_rank`, used to order events that share a timestamp
    - `tip_amount` only appears in newer app versions (schema drift), so it is
      created as 0 when missing and defaulted to 0 when null
    """
    if "tip_amount" not in df.columns:
        df = df.withColumn("tip_amount", F.lit(None).cast("double"))

    rank_map = F.create_map(*[x for k, v in STATUS_RANK.items() for x in (F.lit(k), F.lit(v))])
    status = F.upper(F.trim(F.col("status")))

    return (
        df.withColumn("status", status)
        .withColumn("event_ts", F.to_timestamp("event_ts"))
        .withColumn("city", F.initcap(F.trim("city")))
        .withColumn("payment_mode", F.upper(F.trim("payment_mode")))
        .withColumn("order_value", F.col("order_value").cast("decimal(10,2)"))
        .withColumn(
            "delivery_fee", F.coalesce(F.col("delivery_fee").cast("decimal(10,2)"), F.lit(0).cast("decimal(10,2)"))
        )
        .withColumn("tip_amount", F.coalesce(F.col("tip_amount").cast("decimal(10,2)"), F.lit(0).cast("decimal(10,2)")))
        .withColumn("status_rank", rank_map[status])
    )


def standardize_rider_pings(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("ping_ts", F.to_timestamp("ping_ts"))
        .withColumn("rider_status", F.upper(F.trim("rider_status")))
        .withColumn("lat", F.col("lat").cast("double"))
        .withColumn("lon", F.col("lon").cast("double"))
    )


def standardize_restaurants(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("updated_at", F.to_timestamp("updated_at"))
        .withColumn("city", F.initcap(F.trim("city")))
        .withColumn("commission_pct", F.col("commission_pct").cast("decimal(5,2)"))
        .withColumn("is_active", F.col("is_active").cast("boolean"))
    )


def dedupe_events(df: DataFrame, key: str, ts_col: str, watermark: str) -> DataFrame:
    """Streaming-safe de-duplication.

    Mobile clients retry on flaky networks, so the same event can land more
    than once. `dropDuplicatesWithinWatermark` keeps state only for the
    watermark period, so state does not grow forever. For batch inputs
    (backfills, unit tests) a plain dropDuplicates gives the same result.
    """
    if not df.isStreaming:
        return df.dropDuplicates([key])
    return df.withWatermark(ts_col, watermark).dropDuplicatesWithinWatermark([key])


# --------------------------------------------------------------------------- #
# Gold: order lifecycle fact
# --------------------------------------------------------------------------- #


def _status_ts(status: str):
    return F.min(F.when(F.col("status") == status, F.col("event_ts")))


def _minutes_between(start, end):
    return F.round((F.unix_timestamp(end) - F.unix_timestamp(start)) / 60.0, 2)


def build_order_lifecycle(events: DataFrame, sla_minutes: int = DELIVERY_SLA_MINUTES) -> DataFrame:
    """One row per order with a timestamp for every stage and the durations
    between them (the classic 'accumulating snapshot' fact table)."""
    per_order = events.groupBy("order_id").agg(
        F.max("customer_id").alias("customer_id"),
        F.max("restaurant_id").alias("restaurant_id"),
        F.max("city").alias("city"),
        F.max("zone").alias("zone"),
        F.max("rider_id").alias("rider_id"),
        F.max("payment_mode").alias("payment_mode"),
        F.max("order_value").alias("order_value"),
        F.max("delivery_fee").alias("delivery_fee"),
        F.max("tip_amount").alias("tip_amount"),
        _status_ts("PLACED").alias("placed_ts"),
        _status_ts("ACCEPTED").alias("accepted_ts"),
        _status_ts("PICKED_UP").alias("picked_up_ts"),
        _status_ts("DELIVERED").alias("delivered_ts"),
        _status_ts("CANCELLED").alias("cancelled_ts"),
        F.max_by("cancel_reason", "event_ts").alias("cancel_reason"),
    )

    final_status = (
        F.when(F.col("cancelled_ts").isNotNull(), "CANCELLED")
        .when(F.col("delivered_ts").isNotNull(), "DELIVERED")
        .when(F.col("picked_up_ts").isNotNull(), "OUT_FOR_DELIVERY")
        .when(F.col("accepted_ts").isNotNull(), "PREPARING")
        .otherwise("PLACED")
    )

    return (
        per_order.withColumn("order_status", final_status)
        .withColumn("order_date", F.to_date("placed_ts"))
        .withColumn("order_hour", F.hour("placed_ts"))
        .withColumn("accept_minutes", _minutes_between("placed_ts", "accepted_ts"))
        .withColumn("prep_minutes", _minutes_between("accepted_ts", "picked_up_ts"))
        .withColumn("last_mile_minutes", _minutes_between("picked_up_ts", "delivered_ts"))
        .withColumn("delivery_minutes", _minutes_between("placed_ts", "delivered_ts"))
        .withColumn(
            "sla_breached",
            F.when(F.col("delivered_ts").isNull(), F.lit(None).cast("boolean")).otherwise(
                F.col("delivery_minutes") > sla_minutes
            ),
        )
    )


def attach_restaurant_as_of(lifecycle: DataFrame, dim_restaurant_scd2: DataFrame) -> DataFrame:
    """Point-in-time join to the SCD Type 2 restaurant dimension.

    The commission a restaurant paid *when the order was placed* is what
    matters for revenue, not today's commission. AUTO CDC SCD2 tables expose
    `__START_AT` / `__END_AT` (NULL = current version).
    """
    d = dim_restaurant_scd2.select(
        F.col("restaurant_id").alias("_rid"),
        "restaurant_name",
        "cuisine",
        "commission_pct",
        F.col("__START_AT").alias("_valid_from"),
        F.col("__END_AT").alias("_valid_to"),
    )
    cond = (
        (F.col("restaurant_id") == F.col("_rid"))
        & (F.col("placed_ts") >= F.col("_valid_from"))
        & (F.col("_valid_to").isNull() | (F.col("placed_ts") < F.col("_valid_to")))
    )
    return (
        lifecycle.join(d, cond, "left")
        .drop("_rid", "_valid_from", "_valid_to")
        .withColumn(
            "platform_commission",
            F.round(F.col("order_value") * F.col("commission_pct") / 100, 2),
        )
    )


# --------------------------------------------------------------------------- #
# Gold: KPI aggregates
# --------------------------------------------------------------------------- #


def city_daily_kpis(lifecycle: DataFrame) -> DataFrame:
    delivered = F.col("order_status") == "DELIVERED"
    cancelled = F.col("order_status") == "CANCELLED"
    return (
        lifecycle.groupBy("order_date", "city")
        .agg(
            F.count("*").alias("total_orders"),
            F.sum(delivered.cast("int")).alias("delivered_orders"),
            F.sum(cancelled.cast("int")).alias("cancelled_orders"),
            F.sum(F.when(delivered, F.col("order_value"))).alias("gmv"),
            F.sum(F.when(delivered, F.col("platform_commission"))).alias("commission_revenue"),
            F.round(F.avg("delivery_minutes"), 2).alias("avg_delivery_minutes"),
            F.percentile_approx("delivery_minutes", 0.9).alias("p90_delivery_minutes"),
            F.round(F.avg(F.col("sla_breached").cast("double")) * 100, 2).alias("sla_breach_pct"),
        )
        .withColumn("cancellation_pct", F.round(F.col("cancelled_orders") / F.col("total_orders") * 100, 2))
    )


def restaurant_performance(lifecycle: DataFrame) -> DataFrame:
    return lifecycle.groupBy("restaurant_id", "restaurant_name", "city", "cuisine").agg(
        F.count("*").alias("orders"),
        F.sum(F.when(F.col("order_status") == "DELIVERED", F.col("order_value"))).alias("gmv"),
        F.percentile_approx("prep_minutes", 0.5).alias("p50_prep_minutes"),
        F.percentile_approx("prep_minutes", 0.9).alias("p90_prep_minutes"),
        F.round(F.avg((F.col("order_status") == "CANCELLED").cast("double")) * 100, 2).alias("cancellation_pct"),
    )


# --------------------------------------------------------------------------- #
# Near-real-time zone demand vs. rider supply
# --------------------------------------------------------------------------- #


def demand_supply_signals(order_events: DataFrame, rider_pings: DataFrame) -> DataFrame:
    """Put both streams into one shape so they can share a single windowed
    aggregation (one stateful operator instead of a stream-stream join)."""
    demand = order_events.filter(F.col("status") == "PLACED").select(
        "city",
        "zone",
        F.col("event_ts").alias("ts"),
        F.lit("order").alias("kind"),
        F.lit(None).cast("string").alias("rider_id"),
        F.lit(None).cast("string").alias("rider_status"),
    )
    supply = rider_pings.select(
        "city",
        "zone",
        F.col("ping_ts").alias("ts"),
        F.lit("ping").alias("kind"),
        "rider_id",
        "rider_status",
    )
    return demand.unionByName(supply)


def zone_demand_supply(signals: DataFrame, window: str, watermark: str) -> DataFrame:
    is_order = F.col("kind") == "order"
    idle_rider = F.when((F.col("kind") == "ping") & (F.col("rider_status") == "IDLE"), F.col("rider_id"))
    any_rider = F.when(F.col("kind") == "ping", F.col("rider_id"))
    return (
        signals.withWatermark("ts", watermark)
        .groupBy(F.window("ts", window).alias("w"), "city", "zone")
        .agg(
            F.sum(is_order.cast("int")).alias("orders_placed"),
            F.approx_count_distinct(idle_rider).alias("idle_riders"),
            F.approx_count_distinct(any_rider).alias("active_riders"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "city",
            "zone",
            "orders_placed",
            "idle_riders",
            "active_riders",
            F.round(F.col("orders_placed") / F.greatest(F.col("idle_riders"), F.lit(1)), 2).alias(
                "demand_supply_ratio"
            ),
        )
        .withColumn(
            "surge_flag",
            F.when(F.col("demand_supply_ratio") >= 2.0, "HIGH")
            .when(F.col("demand_supply_ratio") >= 1.2, "MEDIUM")
            .otherwise("NORMAL"),
        )
    )


def latest_per_key(df: DataFrame, key: str, ts_col: str) -> DataFrame:
    """Keep the newest row per key inside one micro-batch (used before MERGE,
    because MERGE fails if several source rows match the same target row)."""
    w = Window.partitionBy(key).orderBy(F.col(ts_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
