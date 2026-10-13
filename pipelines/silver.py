# Lakeflow Declarative Pipeline - SILVER layer
# ---------------------------------------------------------------------------
# Cleaned, typed, de-duplicated and validated data.
#   * expectations drop bad rows; a quarantine table keeps them for triage
#   * AUTO CDC keeps the *current* state of every order (SCD Type 1)
#   * AUTO CDC keeps the full *history* of restaurants (SCD Type 2)
# ---------------------------------------------------------------------------
import sys

sys.path.append(spark.conf.get("bundle.sourcePath", "."))

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from fooddelivery.config import EVENT_WATERMARK
from fooddelivery.quality import ORDER_EVENT_RULES, RIDER_PING_RULES, quarantine
from fooddelivery.transforms import (
    dedupe_events,
    standardize_order_events,
    standardize_restaurants,
    standardize_rider_pings,
)

CATALOG = spark.conf.get("fd.catalog")


# ----------------------------------------------------------------- orders ---
@dp.temporary_view(comment="Typed + standardised order events, before validation.")
def order_events_typed():
    return standardize_order_events(spark.readStream.table(f"{CATALOG}.bronze.order_events_raw"))


@dp.table(
    name=f"{CATALOG}.silver.order_events",
    comment="Validated, de-duplicated order status events.",
    table_properties={"quality": "silver"},
    cluster_by=["order_id"],
)
@dp.expect_all_or_drop(ORDER_EVENT_RULES)
def order_events():
    return dedupe_events(
        spark.readStream.table("order_events_typed"), key="event_id", ts_col="event_ts", watermark=EVENT_WATERMARK
    ).drop("_rescued_data")


@dp.table(
    name=f"{CATALOG}.silver.order_events_quarantine",
    comment="Order events that failed data quality rules, with the names of the failed rules.",
    table_properties={"quality": "quarantine"},
)
def order_events_quarantine():
    return quarantine(spark.readStream.table("order_events_typed"), ORDER_EVENT_RULES)


# Current state of every order (one row per order_id), maintained by AUTO CDC.
# sequence_by uses (event_ts, status_rank) so that late or out-of-order events
# never move an order backwards, and ties are broken by lifecycle stage.
dp.create_streaming_table(
    name=f"{CATALOG}.silver.orders_current",
    comment="Latest status of each order (SCD Type 1 via AUTO CDC).",
    table_properties={"quality": "silver"},
)
dp.create_auto_cdc_flow(
    target=f"{CATALOG}.silver.orders_current",
    source=f"{CATALOG}.silver.order_events",
    keys=["order_id"],
    sequence_by=F.struct("event_ts", "status_rank"),
    stored_as_scd_type=1,
    except_column_list=["_source_file", "_file_modified_at", "_ingested_at"],
)


# ------------------------------------------------------------ rider pings ---
@dp.table(
    name=f"{CATALOG}.silver.rider_pings",
    comment="Validated rider GPS pings (GPS glitches and malformed rows dropped).",
    table_properties={"quality": "silver"},
    cluster_by=["rider_id"],
)
@dp.expect_all_or_drop(RIDER_PING_RULES)
@dp.expect("battery_reported", "battery_pct IS NOT NULL")  # warn only, keep the row
def rider_pings():
    return dedupe_events(
        standardize_rider_pings(spark.readStream.table(f"{CATALOG}.bronze.rider_pings_raw")).withColumn(
            "ping_id", F.concat_ws("|", "rider_id", "ping_ts")
        ),
        key="ping_id",
        ts_col="ping_ts",
        watermark=EVENT_WATERMARK,
    )


# ------------------------------------------------------------ restaurants ---
@dp.temporary_view
def restaurants_typed():
    return standardize_restaurants(spark.readStream.table(f"{CATALOG}.bronze.restaurants_raw"))


dp.create_streaming_table(
    name=f"{CATALOG}.silver.dim_restaurant",
    comment="Restaurant dimension with full history (SCD Type 2 via AUTO CDC).",
    table_properties={"quality": "silver"},
    expect_all_or_drop={"valid_restaurant_id": "restaurant_id IS NOT NULL"},
)
dp.create_auto_cdc_flow(
    target=f"{CATALOG}.silver.dim_restaurant",
    source="restaurants_typed",
    keys=["restaurant_id"],
    sequence_by="updated_at",
    stored_as_scd_type=2,
    # A new version is only opened when a business attribute changes.
    track_history_column_list=["commission_pct", "is_active", "cuisine", "zone"],
    except_column_list=["_source_file", "_file_modified_at", "_ingested_at", "_rescued_data"],
)
