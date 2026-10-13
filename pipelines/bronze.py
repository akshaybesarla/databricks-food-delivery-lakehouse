# Lakeflow Declarative Pipeline - BRONZE layer
# ---------------------------------------------------------------------------
# Raw, append-only copies of the three landing feeds, ingested incrementally
# with Auto Loader. Nothing is filtered here: Bronze is the replayable source
# of truth, so every record is kept together with where and when it came from.
# ---------------------------------------------------------------------------
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("fd.catalog")
LANDING = spark.conf.get("fd.landing_path")  # e.g. /Volumes/<catalog>/landing/raw


def autoload(feed: str, schema_hints: str, evolution: str = "addNewColumns"):
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints", schema_hints)
        .option("cloudFiles.schemaEvolutionMode", evolution)
        .load(f"{LANDING}/{feed}")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_file_modified_at", F.col("_metadata.file_modification_time"))
        .withColumn("_ingested_at", F.current_timestamp())
    )


@dp.table(
    name=f"{CATALOG}.bronze.order_events_raw",
    comment="Raw order status events (CDC feed from the order service). Append-only.",
    table_properties={"quality": "bronze", "delta.enableChangeDataFeed": "true"},
    cluster_by=["event_date"],
)
def order_events_raw():
    # addNewColumns: when app v5.2 starts sending `tip_amount`, the schema
    # evolves and the pipeline picks the new column up on its automatic restart.
    return autoload(
        "order_events",
        "event_ts STRING, order_value DOUBLE, delivery_fee DOUBLE, order_id STRING, rider_id STRING",
    ).withColumn("event_date", F.to_date(F.substring("event_ts", 1, 10)))


@dp.table(
    name=f"{CATALOG}.bronze.rider_pings_raw",
    comment="Raw GPS + status heartbeats from delivery partners. Append-only.",
    table_properties={"quality": "bronze"},
    cluster_by=["ping_date"],
)
def rider_pings_raw():
    # rescue: unexpected fields from new app builds land in _rescued_data
    # instead of breaking a high-volume feed.
    return autoload("rider_pings", "lat DOUBLE, lon DOUBLE, ping_ts STRING", evolution="rescue").withColumn(
        "ping_date", F.to_date(F.substring("ping_ts", 1, 10))
    )


@dp.table(
    name=f"{CATALOG}.bronze.restaurants_raw",
    comment="Restaurant master data change records (full snapshot first, then changes).",
    table_properties={"quality": "bronze"},
)
def restaurants_raw():
    return autoload("restaurants", "commission_pct DOUBLE, updated_at STRING, is_active BOOLEAN")
