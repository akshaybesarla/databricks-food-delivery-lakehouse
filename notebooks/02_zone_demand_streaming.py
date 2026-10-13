# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Zone demand vs. rider supply (Structured Streaming)
# MAGIC Answers the operations question *"which zones need surge pricing or more riders right now?"*
# MAGIC
# MAGIC * Reads the Silver order events and rider pings as **streams** (Delta as a streaming source)
# MAGIC * Combines them into one signal stream and runs a **5-minute tumbling-window aggregation**
# MAGIC   with a **30-minute event-time watermark** (late events are still counted, state stays bounded)
# MAGIC * Upserts every micro-batch into Gold with **`foreachBatch` + Delta `MERGE`**, so window rows
# MAGIC   are updated in place as late data arrives and re-runs are idempotent
# MAGIC * Also maintains `gold.rider_latest_location` (one row per rider) the same way
# MAGIC
# MAGIC Runs with `trigger(availableNow=True)`: processes everything new since the last checkpoint and
# MAGIC stops - the cost profile of a batch job with exactly-once streaming semantics.
# MAGIC Switch to `processingTime="1 minute"` on classic compute for continuous operation.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
catalog = dbutils.widgets.get("catalog")
checkpoint_root = f"/Volumes/{catalog}/landing/raw/_checkpoints"

# COMMAND ----------

import os
import sys

src = os.path.abspath(os.path.join(os.getcwd(), "..", "src"))
if src not in sys.path:
    sys.path.append(src)

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from fooddelivery.config import DEMAND_WINDOW, EVENT_WATERMARK
from fooddelivery.transforms import demand_supply_signals, latest_per_key, zone_demand_supply

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.zone_demand_supply_5m (
  window_start TIMESTAMP, window_end TIMESTAMP, city STRING, zone STRING,
  orders_placed BIGINT, idle_riders BIGINT, active_riders BIGINT,
  demand_supply_ratio DOUBLE, surge_flag STRING, updated_at TIMESTAMP
) CLUSTER BY (city, window_start)
COMMENT 'Orders vs idle riders per zone in 5-minute windows. surge_flag drives surge pricing / rider nudges.'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.rider_latest_location (
  rider_id STRING, city STRING, zone STRING, lat DOUBLE, lon DOUBLE,
  rider_status STRING, battery_pct BIGINT, ping_ts TIMESTAMP, updated_at TIMESTAMP
) COMMENT 'Last known position and status of every rider.'
""")

# COMMAND ----------


def upsert_zone_windows(batch_df, batch_id):
    target = DeltaTable.forName(batch_df.sparkSession, f"{catalog}.gold.zone_demand_supply_5m")
    (
        target.alias("t")
        .merge(
            batch_df.withColumn("updated_at", F.current_timestamp()).alias("s"),
            "t.window_start = s.window_start AND t.city = s.city AND t.zone = s.zone",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


orders = spark.readStream.table(f"{catalog}.silver.order_events")
pings = spark.readStream.table(f"{catalog}.silver.rider_pings")

zone_query = (
    zone_demand_supply(demand_supply_signals(orders, pings), DEMAND_WINDOW, EVENT_WATERMARK)
    .writeStream.outputMode("update")
    .foreachBatch(upsert_zone_windows)
    .option("checkpointLocation", f"{checkpoint_root}/zone_demand_supply_5m")
    .queryName("zone_demand_supply_5m")
    .trigger(availableNow=True)
    .start()
)

# COMMAND ----------


def upsert_latest_location(batch_df, batch_id):
    latest = latest_per_key(batch_df, "rider_id", "ping_ts").select(
        "rider_id",
        "city",
        "zone",
        "lat",
        "lon",
        "rider_status",
        "battery_pct",
        "ping_ts",
        F.current_timestamp().alias("updated_at"),
    )
    target = DeltaTable.forName(batch_df.sparkSession, f"{catalog}.gold.rider_latest_location")
    (
        target.alias("t")
        .merge(latest.alias("s"), "t.rider_id = s.rider_id")
        .whenMatchedUpdateAll(condition="s.ping_ts > t.ping_ts")  # never go back in time
        .whenNotMatchedInsertAll()
        .execute()
    )


location_query = (
    pings.writeStream.foreachBatch(upsert_latest_location)
    .option("checkpointLocation", f"{checkpoint_root}/rider_latest_location")
    .queryName("rider_latest_location")
    .trigger(availableNow=True)
    .start()
)

# COMMAND ----------

zone_query.awaitTermination()
location_query.awaitTermination()

display(
    spark.table(f"{catalog}.gold.zone_demand_supply_5m")
    .filter("surge_flag != 'NORMAL'")
    .orderBy(F.desc("window_start"), F.desc("demand_supply_ratio"))
    .limit(20)
)
