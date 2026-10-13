# Lakeflow Declarative Pipeline - GOLD layer
# ---------------------------------------------------------------------------
# Business-ready materialized views. On serverless, materialized views are
# refreshed incrementally where possible (Enzyme), so only changed data is
# recomputed on each pipeline update.
# ---------------------------------------------------------------------------
import sys

sys.path.append(spark.conf.get("bundle.sourcePath", "."))

from pyspark import pipelines as dp

from fooddelivery.config import DELIVERY_SLA_MINUTES
from fooddelivery.transforms import (
    attach_restaurant_as_of,
    build_order_lifecycle,
    city_daily_kpis,
    restaurant_performance,
)

CATALOG = spark.conf.get("fd.catalog")


@dp.materialized_view(
    name=f"{CATALOG}.gold.fact_order_lifecycle",
    comment=(
        "One row per order with stage timestamps, durations, SLA flag and the restaurant "
        f"commission valid at order time. SLA = {DELIVERY_SLA_MINUTES} minutes."
    ),
    table_properties={"quality": "gold"},
    cluster_by=["order_date", "city"],
)
@dp.expect("delivered_after_placed", "delivered_ts IS NULL OR delivered_ts >= placed_ts")
@dp.expect("has_placed_event", "placed_ts IS NOT NULL")
def fact_order_lifecycle():
    events = spark.read.table(f"{CATALOG}.silver.order_events")
    dim = spark.read.table(f"{CATALOG}.silver.dim_restaurant")
    return attach_restaurant_as_of(build_order_lifecycle(events), dim)


@dp.materialized_view(
    name=f"{CATALOG}.gold.kpi_city_daily",
    comment="Daily operational KPIs per city: orders, GMV, cancellations, delivery time, SLA breach %.",
    table_properties={"quality": "gold"},
)
def kpi_city_daily():
    return city_daily_kpis(spark.read.table(f"{CATALOG}.gold.fact_order_lifecycle"))


@dp.materialized_view(
    name=f"{CATALOG}.gold.restaurant_performance",
    comment="Restaurant scorecard: volume, GMV, p50/p90 food preparation time, cancellation %.",
    table_properties={"quality": "gold"},
)
def restaurant_performance_mv():
    return restaurant_performance(spark.read.table(f"{CATALOG}.gold.fact_order_lifecycle"))
