"""Data quality rules.

The same rule dictionaries feed two things:
  1. Lakeflow expectations (`@dp.expect_all_or_drop(ORDER_EVENT_RULES)`) on the
     clean Silver table, so bad rows never reach downstream consumers.
  2. A quarantine table that keeps the bad rows *and* the names of the rules
     they broke, so they can be investigated and replayed instead of silently
     disappearing.

Every rule is written to be null-safe (it evaluates to TRUE or FALSE, never
NULL) so both paths agree on exactly which rows are bad.
"""

from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from fooddelivery.config import ORDER_STATUSES, RIDER_STATUSES

_statuses = ", ".join(f"'{s}'" for s in ORDER_STATUSES)
_rider_statuses = ", ".join(f"'{s}'" for s in RIDER_STATUSES)

ORDER_EVENT_RULES = {
    "valid_event_id": "event_id IS NOT NULL",
    "valid_order_id": "order_id IS NOT NULL",
    "valid_event_ts": "event_ts IS NOT NULL",
    "valid_status": f"coalesce(status IN ({_statuses}), false)",
    "valid_order_value": "coalesce(order_value >= 0 AND order_value < 50000, false)",
    "valid_restaurant": "restaurant_id IS NOT NULL",
}

RIDER_PING_RULES = {
    "valid_rider_id": "rider_id IS NOT NULL",
    "valid_ping_ts": "ping_ts IS NOT NULL",
    "valid_rider_status": f"coalesce(rider_status IN ({_rider_statuses}), false)",
    # Rough bounding box for India - catches swapped lat/lon and 0,0 GPS glitches.
    "valid_coordinates": "coalesce(lat BETWEEN 6 AND 37 AND lon BETWEEN 68 AND 98, false)",
}


def all_rules_pass(rules: dict) -> Column:
    """Single boolean column: TRUE only if every rule passes."""
    return reduce(lambda a, b: a & b, [F.expr(f"({r})") for r in rules.values()])


def with_failed_rules(df: DataFrame, rules: dict) -> DataFrame:
    """Add `_failed_rules` (array<string>) listing every rule a row breaks."""
    checks = [F.when(~F.expr(f"({expr})"), F.lit(name)) for name, expr in rules.items()]
    return df.withColumn("_failed_rules", F.filter(F.array(*checks), lambda x: x.isNotNull()))


def quarantine(df: DataFrame, rules: dict) -> DataFrame:
    """Rows that fail at least one rule, tagged with the rules they failed."""
    return with_failed_rules(df.filter(~all_rules_pass(rules)), rules)
