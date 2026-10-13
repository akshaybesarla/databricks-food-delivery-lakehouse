# Explaining this project in an interview

## The 60-second pitch

"I built a streaming lakehouse on Databricks for a food delivery platform. Three feeds - order status events, rider GPS pings and restaurant master data - land as JSON in a Unity Catalog Volume. Auto Loader ingests them into Bronze. A Lakeflow Declarative Pipeline in PySpark cleans and validates them in Silver: duplicates from client retries are removed with a watermark, bad records are dropped by expectations but kept in a quarantine table, AUTO CDC maintains the current state of every order, and restaurants are kept as an SCD Type 2 dimension. Gold has an order-lifecycle fact table and KPI views: delivery time, SLA breach %, kitchen prep time, cancellations and commission revenue using the commission valid at order time. A separate Structured Streaming job computes demand versus idle riders per zone in 5-minute windows and upserts it with foreachBatch MERGE, which is what you would use to trigger surge pricing. A Lakeflow Job orchestrates it hourly, it is deployed with Asset Bundles, and GitHub Actions runs the PySpark unit tests."

## Walk the data through the layers (draw this)

1. **Landing** - `/Volumes/<catalog>/landing/raw/{order_events,rider_pings,restaurants}/*.json`
2. **Bronze** - Auto Loader streaming tables, schema inferred + hints, `_source_file` and `_ingested_at` added, nothing filtered.
3. **Silver** - types, standardised values, dedup, expectations, quarantine, AUTO CDC (SCD1 orders, SCD2 restaurants).
4. **Gold** - `fact_order_lifecycle` (accumulating snapshot), `kpi_city_daily`, `restaurant_performance`, `zone_demand_supply_5m`, `rider_latest_location`, `dq_expectation_results`.

## Questions you should be ready for

**Why Auto Loader and not `spark.read.json` on a schedule?**
It tracks which files were already processed (checkpoint), so each run only reads new files, exactly once. It handles schema inference and evolution, and scales to millions of files with file notification mode.

**What happens when a new column appears?**
Order events use `schemaEvolutionMode = addNewColumns`: the stream stops, the schema is updated, and the pipeline restarts automatically with the new column (`tip_amount`). Older rows get `tip_amount = 0` in Silver. Rider pings use `rescue`, so unexpected fields go to `_rescued_data` instead of changing a high-volume table.

**How do you remove duplicates in a stream without unbounded state?**
`withWatermark("event_ts", "30 minutes").dropDuplicatesWithinWatermark(["event_id"])`. Spark only remembers event IDs for 30 minutes of event time, so state is bounded. Retries arrive within seconds, so 30 minutes is safe.

**What is a watermark?**
The point in event time after which Spark assumes no more data will arrive for older windows: max event time seen minus the threshold. It decides when window state can be dropped and how late an event can be and still be counted.

**Expectations: what are the three actions?**
`expect` (warn, keep the row, record metrics), `expect_or_drop` (drop the row), `expect_or_fail` (stop the update). I drop in Silver but also write failing rows to a quarantine table with the failed rule names, so nothing is lost silently. Rules are null-safe so both sides split rows identically.

**Why AUTO CDC instead of MERGE?**
It handles out-of-order and late events using `sequence_by`, deduplicates per key, and does SCD2 bookkeeping (`__START_AT`, `__END_AT`) for you. With a hand-written MERGE you would have to code all of that and handle ordering yourself.

**Why `sequence_by = struct(event_ts, status_rank)`?**
Two status events can have the same timestamp. The struct compares `event_ts` first and then the lifecycle rank, so DELIVERED always wins over PICKED_UP and an order can never move backwards.

**What is `track_history_column_list`?**
Only changes in those columns (commission, active flag, cuisine, zone) open a new SCD2 version. A change to, say, the name would update in place.

**How does the point-in-time join work?**
Join orders to `dim_restaurant` on `restaurant_id` and `placed_ts >= __START_AT AND (placed_ts < __END_AT OR __END_AT IS NULL)`. Commission revenue then uses the commission that was valid when the order was placed, not today's value.

**Streaming table vs. materialized view?**
Streaming tables process each input row once (append-only sources, low latency). Materialized views are recomputed from their query, incrementally where possible on serverless - right for aggregates and joins that must reflect updates, like Gold KPIs.

**Why `foreachBatch` + MERGE for the zone windows?**
In `update` mode a window can be emitted several times as late data arrives. MERGE on `(window_start, city, zone)` updates the existing row instead of duplicating it, and makes reprocessing a batch idempotent. For latest rider location I first keep the newest ping per rider in the micro-batch (MERGE fails if several source rows match one target row) and only update when the new ping is newer.

**Why union the two streams instead of a stream-stream join?**
A stream-stream join needs watermarks and time constraints on both sides and keeps both sides in state. Both signals only need counting per zone and window, so a union plus one aggregation is simpler and uses less state.

**What does `trigger(availableNow=True)` do?**
Processes everything available since the last checkpoint, possibly in several micro-batches, then stops. Exactly-once streaming semantics with batch cost - ideal for an hourly job. Serverless supports it; continuous would use `processingTime`.

**How is it deployed?**
Asset Bundles: `databricks.yml` plus YAML for the pipeline and job. `databricks bundle deploy -t dev` uploads code and creates/updates resources. GitHub Actions runs ruff and pytest on every PR, and deploys on merge to main.

**How do you test PySpark code?**
All logic is in `src/fooddelivery` as pure DataFrame functions. pytest builds small DataFrames with a local SparkSession and asserts results - including SLA calculation, quarantine rules, the SCD2 as-of join and a real streaming dedup test.

**How would you monitor it in production?**
The pipeline event log (expectation metrics -> `gold.dq_expectation_results`, plus SQL alerts), job failure notifications and retries, and freshness checks on Gold.

**How would you scale it?**
Kafka/Event Hubs as source, file notification mode for Auto Loader, liquid clustering keys based on query patterns, continuous pipeline mode for the order stream, and Photon/serverless autoscaling.

**What was hard?**
Getting late and out-of-order events right: an order's DELIVERED event can arrive before its PICKED_UP event, or two events share a timestamp. The `sequence_by` tie-breaker and watermark-based dedup solved it, and the lifecycle fact uses `min()` per status so arrival order never matters.

## Numbers from a 12-hour simulated run

~27K order events, ~6.8K orders, 4 cities, 20 zones, 160 restaurants, 500 riders, ~180K GPS pings, ~3-4% bad or duplicate records, 8-9% cancellations, avg delivery ~31 min, p90 ~41 min, ~11% SLA breaches (40-minute SLA). Re-run and quote your own numbers - they will be in your tables.
