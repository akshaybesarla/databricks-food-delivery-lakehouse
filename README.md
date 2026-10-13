# Food Delivery Operations Lakehouse on Databricks

An end-to-end, streaming-first lakehouse for a quick-commerce **food delivery platform** (think Swiggy / Zomato / Uber Eats), built on **Databricks** with **PySpark**, **Auto Loader**, **Lakeflow Declarative Pipelines**, **Structured Streaming**, **Delta Lake**, **Unity Catalog** and **Lakeflow Jobs**, deployed as code with **Databricks Asset Bundles** and tested in **GitHub Actions**.

It answers the questions an operations team at a delivery company asks every day:

* Are we keeping the delivery promise? (delivery time, p90, **SLA breach %** per city)
* Where is the time lost: restaurant acceptance, **kitchen prep**, or **last mile**?
* Why are orders cancelled, and does a **rider shortage** cause them?
* Which **zones need surge pricing or more riders right now**? (5-minute demand vs. supply)
* How much commission did we earn, using the commission **valid at the time of each order** (SCD Type 2)?

---

## Architecture

```mermaid
%%{init: {"theme": "base", "themeVariables": {"fontFamily": "Segoe UI, Helvetica, Arial", "fontSize": "15px", "lineColor": "#5B6B7F"}}}%%
flowchart LR
    subgraph SRC["📱 SOURCE SYSTEMS"]
        direction TB
        OS["🛵 Order Service<br/><i>status events · CDC</i>"]
        RA["📍 Rider App<br/><i>GPS pings</i>"]
        RP["🍽️ Restaurant Portal<br/><i>master data</i>"]
    end

    V[("📂 Landing Volume<br/><i>landing.raw · JSON</i>")]

    subgraph BRONZE["🥉 BRONZE · raw, append-only"]
        direction TB
        B1["order_events_raw"]
        B2["rider_pings_raw"]
        B3["restaurants_raw"]
    end

    subgraph SILVER["🥈 SILVER · clean & validated"]
        direction TB
        S1["order_events<br/><i>expectations + dedup</i>"]
        SQ["⚠️ order_events_quarantine"]
        S2["orders_current<br/><i>AUTO CDC · SCD1</i>"]
        S3["rider_pings"]
        S4["dim_restaurant<br/><i>AUTO CDC · SCD2</i>"]
    end

    subgraph GOLD["🥇 GOLD · business-ready"]
        direction TB
        G1["fact_order_lifecycle"]
        G2["kpi_city_daily"]
        G3["restaurant_performance"]
        G4["zone_demand_supply_5m"]
        G5["rider_latest_location"]
        DQ["dq_expectation_results"]
    end

    ST{{"⚡ Structured Streaming<br/><i>5-min windows · watermark<br/>foreachBatch MERGE</i>"}}
    BI["📊 Databricks SQL<br/>AI/BI Dashboard"]

    OS & RA & RP --> V
    V == "Auto Loader" ==> B1 & B2 & B3
    B1 --> S1
    B1 -. "failed rules" .-> SQ
    S1 --> S2
    B2 --> S3
    B3 --> S4
    S1 & S4 --> G1
    G1 --> G2 & G3
    S1 & S3 --> ST
    ST --> G4 & G5
    SILVER -. "pipeline event log" .-> DQ
    G2 & G3 & G4 ==> BI

    classDef src fill:#EDE7FF,stroke:#7C4DFF,stroke-width:2px,color:#2D1B69
    classDef land fill:#E0F7FA,stroke:#00838F,stroke-width:2px,color:#004D55
    classDef bronze fill:#F9E0C7,stroke:#B26A2B,stroke-width:2px,color:#5A2E0A
    classDef silver fill:#ECEFF4,stroke:#7B8794,stroke-width:2px,color:#2B333B
    classDef gold fill:#FFF3C4,stroke:#D4A017,stroke-width:2px,color:#5C4300
    classDef warn fill:#FFE0E0,stroke:#D32F2F,stroke-width:2px,color:#7F1010
    classDef stream fill:#DCEBFF,stroke:#1E63C6,stroke-width:2px,color:#0B2E66
    classDef bi fill:#DFF5E3,stroke:#2E8B57,stroke-width:2px,color:#123D22

    class OS,RA,RP src
    class V land
    class B1,B2,B3 bronze
    class S1,S2,S3,S4 silver
    class SQ warn
    class G1,G2,G3,G4,G5,DQ gold
    class ST stream
    class BI bi

    style SRC fill:#F7F4FF,stroke:#7C4DFF,stroke-width:2px,color:#2D1B69
    style BRONZE fill:#FDF3EA,stroke:#B26A2B,stroke-width:2px,color:#5A2E0A
    style SILVER fill:#F7F8FA,stroke:#7B8794,stroke-width:2px,color:#2B333B
    style GOLD fill:#FFFBEA,stroke:#D4A017,stroke-width:2px,color:#5C4300
```

### Orchestration · Lakeflow Jobs

Defined as code in `resources/food_delivery.job.yml`, runs hourly (IST) on serverless compute.

```mermaid
%%{init: {"theme": "base", "themeVariables": {"fontFamily": "Segoe UI, Helvetica, Arial", "fontSize": "15px", "lineColor": "#5B6B7F"}}}%%
flowchart LR
    T1["🏗️ setup_unity_catalog"] --> T2["🎲 generate_source_events"]
    T2 --> T3["🔄 medallion_pipeline<br/><i>Lakeflow Declarative Pipeline</i>"]
    T3 --> T4["⚡ zone_demand_streaming"]
    T3 --> T5["✅ data_quality_report"]

    classDef setup fill:#EDE7FF,stroke:#7C4DFF,stroke-width:2px,color:#2D1B69
    classDef gen fill:#E0F7FA,stroke:#00838F,stroke-width:2px,color:#004D55
    classDef pipe fill:#FFE3D6,stroke:#FF3621,stroke-width:2px,color:#7A1500
    classDef stream fill:#DCEBFF,stroke:#1E63C6,stroke-width:2px,color:#0B2E66
    classDef dq fill:#DFF5E3,stroke:#2E8B57,stroke-width:2px,color:#123D22
    class T1 setup
    class T2 gen
    class T3 pipe
    class T4 stream
    class T5 dq
```

## What makes the data realistic

`src/fooddelivery/simulator.py` plays the role of the upstream systems. It is deterministic (seeded), resumable between job runs, and intentionally messy:

| Real-world problem | How it shows up | How the platform handles it |
|---|---|---|
| Client retries | ~3% duplicate events with the same `event_id` | `dropDuplicatesWithinWatermark` in Silver |
| Late-arriving data | ~2% of events land in a later file than their event time | 30-min event-time watermark; AUTO CDC `sequence_by` never moves an order backwards |
| Out-of-order status changes | events with equal timestamps | `sequence_by = struct(event_ts, status_rank)` tie-breaker |
| Bad records | null `order_id`/`restaurant_id`, negative amounts, unknown statuses, 0,0 GPS | `expect_all_or_drop` + quarantine table with the failed rule names |
| Schema drift | app v5.2 starts sending `tip_amount` from batch 4 | Auto Loader `addNewColumns`; `tip_amount` defaulted for old rows |
| Slowly changing master data | restaurant commission / active flag / cuisine change over time | AUTO CDC **SCD Type 2** + point-in-time join in Gold |
| Peak hours and rider shortage | lunch/dinner peaks in IST, finite riders per zone | `NO_RIDER_AVAILABLE` cancellations; surge flag per zone |

A 12-hour simulated day produces roughly 27K order events, 180K rider pings, ~6.8K orders across Hyderabad, Bengaluru, Chennai and Mumbai, with ~8–9% cancellations and ~11% SLA breaches.

## Repository layout

```
databricks.yml                     Asset Bundle (dev / prod targets, catalog variable)
resources/
  food_delivery.pipeline.yml       Lakeflow Declarative Pipeline definition (serverless)
  food_delivery.job.yml            Lakeflow Job: 5 tasks, schedule, retries, parameters
pipelines/
  bronze.py                        Auto Loader streaming tables + ingestion metadata
  silver.py                        typing, dedup, expectations, quarantine, AUTO CDC SCD1/SCD2
  gold.py                          materialized views: lifecycle fact, city KPIs, restaurant scorecard
notebooks/
  00_setup_unity_catalog.sql       schemas, landing Volume, grants
  01_generate_events.py            runs the simulator, writes JSON to the Volume
  02_zone_demand_streaming.py      Structured Streaming windows + foreachBatch MERGE
  03_data_quality_report.py        expectation metrics from the pipeline event log
src/fooddelivery/
  transforms.py                    all PySpark business logic (pure functions, unit tested)
  quality.py                       data quality rules shared by expectations and quarantine
  simulator.py                     source data simulator
  config.py                        SLA, watermark, window size, statuses, cities
sql/dashboard_queries.sql          queries for the Databricks SQL / AI/BI dashboard
tests/                             pytest suite run locally and in CI (incl. a real streaming test)
.github/workflows/ci.yml           lint + tests on every PR, bundle deploy on main
```

## Run it (Databricks Free Edition works)

1. Create a free workspace at Databricks Free Edition. Generate a personal access token (User settings -> Developer).
2. Install the Databricks CLI and authenticate:
   ```bash
   databricks configure        # host = your workspace URL, token = the PAT
   ```
3. Deploy and run:
   ```bash
   databricks bundle validate
   databricks bundle deploy -t dev
   databricks bundle run -t dev food_delivery_job
   ```
   Each run adds one simulated hour of business. Run it 3–4 times to see dinner-peak surge windows.
4. Explore the tables in Catalog Explorer, open the pipeline graph, and paste `sql/dashboard_queries.sql` into a Databricks SQL dashboard.

The catalog defaults to `workspace` (exists in Free Edition). Use another one with `--var catalog=<name>`.

### Run the tests locally

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt      # needs Java 17
pytest -q
```

## Key design decisions

* **Business logic lives in `src/`, not in pipeline files.** The pipeline files only declare tables; every transformation is a pure DataFrame function, so it is unit-tested locally and in CI without a cluster.
* **Bronze is append-only and keeps everything** (with `_source_file`, `_ingested_at`), so Silver/Gold can always be rebuilt with a full refresh.
* **Bad data is quarantined, not just dropped.** The same rule dictionary drives the expectations and the quarantine table, so both sides always agree; the event log turns expectation results into a trended Gold table.
* **AUTO CDC instead of hand-written MERGE** for order state and restaurant history: it handles out-of-order events and SCD2 bookkeeping (`__START_AT`/`__END_AT`) declaratively.
* **One windowed aggregation instead of a stream-stream join** for demand vs. supply: both streams are unioned into a common shape, which keeps state small and simple.
* **`foreachBatch` + MERGE** makes the streaming outputs idempotent and lets late data update an existing window row.
* **`availableNow` trigger and serverless compute** give exactly-once streaming semantics at batch cost; switching to continuous is a config change.
* **Liquid clustering** (`cluster_by`) on the large tables instead of static partitioning.
* **Everything is code**: pipeline, job, schedule and parameters are deployed by Asset Bundles from GitHub Actions.

## Possible extensions

* Replace the Volume landing zone with Kafka / Event Hubs as the order-event source.
* Add a delivery-time prediction model (features from `fact_order_lifecycle`, MLflow, Model Serving).
* Column masks on `customer_id` and row filters per city for regional ops teams.
* SQL alerts on `sla_breach_pct` and `pass_rate_pct`.

## Author

**Akshay Besarla** - Data Engineer | [LinkedIn](https://www.linkedin.com/in/akshay-besarla-249979265) | [GitHub](https://github.com/akshaybesarla)
