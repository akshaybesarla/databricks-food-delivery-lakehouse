# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Data quality report and pipeline observability
# MAGIC Reads the Lakeflow pipeline **event log** and turns expectation results into a Gold table,
# MAGIC so data quality is trended over time and can be alerted on (Databricks SQL alert on
# MAGIC `pass_rate_pct < 97`). Also summarises the quarantine table by failed rule.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("pipeline_id", "")
catalog = dbutils.widgets.get("catalog")
pipeline_id = dbutils.widgets.get("pipeline_id")

# COMMAND ----------

dq = spark.sql(f"""
WITH expectations AS (
  SELECT
    timestamp,
    origin.update_id AS update_id,
    explode(from_json(details:flow_progress:data_quality:expectations,
                      'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) AS e
  FROM event_log('{pipeline_id}')
  WHERE event_type = 'flow_progress'
    AND details:flow_progress:data_quality:expectations IS NOT NULL
)
SELECT
  update_id,
  max(timestamp)            AS update_ts,
  e.dataset                 AS dataset,
  e.name                    AS expectation,
  sum(e.passed_records)     AS passed_records,
  sum(e.failed_records)     AS failed_records,
  round(100 * sum(e.passed_records) / nullif(sum(e.passed_records) + sum(e.failed_records), 0), 2) AS pass_rate_pct
FROM expectations
GROUP BY update_id, e.dataset, e.name
""")

(dq.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.dq_expectation_results"))

display(dq.orderBy("pass_rate_pct"))

# COMMAND ----------

# MAGIC %md ### Why were rows quarantined?

# COMMAND ----------

display(
    spark.sql(f"""
SELECT rule, count(*) AS rows_quarantined, min(_ingested_at) AS first_seen, max(_ingested_at) AS last_seen
FROM {catalog}.silver.order_events_quarantine
LATERAL VIEW explode(_failed_rules) AS rule
GROUP BY rule
ORDER BY rows_quarantined DESC
""")
)
