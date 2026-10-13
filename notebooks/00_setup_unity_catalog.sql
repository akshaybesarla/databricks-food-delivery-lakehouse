-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 · Unity Catalog setup
-- MAGIC Creates the medallion schemas, the landing Volume that the event feeds are dropped into,
-- MAGIC and (optionally) the access model. Idempotent - safe to run on every job run.
-- MAGIC
-- MAGIC | Schema | Purpose | Who reads it |
-- MAGIC |---|---|---|
-- MAGIC | `landing` | Volume `raw` with JSON files as delivered by source systems | pipeline only |
-- MAGIC | `bronze` | Raw, append-only Delta tables (Auto Loader) | data engineers |
-- MAGIC | `silver` | Clean, validated, de-duplicated, CDC-applied | engineers, data scientists |
-- MAGIC | `gold` | Business-ready facts and KPIs | analysts, dashboards |

-- COMMAND ----------

CREATE WIDGET TEXT catalog DEFAULT 'workspace';

-- COMMAND ----------

USE CATALOG IDENTIFIER(:catalog);

CREATE SCHEMA IF NOT EXISTS landing COMMENT 'Raw files delivered by source systems';
CREATE SCHEMA IF NOT EXISTS bronze  COMMENT 'Raw append-only Delta tables';
CREATE SCHEMA IF NOT EXISTS silver  COMMENT 'Cleaned, validated, conformed data';
CREATE SCHEMA IF NOT EXISTS gold    COMMENT 'Business-ready facts and KPIs';

CREATE VOLUME IF NOT EXISTS landing.raw COMMENT 'Landing zone: order_events/, rider_pings/, restaurants/';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Access model (run once in a workspace that has these account groups)
-- MAGIC Least privilege: analysts only ever see Gold; data scientists also get Silver.
-- MAGIC ```sql
-- MAGIC GRANT USE CATALOG ON CATALOG workspace TO `analysts`;
-- MAGIC GRANT USE SCHEMA, SELECT ON SCHEMA workspace.gold TO `analysts`;
-- MAGIC GRANT USE SCHEMA, SELECT ON SCHEMA workspace.silver TO `data_scientists`;
-- MAGIC GRANT READ VOLUME, WRITE VOLUME ON VOLUME workspace.landing.raw TO `ingestion_sp`;
-- MAGIC ```
-- MAGIC Customer IDs are pseudonymous in this dataset; in production a column mask
-- MAGIC (`ALTER TABLE ... ALTER COLUMN customer_id SET MASK ...`) would hide them from analysts.
