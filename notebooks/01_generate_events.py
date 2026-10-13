# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Generate source events
# MAGIC Plays the role of the upstream systems (order service, rider app, restaurant admin portal).
# MAGIC Each run continues the simulated timeline from where the previous run stopped and drops
# MAGIC new JSON files into the landing Volume, exactly like an upstream export job would.
# MAGIC
# MAGIC State lives in `/Volumes/<catalog>/landing/raw/_simulator_state.json`.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("batches", "4")  # 4 x 15 min = 1 hour of business per run
dbutils.widgets.text("seed", "42")

catalog = dbutils.widgets.get("catalog")
batches = int(dbutils.widgets.get("batches"))
seed = int(dbutils.widgets.get("seed"))
landing = f"/Volumes/{catalog}/landing/raw"
state_path = f"{landing}/_simulator_state.json"

# COMMAND ----------

import os
import sys

# Make the repo's src/ folder importable (works for bundle deployments and Git folders).
src = os.path.abspath(os.path.join(os.getcwd(), "..", "src"))
if src not in sys.path:
    sys.path.append(src)

from fooddelivery.simulator import FoodDeliverySimulator, write_batch

# COMMAND ----------

if os.path.exists(state_path):
    with open(state_path) as fh:
        sim = FoodDeliverySimulator.from_state_json(fh.read(), seed=seed)
    print(f"Resuming simulation at batch {sim.state.next_batch} ({sim.state.clock})")
else:
    sim = FoodDeliverySimulator(seed=seed)
    print("Starting a new simulation")

summary = []
for _ in range(batches):
    batch = sim.next_batch()
    counts = write_batch(batch, landing)
    summary.append({"batch_id": batch["batch_id"], "window_start": batch["window_start"], **counts})

with open(state_path, "w") as fh:
    fh.write(sim.state_json())

display(spark.createDataFrame(summary))
