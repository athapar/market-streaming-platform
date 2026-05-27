# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline Orchestrator — Single-Click Full Pipeline Run
# MAGIC
# MAGIC Runs every layer of the streaming pipeline in dependency order:
# MAGIC
# MAGIC ```
# MAGIC Bronze (Kafka → Delta)
# MAGIC    ├── Silver AM bars
# MAGIC    ├── Silver trades
# MAGIC    └── Silver quotes
# MAGIC          ├── Gold minute bars + daily rollup
# MAGIC          ├── Gold trades
# MAGIC          └── Gold quote stats
# MAGIC                └── Snowflake sync (all tables)
# MAGIC ```
# MAGIC
# MAGIC **Usage:** Attach to a cluster and Run All. Each step reports its status
# MAGIC and row counts. If any step fails, subsequent steps are skipped and the
# MAGIC failure is reported with the notebook name and error.
# MAGIC
# MAGIC **When to use:**
# MAGIC - Interactive catch-up during/after market hours
# MAGIC - Ad-hoc backfill after a producer outage
# MAGIC - Testing after code changes
# MAGIC
# MAGIC For scheduled, unattended runs, use the Databricks Workflow defined in
# MAGIC `workflows/pipeline_workflow.yml`.

# COMMAND ----------

# MAGIC %md ## Configuration

# COMMAND ----------

import time
from datetime import datetime, timezone

dbutils.widgets.dropdown("trigger_type", "availableNow",
                         ["availableNow", "processingTime", "once"],
                         "Trigger type")
dbutils.widgets.text("timeout_seconds", "600",
                     "Max seconds per notebook step")

trigger_type    = dbutils.widgets.get("trigger_type")
timeout_seconds = int(dbutils.widgets.get("timeout_seconds"))

notebook_base = "/Workspace/Users/armaant.08@gmail.com/market-streaming-platform/notebooks"

print(f"trigger_type    = {trigger_type}")
print(f"timeout_seconds = {timeout_seconds}")
print(f"notebook_base   = {notebook_base}")
print(f"started at      = {datetime.now(timezone.utc).isoformat()}")

# COMMAND ----------

# MAGIC %md ## Pipeline Steps

# COMMAND ----------

def run_step(name: str, notebook: str, params: dict = None) -> dict:
    """Run a notebook and return status dict."""
    full_path = f"{notebook_base}/{notebook}"
    default_params = {"trigger_type": trigger_type}
    if params:
        default_params.update(params)

    print(f"\n{'='*60}")
    print(f"[{name}] starting: {full_path}")
    print(f"  params: {default_params}")
    t0 = time.time()

    try:
        result = dbutils.notebook.run(full_path, timeout_seconds, default_params)
        elapsed = time.time() - t0
        print(f"[{name}] completed in {elapsed:.1f}s — result: {result}")
        return {"step": name, "status": "success", "elapsed_s": round(elapsed, 1),
                "result": result}
    except Exception as exc:
        elapsed = time.time() - t0
        print(f"[{name}] FAILED after {elapsed:.1f}s — {exc}")
        return {"step": name, "status": "failed", "elapsed_s": round(elapsed, 1),
                "error": str(exc)[:200]}

# COMMAND ----------

# MAGIC %md ### Step 1: Bronze — Kafka → Delta

# COMMAND ----------

results = []
r = run_step("bronze", "bronze_ingest")
results.append(r)

# COMMAND ----------

# MAGIC %md ### Step 2: Silver — Bronze → Typed, Deduped, FIGI-Joined
# MAGIC
# MAGIC Three Silver tables run sequentially (they all read from the same Bronze
# MAGIC table but write to independent targets with separate checkpoints).

# COMMAND ----------

if results[-1]["status"] == "success":
    r = run_step("silver_am", "silver_ingest")
    results.append(r)
else:
    print("[silver_am] SKIPPED — bronze failed")
    results.append({"step": "silver_am", "status": "skipped"})

# COMMAND ----------

if results[-1]["status"] != "failed":
    r = run_step("silver_trades", "trades_silver_ingest")
    results.append(r)
else:
    print("[silver_trades] SKIPPED — previous step failed")
    results.append({"step": "silver_trades", "status": "skipped"})

# COMMAND ----------

if results[-1]["status"] != "failed":
    r = run_step("silver_quotes", "quotes_silver_ingest")
    results.append(r)
else:
    print("[silver_quotes] SKIPPED — previous step failed")
    results.append({"step": "silver_quotes", "status": "skipped"})

# COMMAND ----------

# MAGIC %md ### Step 3: Gold — Silver → Serving Tables

# COMMAND ----------

# Gold AM depends on Silver AM
silver_am_ok = any(r["step"] == "silver_am" and r["status"] == "success" for r in results)

if silver_am_ok:
    r = run_step("gold_am", "gold_ingest")
    results.append(r)
else:
    print("[gold_am] SKIPPED — silver_am did not succeed")
    results.append({"step": "gold_am", "status": "skipped"})

# COMMAND ----------

# Gold trades depends on Silver trades
silver_trades_ok = any(r["step"] == "silver_trades" and r["status"] == "success" for r in results)

if silver_trades_ok:
    r = run_step("gold_trades", "trades_gold_ingest")
    results.append(r)
else:
    print("[gold_trades] SKIPPED — silver_trades did not succeed")
    results.append({"step": "gold_trades", "status": "skipped"})

# COMMAND ----------

# Gold quotes depends on Silver quotes
silver_quotes_ok = any(r["step"] == "silver_quotes" and r["status"] == "success" for r in results)

if silver_quotes_ok:
    r = run_step("gold_quotes", "quotes_gold_ingest")
    results.append(r)
else:
    print("[gold_quotes] SKIPPED — silver_quotes did not succeed")
    results.append({"step": "gold_quotes", "status": "skipped"})

# COMMAND ----------

# MAGIC %md ### Step 4: Snowflake Sync

# COMMAND ----------

any_gold_ok = any(
    r["step"].startswith("gold_") and r["status"] == "success"
    for r in results
)

if any_gold_ok:
    r = run_step("snowflake_sync", "snowflake_sync")
    results.append(r)
else:
    print("[snowflake_sync] SKIPPED — no Gold steps succeeded")
    results.append({"step": "snowflake_sync", "status": "skipped"})

# COMMAND ----------

# MAGIC %md ## Pipeline Summary

# COMMAND ----------

from pyspark.sql import Row

summary_rows = [Row(**r) for r in results]
summary_df = spark.createDataFrame(summary_rows)
summary_df.display()

# COMMAND ----------

total_elapsed = sum(r.get("elapsed_s", 0) for r in results)
succeeded = sum(1 for r in results if r["status"] == "success")
failed = sum(1 for r in results if r["status"] == "failed")
skipped = sum(1 for r in results if r["status"] == "skipped")

print(f"\n{'='*60}")
print(f"Pipeline complete at {datetime.now(timezone.utc).isoformat()}")
print(f"  Total elapsed : {total_elapsed:.1f}s")
print(f"  Succeeded     : {succeeded}")
print(f"  Failed        : {failed}")
print(f"  Skipped       : {skipped}")

if failed > 0:
    failed_steps = [r["step"] for r in results if r["status"] == "failed"]
    print(f"  Failed steps  : {', '.join(failed_steps)}")
    dbutils.notebook.exit(f"FAILED: {', '.join(failed_steps)}")
else:
    dbutils.notebook.exit(f"OK: {succeeded} steps in {total_elapsed:.1f}s")
