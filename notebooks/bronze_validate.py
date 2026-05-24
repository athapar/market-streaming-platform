# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Validation — Crash Test Deliverable
# MAGIC
# MAGIC Run this after the crash test (kill cluster mid-stream, restart, let
# MAGIC ingestion catch up). The queries below answer the question the spec
# MAGIC §5.6 demands an answer to:
# MAGIC
# MAGIC > Did the restart preserve exactly-once? Are Kafka offsets contiguous
# MAGIC > in Delta with no gaps and no duplicates?
# MAGIC
# MAGIC **What "pass" looks like:**
# MAGIC 1. `offset_gap_check` returns zero rows (no missing offsets per
# MAGIC    `(kafka_topic, kafka_partition)`).
# MAGIC 2. `offset_duplicate_check` returns zero rows.
# MAGIC 3. Delta history shows multiple commits across the restart with no
# MAGIC    write failures or restored versions.
# MAGIC
# MAGIC Save the cell outputs as evidence; that's the Phase 2 deliverable.

# COMMAND ----------
dbutils.widgets.text("target_catalog", "main", "Target catalog")
dbutils.widgets.text("target_schema", "market_streaming", "Target schema")
dbutils.widgets.text("target_table_name", "bronze_market_events", "Target table")

target_table = (
    f"{dbutils.widgets.get('target_catalog')}."
    f"{dbutils.widgets.get('target_schema')}."
    f"{dbutils.widgets.get('target_table_name')}"
)
print(f"validating: {target_table}")

# COMMAND ----------
# MAGIC %md ## 1. Row count and offset range per topic+partition

# COMMAND ----------
spark.sql(f"""
SELECT
  kafka_topic,
  kafka_partition,
  COUNT(*) AS rows,
  MIN(kafka_offset) AS min_offset,
  MAX(kafka_offset) AS max_offset,
  MAX(kafka_offset) - MIN(kafka_offset) + 1 AS expected_rows,
  (MAX(kafka_offset) - MIN(kafka_offset) + 1) - COUNT(*) AS gap_count
FROM {target_table}
GROUP BY kafka_topic, kafka_partition
ORDER BY kafka_topic, kafka_partition
""").display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. offset_gap_check — should return ZERO rows
# MAGIC
# MAGIC Looks for `(topic, partition, offset)` values missing in the middle of
# MAGIC a continuous run. If anything appears here, the checkpoint failed to
# MAGIC preserve exactly-once across the restart.

# COMMAND ----------
spark.sql(f"""
WITH numbered AS (
  SELECT
    kafka_topic,
    kafka_partition,
    kafka_offset,
    LAG(kafka_offset) OVER (
      PARTITION BY kafka_topic, kafka_partition ORDER BY kafka_offset
    ) AS prev_offset
  FROM {target_table}
)
SELECT
  kafka_topic, kafka_partition, prev_offset, kafka_offset,
  kafka_offset - prev_offset AS jump
FROM numbered
WHERE prev_offset IS NOT NULL
  AND kafka_offset - prev_offset > 1
ORDER BY kafka_topic, kafka_partition, kafka_offset
""").display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. offset_duplicate_check — should return ZERO rows
# MAGIC
# MAGIC Looks for `(topic, partition, offset)` rows that appear more than
# MAGIC once. A duplicate here means the checkpoint was deleted or the
# MAGIC commit/offset coordination broke.

# COMMAND ----------
spark.sql(f"""
SELECT kafka_topic, kafka_partition, kafka_offset, COUNT(*) AS occurrences
FROM {target_table}
GROUP BY kafka_topic, kafka_partition, kafka_offset
HAVING COUNT(*) > 1
ORDER BY occurrences DESC, kafka_topic, kafka_partition, kafka_offset
""").display()

# COMMAND ----------
# MAGIC %md ## 4. Delta history — confirm restart shows new commits

# COMMAND ----------
spark.sql(f"DESCRIBE HISTORY {target_table}").display()

# COMMAND ----------
# MAGIC %md ## 5. Sample recent rows

# COMMAND ----------
spark.sql(f"""
SELECT
  kafka_topic, kafka_partition, kafka_offset, kafka_timestamp,
  event_type, LEFT(raw_payload, 200) AS payload_preview
FROM {target_table}
ORDER BY kafka_timestamp DESC
LIMIT 20
""").display()
