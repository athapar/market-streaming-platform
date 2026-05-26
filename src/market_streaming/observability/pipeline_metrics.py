"""
Pipeline batch-level metrics — written by each Spark foreachBatch handler.

Records one row per (layer, batch_id) with row counts, duration, and timestamp.
The metrics table lives alongside Bronze/Silver/Gold in the same catalog and is
synced to Snowflake for dbt observability models and the dashboard.
"""
from __future__ import annotations

import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from pyspark.sql import Row
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


METRICS_SCHEMA = StructType([
    StructField("layer",          StringType(),    False),
    StructField("batch_id",       IntegerType(),   False),
    StructField("rows_in",        LongType(),      False),
    StructField("rows_out",       LongType(),      False),
    StructField("duration_ms",    LongType(),      False),
    StructField("started_at",     TimestampType(), False),
    StructField("completed_at",   TimestampType(), False),
    StructField("status",         StringType(),    False),
    StructField("error_message",  StringType(),    True),
])


def metrics_ddl(table: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
  layer          STRING        NOT NULL,
  batch_id       INTEGER       NOT NULL,
  rows_in        BIGINT        NOT NULL,
  rows_out       BIGINT        NOT NULL,
  duration_ms    BIGINT        NOT NULL,
  started_at     TIMESTAMP     NOT NULL,
  completed_at   TIMESTAMP     NOT NULL,
  status         STRING        NOT NULL,
  error_message  STRING
)
USING DELTA
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
""".strip()


@dataclass
class BatchTracker:
    """Context manager that times a foreachBatch execution and records metrics."""

    spark: "SparkSession"
    metrics_table: str
    layer: str
    batch_id: int = 0
    rows_in: int = 0
    rows_out: int = 0
    _start: float = field(default=0.0, repr=False)
    _started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc), repr=False)

    def begin(self) -> "BatchTracker":
        self._start = time.monotonic()
        self._started_at = datetime.now(timezone.utc)
        return self

    def record(self, *, rows_in: int, rows_out: int) -> None:
        self.rows_in = rows_in
        self.rows_out = rows_out

    def finish(self, status: str = "success", error_message: str | None = None) -> None:
        completed_at = datetime.now(timezone.utc)
        duration_ms = int((time.monotonic() - self._start) * 1000)

        row = Row(
            layer=self.layer,
            batch_id=self.batch_id,
            rows_in=self.rows_in,
            rows_out=self.rows_out,
            duration_ms=duration_ms,
            started_at=self._started_at,
            completed_at=completed_at,
            status=status,
            error_message=error_message,
        )
        df = self.spark.createDataFrame([row], schema=METRICS_SCHEMA)
        df.write.format("delta").mode("append").saveAsTable(self.metrics_table)


@contextmanager
def track_batch(
    spark: "SparkSession",
    metrics_table: str,
    layer: str,
    batch_id: int,
):
    """Context manager for foreachBatch instrumentation.

    Usage::

        with track_batch(spark, table, "silver", batch_id) as tracker:
            tracker.record(rows_in=100, rows_out=95)
            # ... do work ...
    """
    tracker = BatchTracker(
        spark=spark,
        metrics_table=metrics_table,
        layer=layer,
        batch_id=batch_id,
    )
    tracker.begin()
    try:
        yield tracker
        tracker.finish("success")
    except Exception as exc:
        tracker.finish("error", str(exc)[:500])
        raise
