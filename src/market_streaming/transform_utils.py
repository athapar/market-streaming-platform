"""
Small, pure Spark helpers shared across the Silver/Gold transform layers.

Extracted so the performance-sensitive logic (within-batch dedup and the MERGE
condition that drives partition pruning) lives in one place and is unit-testable
without a Databricks cluster or a Delta write.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def dedup_keep_latest(
    df: "DataFrame",
    keys: list[str],
    order_col: str = "kafka_offset",
) -> "DataFrame":
    """Keep one row per ``keys`` — the one with the greatest ``order_col``.

    Implemented as a single hash aggregation over a struct ordered by
    ``order_col``. struct comparison is field-by-field, so
    ``max(struct(order_col, ...))`` picks the winning row without the
    sort+shuffle that ``row_number()`` over a window requires (OPT-4).

    The returned DataFrame carries exactly the input columns (re-ordered with
    ``order_col`` first); the grouping keys come through inside the struct, so
    there is no duplicate-column ambiguity.
    """
    rest = [c for c in df.columns if c != order_col]
    return (
        df
        .groupBy(*keys)
        .agg(F.max(F.struct(order_col, *rest)).alias("_m"))
        .select("_m.*")
    )


def build_merge_condition(keys: list[str], partition_col: str | None = None) -> str:
    """Build a Delta MERGE ``ON`` predicate of ``tgt.k = src.k`` equalities.

    When ``partition_col`` is given (and not already among ``keys``) it is added
    first so Delta can prune partitions instead of scanning the whole target
    table (OPT-3). It is always functionally derived from the keys, so it never
    changes which rows match — only how few files Delta has to open.
    """
    merge_keys = (
        [partition_col, *keys]
        if partition_col and partition_col not in keys
        else list(keys)
    )
    return " AND ".join(f"tgt.{k} = src.{k}" for k in merge_keys)
