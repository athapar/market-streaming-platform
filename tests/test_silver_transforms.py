import pytest

pyspark = pytest.importorskip("pyspark")

from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from market_streaming.silver.transforms import (
    MERGE_KEYS,
    SILVER_COLUMNS,
    join_security_master,
    parse_silver,
    silver_ddl,
)


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder
        .master("local[1]")
        .appName("silver-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield s
    s.stop()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _bronze_schema() -> StructType:
    """Subset of Bronze columns that Silver's parse_silver reads."""
    return StructType([
        StructField("kafka_topic",       StringType(),    True),
        StructField("kafka_partition",   IntegerType(),   True),
        StructField("kafka_offset",      LongType(),      True),
        StructField("kafka_timestamp",   TimestampType(), True),
        StructField("raw_payload",       StringType(),    True),
        StructField("ingest_timestamp",  TimestampType(), True),
        StructField("event_type",        StringType(),    True),
        StructField("ingest_date",       DateType(),      True),
    ])


def _seed_schema() -> StructType:
    return StructType([
        StructField("ticker",         StringType(), True),
        StructField("composite_figi", StringType(), True),
    ])


def _make_bronze_row(
    symbol: str = "AAPL",
    raw_payload: str | None = None,
    kafka_offset: int = 1,
) -> tuple:
    if raw_payload is None:
        raw_payload = (
            f'{{"ev":"AM","sym":"{symbol}","o":150.0,"h":151.0,'
            f'"l":149.0,"c":150.5,"v":1000000,"vw":150.3,"n":500,'
            f'"s":1716480000000,"e":1716480060000}}'
        )
    ts = datetime(2026, 5, 23, 14, 0, tzinfo=timezone.utc)
    from datetime import date
    return (
        "market.aggregates",
        0,
        kafka_offset,
        ts,
        raw_payload,
        ts,
        "AM",
        date(2026, 5, 23),
    )


# ---------------------------------------------------------------------------
# parse_silver
# ---------------------------------------------------------------------------

def test_parse_silver_produces_expected_columns(spark):
    df = spark.createDataFrame([_make_bronze_row()], _bronze_schema())
    result = parse_silver(df)
    assert set(result.columns) >= {
        "symbol", "event_type", "open_price", "high_price", "low_price",
        "close_price", "volume", "vwap", "trade_count",
        "window_start", "window_end", "event_date",
        "kafka_topic", "kafka_partition", "kafka_offset",
        "bronze_ingest_timestamp", "silver_timestamp",
    }


def test_parse_silver_correct_values(spark):
    df = spark.createDataFrame([_make_bronze_row("AAPL")], _bronze_schema())
    row = parse_silver(df).collect()[0].asDict()

    assert row["symbol"] == "AAPL"
    assert row["event_type"] == "AM"
    assert row["open_price"] == pytest.approx(150.0)
    assert row["close_price"] == pytest.approx(150.5)
    assert row["volume"] == 1_000_000
    assert row["trade_count"] == 500
    # epoch ms 1716480000000 → 2024-05-23 14:00:00 UTC
    assert row["window_start"] is not None
    assert row["event_date"] is not None


def test_parse_silver_drops_raw_payload(spark):
    df = spark.createDataFrame([_make_bronze_row()], _bronze_schema())
    result = parse_silver(df)
    assert "raw_payload" not in result.columns
    assert "ingest_date" not in result.columns


def test_parse_silver_drops_invalid_json(spark):
    """Rows with unparseable payload must be dropped (symbol/window_start → NULL → filtered)."""
    df = spark.createDataFrame(
        [_make_bronze_row(raw_payload="not json at all")],
        _bronze_schema(),
    )
    result = parse_silver(df)
    assert result.count() == 0


def test_parse_silver_drops_missing_symbol(spark):
    """Payload missing 'sym' field → symbol is NULL → row is filtered out."""
    payload = '{"ev":"AM","o":100.0,"h":101.0,"l":99.0,"c":100.5,"v":500,"vw":100.2,"n":10,"s":1716480000000,"e":1716480060000}'
    df = spark.createDataFrame(
        [_make_bronze_row(raw_payload=payload)],
        _bronze_schema(),
    )
    assert parse_silver(df).count() == 0


# ---------------------------------------------------------------------------
# join_security_master
# ---------------------------------------------------------------------------

def test_join_attaches_figi_for_known_symbol(spark):
    df = spark.createDataFrame([_make_bronze_row("AAPL")], _bronze_schema())
    parsed = parse_silver(df)
    seed = spark.createDataFrame(
        [("AAPL", "BBG000B9XRY4")],
        _seed_schema(),
    )
    result = join_security_master(parsed, seed).collect()[0].asDict()
    assert result["composite_figi"] == "BBG000B9XRY4"


def test_join_null_figi_for_unknown_symbol(spark):
    """Unknown symbol gets NULL figi but the row is NOT dropped."""
    df = spark.createDataFrame([_make_bronze_row("UNKNOWN")], _bronze_schema())
    parsed = parse_silver(df)
    seed = spark.createDataFrame(
        [("AAPL", "BBG000B9XRY4")],
        _seed_schema(),
    )
    result = join_security_master(parsed, seed).collect()
    assert len(result) == 1
    assert result[0]["composite_figi"] is None


def test_join_no_rows_dropped(spark):
    """Row count is unchanged regardless of whether symbol is in seed."""
    rows = [_make_bronze_row("AAPL", kafka_offset=1),
            _make_bronze_row("MSFT", kafka_offset=2),
            _make_bronze_row("UNKNOWN", kafka_offset=3)]
    df = spark.createDataFrame(rows, _bronze_schema())
    parsed = parse_silver(df)
    seed = spark.createDataFrame(
        [("AAPL", "BBG000B9XRY4"), ("MSFT", "BBG000BPH459")],
        _seed_schema(),
    )
    assert join_security_master(parsed, seed).count() == 3


# ---------------------------------------------------------------------------
# silver_ddl
# ---------------------------------------------------------------------------

def test_silver_ddl_is_delta(spark):
    ddl = silver_ddl("main.market_streaming.silver_market_events")
    assert "USING DELTA" in ddl


def test_silver_ddl_contains_table_name(spark):
    ddl = silver_ddl("main.market_streaming.silver_market_events")
    assert "main.market_streaming.silver_market_events" in ddl


def test_silver_ddl_partitioned_by_event_date(spark):
    ddl = silver_ddl("main.market_streaming.silver_market_events")
    assert "PARTITIONED BY (event_date)" in ddl


def test_silver_ddl_cdf_enabled(spark):
    ddl = silver_ddl("main.market_streaming.silver_market_events")
    assert "enableChangeDataFeed" in ddl
    assert "'true'" in ddl


def test_silver_ddl_merge_key_columns_not_null(spark):
    ddl = silver_ddl("main.market_streaming.silver_market_events")
    for key in MERGE_KEYS:
        # The DDL marks merge key columns NOT NULL
        assert f"{key}" in ddl and "NOT NULL" in ddl
