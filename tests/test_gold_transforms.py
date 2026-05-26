import pytest

pyspark = pytest.importorskip("pyspark")

from datetime import date, datetime, timezone

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

from market_streaming.gold.transforms import (
    DAILY_MERGE_KEYS,
    GOLD_MINUTE_COLUMNS,
    MINUTE_MERGE_KEYS,
    aggregate_daily,
    daily_rollup_ddl,
    minute_bars_ddl,
)


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder
        .master("local[1]")
        .appName("gold-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield s
    s.stop()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _silver_schema() -> StructType:
    return StructType([
        StructField("composite_figi",  StringType(),    True),
        StructField("symbol",          StringType(),    True),
        StructField("event_date",      DateType(),      True),
        StructField("window_start",    TimestampType(), True),
        StructField("open_price",      DoubleType(),    True),
        StructField("high_price",      DoubleType(),    True),
        StructField("low_price",       DoubleType(),    True),
        StructField("close_price",     DoubleType(),    True),
        StructField("volume",          LongType(),      True),
        StructField("vwap",            DoubleType(),    True),
        StructField("trade_count",     IntegerType(),   True),
    ])


def _ts(h: int, m: int) -> datetime:
    return datetime(2026, 5, 26, h, m, tzinfo=timezone.utc)


def _make_silver_rows(spark):
    """Three AAPL bars and two MSFT bars on the same date."""
    rows = [
        # figi,            sym,    date,               window_start, o,    h,    l,    c,    vol,    vwap,   n
        ("BBG000B9XRY4", "AAPL", date(2026,5,26), _ts(13,30), 310.0, 311.0, 309.5, 310.5, 10_000, 310.3, 100),
        ("BBG000B9XRY4", "AAPL", date(2026,5,26), _ts(13,31), 310.5, 312.0, 310.0, 311.0, 15_000, 311.0, 150),
        ("BBG000B9XRY4", "AAPL", date(2026,5,26), _ts(13,32), 311.0, 311.5, 310.8, 311.2,  5_000, 311.1,  50),
        ("BBG000BPH459", "MSFT", date(2026,5,26), _ts(13,30), 416.0, 417.0, 415.5, 416.5, 20_000, 416.4, 200),
        ("BBG000BPH459", "MSFT", date(2026,5,26), _ts(13,31), 416.5, 416.8, 416.0, 416.2,  8_000, 416.5,  80),
    ]
    return spark.createDataFrame(rows, _silver_schema())


# ---------------------------------------------------------------------------
# aggregate_daily
# ---------------------------------------------------------------------------

def test_aggregate_daily_row_count(spark):
    df = _make_silver_rows(spark)
    result = aggregate_daily(df)
    # One row per (composite_figi, event_date) — 2 symbols
    assert result.count() == 2


def test_aggregate_daily_open_is_first_bar(spark):
    df = _make_silver_rows(spark)
    result = aggregate_daily(df)
    aapl = result.filter("symbol = 'AAPL'").collect()[0].asDict()
    # First bar (13:30) open = 310.0
    assert aapl["open_price"] == pytest.approx(310.0)


def test_aggregate_daily_close_is_last_bar(spark):
    df = _make_silver_rows(spark)
    result = aggregate_daily(df)
    aapl = result.filter("symbol = 'AAPL'").collect()[0].asDict()
    # Last bar (13:32) close = 311.2
    assert aapl["close_price"] == pytest.approx(311.2)


def test_aggregate_daily_high_low(spark):
    df = _make_silver_rows(spark)
    result = aggregate_daily(df)
    aapl = result.filter("symbol = 'AAPL'").collect()[0].asDict()
    assert aapl["high_price"] == pytest.approx(312.0)   # max of 311, 312, 311.5
    assert aapl["low_price"]  == pytest.approx(309.5)   # min of 309.5, 310, 310.8


def test_aggregate_daily_volume_sum(spark):
    df = _make_silver_rows(spark)
    result = aggregate_daily(df)
    aapl = result.filter("symbol = 'AAPL'").collect()[0].asDict()
    assert aapl["volume"] == 30_000   # 10k + 15k + 5k


def test_aggregate_daily_vwap_volume_weighted(spark):
    df = _make_silver_rows(spark)
    result = aggregate_daily(df)
    aapl = result.filter("symbol = 'AAPL'").collect()[0].asDict()
    expected_vwap = (310.3 * 10_000 + 311.0 * 15_000 + 311.1 * 5_000) / 30_000
    assert aapl["vwap"] == pytest.approx(expected_vwap, rel=1e-4)


def test_aggregate_daily_bar_count(spark):
    df = _make_silver_rows(spark)
    result = aggregate_daily(df)
    aapl = result.filter("symbol = 'AAPL'").collect()[0].asDict()
    assert aapl["bar_count"] == 3


def test_aggregate_daily_first_last_bar(spark):
    df = _make_silver_rows(spark)
    result = aggregate_daily(df)
    aapl = result.filter("symbol = 'AAPL'").collect()[0].asDict()
    assert aapl["first_bar_start"] == _ts(13, 30).replace(tzinfo=None)
    assert aapl["last_bar_start"]  == _ts(13, 32).replace(tzinfo=None)


def test_aggregate_daily_total_trades(spark):
    df = _make_silver_rows(spark)
    result = aggregate_daily(df)
    aapl = result.filter("symbol = 'AAPL'").collect()[0].asDict()
    assert aapl["total_trades"] == 300   # 100 + 150 + 50


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

def test_minute_bars_ddl_delta(spark):
    ddl = minute_bars_ddl("main.mkt.gold_minute_bars")
    assert "USING DELTA" in ddl
    assert "PARTITIONED BY (event_date)" in ddl
    assert "main.mkt.gold_minute_bars" in ddl


def test_daily_rollup_ddl_delta(spark):
    ddl = daily_rollup_ddl("main.mkt.gold_daily_rollup")
    assert "USING DELTA" in ddl
    assert "PARTITIONED BY (event_date)" in ddl
    assert "vwap" in ddl
    assert "bar_count" in ddl


def test_merge_keys_in_gold_minute_columns(spark):
    for k in MINUTE_MERGE_KEYS:
        assert k in GOLD_MINUTE_COLUMNS

def test_daily_merge_keys_correct(spark):
    assert DAILY_MERGE_KEYS == ["composite_figi", "event_date"]
