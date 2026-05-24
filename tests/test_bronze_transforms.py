import pytest

pyspark = pytest.importorskip("pyspark")

from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from market_streaming.bronze.transforms import (
    BRONZE_COLUMNS,
    bronze_ddl,
    enrich_bronze,
    kafka_jaas_config,
)


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder
        .master("local[1]")
        .appName("bronze-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield s
    s.stop()


def _kafka_source_schema() -> StructType:
    # Subset of the real Kafka source schema — enough for enrich_bronze.
    return StructType([
        StructField("key", BinaryType(), True),
        StructField("value", BinaryType(), True),
        StructField("topic", StringType(), True),
        StructField("partition", IntegerType(), True),
        StructField("offset", LongType(), True),
        StructField("timestamp", TimestampType(), True),
    ])


def test_enrich_bronze_projects_expected_columns(spark):
    payload = b'{"ev":"AM","sym":"AAPL","o":150.1,"c":150.2,"s":1700000000000}'
    row = (
        b"AAPL",
        payload,
        "market.aggregates",
        0,
        42,
        datetime(2026, 5, 23, 14, 32, tzinfo=timezone.utc),
    )
    df = spark.createDataFrame([row], _kafka_source_schema())

    result = enrich_bronze(df)
    assert set(result.columns) == set(BRONZE_COLUMNS)

    out = result.collect()[0].asDict()
    assert out["kafka_topic"] == "market.aggregates"
    assert out["kafka_partition"] == 0
    assert out["kafka_offset"] == 42
    assert out["raw_payload"] == payload.decode("utf-8")
    assert out["event_type"] == "AM"
    assert out["ingest_timestamp"] is not None
    assert out["ingest_date"] is not None


def test_enrich_bronze_event_type_null_for_garbage(spark):
    df = spark.createDataFrame(
        [(b"X", b"not even json", "market.aggregates", 0, 1,
          datetime(2026, 5, 23, tzinfo=timezone.utc))],
        _kafka_source_schema(),
    )
    out = enrich_bronze(df).collect()[0].asDict()
    # The row still lands (Bronze never drops a record); event_type is just NULL.
    assert out["event_type"] is None
    assert out["raw_payload"] == "not even json"


def test_kafka_jaas_config_format():
    s = kafka_jaas_config("api_key", "api_secret")
    assert s.startswith("org.apache.kafka.common.security.plain.PlainLoginModule required")
    assert 'username="api_key"' in s
    assert 'password="api_secret"' in s
    assert s.endswith(";")


def test_bronze_ddl_partitions_by_date_and_topic():
    ddl = bronze_ddl("main.streaming.bronze_market_events")
    assert "PARTITIONED BY (ingest_date, kafka_topic)" in ddl
    assert "USING DELTA" in ddl
    assert "main.streaming.bronze_market_events" in ddl
