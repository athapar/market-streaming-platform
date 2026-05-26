# Market Streaming Pipeline

A production-grade real-time market data pipeline built as a technical sequel to a [batch financial data pipeline](https://github.com/athapar/financial-data-pipeline-project). Where the batch pipeline runs nightly ETL in BigQuery and dbt, this project ingests live Polygon.io WebSocket data, streams it through Apache Kafka and Spark Structured Streaming, writes a medallion Delta Lake in Databricks, syncs a Snowflake Gold layer, and reconciles the two pipelines' daily price outputs with dbt.

The design choices throughout prioritise operational correctness over novelty: exactly-once semantics, idempotent writes, surgical deduplication, and a clear audit trail from raw event to analytical table.

---

## Architecture

```
Polygon WebSocket (AM minute aggregates)
        │
        ▼
  Kafka Producer  ──────────────────────────────────────────────┐
  (Python / confluent-kafka)                                    │ delivery failure
  ├── at-least-once delivery                                    ▼
  ├── NDJSON spillover on broker unavailability       data/spillover/*.ndjson
  └── reconnect loop with gap logging                 (replay_spillover.py)
        │
        ▼
  Confluent Kafka  (cloud-managed)
        │
        ▼
  Databricks  —  Spark Structured Streaming + Delta Lake (Unity Catalog)
  │
  ├── Bronze   append-only · raw STRING payload · Kafka offset audit
  │            checkpoint → exactly-once-from-Kafka
  │
  ├── Silver   typed · deduped · FIGI-joined
  │            foreachBatch + Delta MERGE on (symbol, window_start)
  │            CDF enabled → Gold reads only net-new rows
  │
  └── Gold
        ├── gold_minute_bars    1 row per (composite_figi, minute)
        └── gold_daily_rollup   full-day OHLCV · recon join key
              │
              ▼
        Snowflake  MARKET_STREAMING.GOLD
              │
  ┌───────────┴───────────┐
  │                       │
  │              RECON.BATCH_DAILY_PRICES
  │              (batch BigQuery bridge —
  │               scripts/bq_to_snowflake_batch.py)
  │                       │
  └───────────┬───────────┘
              ▼
        dbt  (warehouse/)
        └── mart_recon__daily_delta
              Δclose · Δvwap · Δvolume · recon_status per (symbol, date)
```

A separate script (`scripts/bq_to_snowflake_batch.py`) bridges the batch pipeline: it queries BigQuery's `fct_daily_ohlcv` mart and loads it into `MARKET_STREAMING.RECON.BATCH_DAILY_PRICES`. dbt then joins the two sources on `(composite_figi, price_date)` so a single mart row answers *how closely did the streaming pipeline track the batch ground-truth for each symbol on each day*.

---

## Repository Layout

```
market-streaming-pipeline/
├── src/market_streaming/
│   ├── config.py                      # env var loading, symbols, path constants
│   ├── seed_security_master.py        # BigQuery SCD2 → Parquet FIGI seed
│   ├── producer/
│   │   ├── main.py                    # CLI entry point (--dry-run supported)
│   │   ├── polygon_ws.py              # WebSocket reconnect loop + gap logging
│   │   ├── envelope.py                # typed event wrapper
│   │   ├── kafka_sink.py              # KafkaSink / DryRunSink
│   │   ├── spillover.py               # NDJSON fallback store + replay utilities
│   │   └── metrics.py                 # event counters, periodic heartbeat
│   ├── bronze/transforms.py           # Kafka → raw Delta (append-only)
│   ├── silver/transforms.py           # parse, dedup, FIGI join → Delta MERGE
│   ├── gold/transforms.py             # minute bars + daily rollup → Delta MERGE
│   └── sync/snowflake_writer.py       # Gold Delta → Snowflake (executemany)
│
├── notebooks/
│   ├── bronze_ingest.py               # Databricks: Bronze stream
│   ├── silver_ingest.py               # Databricks: Silver stream
│   ├── gold_ingest.py                 # Databricks: Gold stream
│   └── snowflake_sync.py              # Databricks: Gold → Snowflake
│
├── scripts/
│   ├── bq_to_snowflake_batch.py       # BigQuery daily prices → RECON schema
│   └── replay_spillover.py            # re-publish NDJSON spillover files
│
├── warehouse/                         # dbt project  (profile: warehouse)
│   ├── dbt_project.yml
│   ├── profiles.example.yml
│   └── models/
│       ├── staging/
│       │   ├── sources.yml            # GOLD + RECON source declarations
│       │   ├── stg_streaming__daily_rollup.sql
│       │   └── stg_batch__daily_prices.sql
│       ├── intermediate/
│       │   └── int_recon__daily_aligned.sql   # full outer join on (figi, date)
│       └── marts/
│           ├── mart_recon__daily_delta.sql    # Δclose, Δvwap, recon_status
│           └── schema.yml
│
├── tests/
│   ├── test_config.py
│   └── test_spillover.py              # NDJSON round-trip, gap log, replay
│
├── symbols.txt                        # AAPL MSFT NVDA SPY QQQ
└── pyproject.toml
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Market data source | Polygon.io WebSocket — AM (minute aggregate) channel |
| Message broker | Confluent Kafka (cloud-managed) |
| Stream processing | Apache Spark Structured Streaming on Databricks |
| Storage | Delta Lake — Unity Catalog (medallion: Bronze / Silver / Gold) |
| Analytical serving | Snowflake (`MARKET_STREAMING` database) |
| Transformation / recon | dbt Core + dbt-snowflake |
| Batch pipeline bridge | Google BigQuery (`fct_daily_ohlcv` → `RECON` schema) |
| Security master | OpenFIGI via batch pipeline SCD2 (`int_security_master_scd2`) |
| Language | Python 3.11+ |

---

## Key Design Decisions

### Exactly-once delivery
Kafka offsets are committed atomically with the Delta checkpoint. A process crash re-reads from the last committed Kafka offset and re-applies the batch; the downstream MERGE absorbs the duplicate idempotently. Bronze is append-only — it is the audit log and replay source if Silver or Gold need rebuilding.

### Spillover and gap logging
When the Kafka broker is unreachable the producer writes events to a date-partitioned NDJSON file (`data/spillover/YYYY-MM-DD.ndjson`). `replay_spillover.py` re-publishes those envelopes through the same code path on recovery. Disconnection windows are logged separately (`data/gaps/`) with timestamp, duration, and reason — an observability layer that requires no external infrastructure.

### Deduplication
Silver deduplicates within each micro-batch with `ROW_NUMBER OVER (PARTITION BY symbol, window_start ORDER BY ingest_timestamp DESC)` before the MERGE. This removes within-batch duplicates without a full table scan; the MERGE handles cross-batch duplicates at the Delta level. Exactly one AM bar per `(symbol, minute)` is guaranteed in Silver.

### FIGI join correctness
The security master seed (5 rows for the tracked symbols) is broadcast-joined in Silver so it never shuffles. A symbol absent from the seed gets `NULL composite_figi` — the row still lands, no silent data loss. The Gold primary key is `(composite_figi, window_start)`; NULL-FIGI rows are visible in Delta but excluded from the Snowflake sync by design.

### Snowflake timestamp compatibility
`write_pandas` stages data as Parquet via Arrow, which serialises timestamps as `int64` epoch microseconds. Snowflake reads that as `NUMBER(38,0)` and rejects it against `TIMESTAMP_NTZ` columns regardless of pandas dtype — a version-sensitive Arrow/connector compatibility issue confirmed across multiple patch attempts. The fix: `sync_table` collects the Spark DataFrame to Python `Row` objects and inserts via `cursor.executemany()` with native `datetime.datetime` instances, bypassing Arrow entirely. For the data volumes here (O(700 rows/day)) this is fast and correct; moving to a bulk-load path later is a one-function change.

### Gold daily rollup correctness
Gold's `foreachBatch` re-reads all Silver rows for each affected date rather than accumulating incrementally. This makes the daily rollup correct under late arrivals: a bar arriving two batches late corrects the open/close because `min_by`/`max_by` over the full Silver partition is deterministic. The tradeoff is a Silver scan per batch, which is acceptable for the current data volume.

### Serverless trigger compatibility
Databricks Serverless (Free Edition) does not support infinite `processingTime` streaming. All three streaming layers default to `trigger(availableNow=True)`, which processes all accumulated changes then exits. Re-running the notebook catches up to current state. The trigger type is a widget parameter — switching to `processingTime` on a Classic cluster requires no code change.

### Recon status taxonomy
The dbt mart distinguishes structurally expected differences from genuine price disagreements:

| Status | Meaning |
|---|---|
| `OK` | Both sources, full session, within tolerance |
| `CLOSE_MISMATCH` | `\|Δclose\|` > 0.5% on a full-session row |
| `VWAP_MISMATCH` | `\|Δvwap\|` > 1.0% on a full-session row |
| `CLOSE_AND_VWAP_MISMATCH` | Both thresholds breached |
| `PARTIAL_SESSION` | Streaming captured a sub-full-day window; volume and VWAP deltas are expected, not flagged as errors |
| `MISSING_STREAMING` | Producer was not running for this (symbol, date) |
| `MISSING_BATCH` | Batch pipeline did not produce data for this date |

---

## Running Locally

### Prerequisites

- Python 3.11+
- A `.env` file with credentials (see *Environment Variables* below)
- Confluent Kafka cluster (bootstrap servers + API key/secret)
- Polygon.io API key (Stocks Starter plan or above)
- Google Cloud project with BigQuery access (for FIGI seed + batch bridge)
- Snowflake account

### Install

```bash
python -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -e ".[producer,recon]"
```

### 1. Seed the security master

Pull the current `composite_figi` mapping from the batch pipeline's BigQuery SCD2 table and write a local Parquet seed:

```bash
python -m market_streaming.seed_security_master
# writes  data/seed/security_master_current.parquet
```

Upload this file to your Databricks workspace so the Silver notebook can broadcast-join it.

### 2. Run the producer (market hours)

```bash
# dry run — receive events, print samples, skip Kafka
python -m market_streaming.producer.main --dry-run

# live run — publish AM aggregates to Kafka
python -m market_streaming.producer.main
```

The producer subscribes to `AM.*` for each symbol in `symbols.txt`. Ctrl-C flushes the Kafka producer; any unflushed messages land in `data/spillover/`.

### 3. Databricks notebooks — run in order

| Notebook | Reads from | Writes to |
|---|---|---|
| `bronze_ingest.py` | Confluent Kafka | `bronze_market_events` Delta |
| `silver_ingest.py` | Bronze Delta | `silver_market_events` Delta |
| `gold_ingest.py` | Silver CDF | `gold_minute_bars` · `gold_daily_rollup` Delta |
| `snowflake_sync.py` | Gold Delta | `MARKET_STREAMING.GOLD.*` Snowflake |

Each notebook uses `sys.path.insert` to load the package from the workspace repo path. Run with `trigger_type = availableNow` on Serverless; switch to `processingTime` on a Classic cluster.

### 4. Batch bridge (after market close)

```bash
python scripts/bq_to_snowflake_batch.py --date YYYY-MM-DD
# --dry-run  fetches from BQ and prints rows; skips the Snowflake write
```

### 5. dbt recon

```bash
cd warehouse
cp profiles.example.yml ~/.dbt/profiles.yml   # fill in env vars

dbt run      # builds staging views, intermediate view, mart table
dbt test     # not_null + accepted_values on recon_status
dbt show --select mart_recon__daily_delta
```

---

## Environment Variables

| Variable | Used by | Description |
|---|---|---|
| `POLYGON_API_KEY` | producer | Polygon.io API key |
| `KAFKA_BOOTSTRAP_SERVERS` | producer, bronze | Confluent bootstrap URL |
| `KAFKA_SASL_USERNAME` | producer, bronze | Confluent API key |
| `KAFKA_SASL_PASSWORD` | producer, bronze | Confluent API secret |
| `KAFKA_TOPIC` | producer, bronze | Topic name (default: `market-events`) |
| `GOOGLE_CLOUD_PROJECT` | seed, recon bridge | GCP project ID |
| `BQ_DATASET_ID` | seed, recon bridge | BigQuery dataset |
| `GOOGLE_APPLICATION_CREDENTIALS` | seed, recon bridge | Path to service account JSON |
| `BQ_MART_TABLE` | recon bridge | Batch mart table (default: `fct_daily_ohlcv`) |
| `SNOWFLAKE_ACCOUNT` | recon bridge, dbt | Snowflake account identifier (e.g. `myorg-ab12345`) |
| `SNOWFLAKE_USER` | recon bridge, dbt | Snowflake user |
| `SNOWFLAKE_PASSWORD` | recon bridge, dbt | Snowflake password |
| `SNOWFLAKE_WAREHOUSE` | recon bridge, dbt | Compute warehouse |
| `SNOWFLAKE_ROLE` | recon bridge, dbt | Role (optional) |

Databricks notebooks read Snowflake credentials from a `market-streaming` Databricks secret scope — see `notebooks/snowflake_sync.py` for the one-time `databricks secrets put-secret` commands.

---

## Snowflake Objects

```
MARKET_STREAMING
├── GOLD
│   ├── GOLD_MINUTE_BARS       PK (composite_figi, window_start)
│   └── GOLD_DAILY_ROLLUP      PK (composite_figi, event_date)
└── RECON
    └── BATCH_DAILY_PRICES     PK (composite_figi, price_date)

dbt output  (schema prefix configurable in profiles.yml)
├── STAGING
│   ├── STG_STREAMING__DAILY_ROLLUP
│   └── STG_BATCH__DAILY_PRICES
├── INTERMEDIATE
│   └── INT_RECON__DAILY_ALIGNED
└── MARTS
    └── MART_RECON__DAILY_DELTA
```

---

## Tests

```bash
pytest tests/ -v
```

`test_config.py` — env var loading, symbol parsing.  
`test_spillover.py` — NDJSON round-trip, pending file discovery, gap record write, replay counter.

---

## Relation to the Batch Pipeline

This project extends the [batch financial data pipeline](https://github.com/athapar/financial-data-pipeline-project), which produces:

- A BigQuery SCD2 security master (`int_security_master_scd2`) — the source of `composite_figi`, used as the identity key across both pipelines.
- A daily OHLCV fact table (`fct_daily_ohlcv`) — the batch ground-truth that the recon mart compares against.

`composite_figi` is the bridge. Silver assigns it via broadcast join against a daily Parquet snapshot of the security master. The dbt recon mart joins both pipelines' daily outputs on `(composite_figi, price_date)`, measuring streaming coverage and price accuracy against the full-session batch run — and surviving ticker renames because the join key is a stable FIGI, not a symbol string.
