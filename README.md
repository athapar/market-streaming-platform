# Real-Time Market Data Lakehouse

A streaming lakehouse layer extending the [batch financial data pipeline](https://github.com/athapar/financial-data-pipeline) with real-time ingestion, Delta Lake medallion storage, and end-of-day reconciliation against the batch fact tables.

Where the batch pipeline answers *"what were the correct daily prices?"* deterministically from REST snapshots, this pipeline answers *"what is happening right now, and does it agree with the batch system at end-of-day?"* The reconciliation contract between the two execution models is the central correctness story.

> **Status:** Phase 0 — scaffolding. See [`implementation.md`](./implementation.md) for the phased build plan and [`spec.md`](./spec.md) for the full design.

## Architecture (target)

```
Polygon WebSocket (AM minute aggregates, delayed ~15 min)
      |
      v
Python producer (local) ----> Confluent Kafka (market.aggregates)
                                         |
                                         v
                              Databricks Spark Structured Streaming
                                         |
                              Bronze Delta  ->  Silver Delta  ->  Gold Delta (minute + daily bars)
                                                                       |
                                                                       v
                                                                  Snowflake
                                                                       |
                                                                       v
                                                  dbt recon  vs  batch fact_daily_prices
```

> Phase 1 ships with Polygon's `AM.*` (minute aggregate) channel only — that's what the current Stocks Starter plan includes. Routing is structured so trade/quote channels can be re-enabled with a one-line change after a plan upgrade.

## Identity carries over from batch

- `composite_figi` (not ticker) is the join key everywhere downstream of Silver.
- The streaming Silver layer attaches `composite_figi` via a daily snapshot of the batch `int_security_master_scd2` table, exported as a Parquet seed (`data/seed/security_master_current.parquet`).
- This is what makes end-of-day recon meaningful across ticker renames.

## Quick start (Phase 0)

1. Copy `.env.example` to `.env` and fill in credentials.
2. Refresh the security master seed from the batch pipeline's BigQuery dataset:
   ```bash
   python -m market_streaming.seed_security_master
   ```
3. Verify the seed:
   ```bash
   python -c "import pandas as pd; print(pd.read_parquet('data/seed/security_master_current.parquet').head())"
   ```

Producer, streaming jobs, and recon are added in subsequent phases.

## Repository layout

```
src/market_streaming/
  config.py                    # env loading, path constants (mirrors batch repo)
  seed_security_master.py      # BigQuery -> Parquet snapshot of SCD2 current rows
notebooks/                     # Databricks streaming jobs (bronze / silver / gold)
warehouse/                     # dbt project (recon: streaming vs batch)
docs/                          # architecture, failure modes, recon report
data/
  seed/                        # security master snapshot
  spillover/                   # producer-side Kafka spillover (Phase 1)
symbols.txt                    # streaming universe (start small)
spec.md                        # full design spec
implementation.md              # phased build plan
```
