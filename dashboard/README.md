# Dashboard

Multi-page Streamlit app over the Snowflake `MARKET_STREAMING` warehouse.
Reads from the streaming Gold layer, the cross-pipeline RECON bridge, and
the dbt-built `MARTS / ANALYTICS / OBSERVABILITY / FUNDAMENTALS` schemas.

## Pages

| # | Page | Source schemas |
|--:|---|---|
| 1 | Pipeline Health   | `OBSERVABILITY`               |
| 2 | Market Overview   | `ANALYTICS`                   |
| 3 | Risk Analytics    | `ANALYTICS`                   |
| 4 | Data Quality      | `OBSERVABILITY`, `MARTS`      |
| 5 | Microstructure    | `ANALYTICS`                   |
| 6 | Fundamentals      | `FUNDAMENTALS`                |
| 7 | Dividends         | `FUNDAMENTALS`                |
| 8 | Reconciliation    | `MARTS` (recon)               |

## Run locally

```bash
pip install -r dashboard/requirements.txt

# credentials: either a .env at repo root OR dashboard/.streamlit/secrets.toml
cp dashboard/.streamlit/secrets.toml.example dashboard/.streamlit/secrets.toml
# ... fill in the [snowflake] section ...

streamlit run dashboard/app.py
```

## Deploy to Streamlit Community Cloud

1. Push the repo to a public GitHub repository.
2. Sign in at https://share.streamlit.io and click **New app**.
3. Configure:
   - **Repository:** `<you>/market-streaming-pipeline`
   - **Branch:** `main`
   - **Main file path:** `dashboard/app.py`
   - **Python version:** 3.11 or 3.12
4. Under **Advanced settings → Secrets**, paste the same TOML block as
   `dashboard/.streamlit/secrets.toml.example`, with real values:
   ```toml
   [snowflake]
   SNOWFLAKE_ACCOUNT   = "myorg-ab12345"
   SNOWFLAKE_USER      = "STREAMING_DASHBOARD_USER"
   SNOWFLAKE_PASSWORD  = "..."
   SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
   SNOWFLAKE_ROLE      = "SYSADMIN"
   ```
5. Click **Deploy**. Streamlit Cloud picks up `dashboard/requirements.txt`
   automatically. First build takes ~3 minutes; subsequent re-deploys are
   incremental and fire on every push to `main`.

## Snowflake role for the dashboard

Recommended: create a read-only role rather than reusing the bridge / sync
role. The dashboard only needs `USAGE` on the database/schemas and `SELECT`
on the tables:

```sql
CREATE ROLE IF NOT EXISTS STREAMING_DASHBOARD_RO;

GRANT USAGE  ON DATABASE MARKET_STREAMING                TO ROLE STREAMING_DASHBOARD_RO;
GRANT USAGE  ON ALL SCHEMAS   IN DATABASE MARKET_STREAMING TO ROLE STREAMING_DASHBOARD_RO;
GRANT USAGE  ON FUTURE SCHEMAS IN DATABASE MARKET_STREAMING TO ROLE STREAMING_DASHBOARD_RO;
GRANT SELECT ON ALL TABLES     IN DATABASE MARKET_STREAMING TO ROLE STREAMING_DASHBOARD_RO;
GRANT SELECT ON FUTURE TABLES  IN DATABASE MARKET_STREAMING TO ROLE STREAMING_DASHBOARD_RO;
GRANT SELECT ON ALL VIEWS      IN DATABASE MARKET_STREAMING TO ROLE STREAMING_DASHBOARD_RO;
GRANT SELECT ON FUTURE VIEWS   IN DATABASE MARKET_STREAMING TO ROLE STREAMING_DASHBOARD_RO;

GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE STREAMING_DASHBOARD_RO;
GRANT ROLE STREAMING_DASHBOARD_RO   TO USER STREAMING_DASHBOARD_USER;
```

The dashboard's `lru_cache(maxsize=1)` on connection params plus
`st.cache_data(ttl=300)` on every query keeps warehouse credit usage low —
each page loads ~1 query, cached for 5 minutes.

## Refresh cadence

The dashboard reads whatever's in Snowflake at query time. Upstream refresh:

| Cadence | Step |
|---|---|
| Per Spark batch | `notebooks/snowflake_sync.py` syncs Gold tables |
| Per trading day, after close | `scripts/bq_to_snowflake_batch.py` · `bq_to_snowflake_returns.py` |
| Quarterly (or as-needed) | `scripts/bq_to_snowflake_fundamentals.py` · `bq_to_snowflake_dividends.py` |
| After any bridge run | `cd warehouse && dbt run` to refresh marts |
