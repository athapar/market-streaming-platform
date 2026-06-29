# Airflow DAGs (Astronomer / Astro Runtime)

Orchestration for the end-of-day chain, built for Astro Runtime (Airflow 3.2.1).
The intraday streaming layer stays on Databricks Workflows; the Kafka producer
stays on Windows Task Scheduler (local) or a small VM.

> **CI validation, not a live cloud deployment.** The DAGs are validated on
> every push by the `dag-validation` job in
> [`.github/workflows/ci.yml`](../.github/workflows/ci.yml), which runs
> `astro dev parse` against the real Astro Runtime image — building the project
> and running the DAG-integrity test with no Astro account or paid deployment.
> Deploying to Astro Cloud (steps below) is optional and only needed for an
> actually-scheduled, hosted run.

## DAGs

| DAG | Schedule | What it does |
|---|---|---|
| `market_streaming_eod` | 22:00 UTC weekdays | Wait for batch pipeline → bridge BQ to Snowflake (prices + returns in parallel) → dbt build → Slack notify |

## DAG topology

```
wait_for_batch_pipeline (BigQuery partition sensor, pokes every 5 min)
        │
        ├── bridge_daily_prices  (BQ → SF.RECON.BATCH_DAILY_PRICES)
        └── bridge_returns       (BQ → SF.RECON.BATCH_DAILY_RETURNS)
                │
                └── dbt_deps → dbt_build → notify_success
```

The sensor enforces the cross-pipeline dependency: this DAG won't bridge or
build until the batch pipeline has published today's row. No more
"1-hour buffer between Task Scheduler tasks and hope" — the sensor is precise
to within `poke_interval` (5 min) and won't hold a worker slot while waiting
(`mode='reschedule'`).

## One-night Astro setup

### 1. Astro account + CLI

```powershell
# Sign up at https://www.astronomer.io/  (free 14-day trial)
# Install the CLI on Windows via winget:
winget install -e --id Astronomer.Astro
astro version
```

### 2. Initialize the project (already done in this repo)

The repo root already contains the Astro project files:

```
Dockerfile          extends Astro Runtime, installs market_streaming[recon]
requirements.txt    apache-airflow-providers-google, -slack
.dockerignore       excludes venv, logs, dashboard, etc. from the image
dags/               DAG files (Astro auto-syncs this directory)
```

If `astro dev init` needs to regenerate any missing files (`.astro/`,
`airflow_settings.yaml`, `packages.txt`), run it at the repo root. It won't
overwrite the existing `Dockerfile` / `requirements.txt` / `dags/` if they
already exist.

### 3. Test locally (optional)

```powershell
astro dev start
# → Airflow UI at http://localhost:8080  (admin / admin)
```

The Variables/Connections from `airflow_settings.yaml` are auto-loaded into
local Airflow only. For cloud, configure them in the Astro UI (next step).

### 4. Create the Astro deployment

```powershell
astro login
astro deployment create
# → choose a name, region, resource size (start with the smallest)
```

### 5. Configure Variables, Connections, and env vars in the Astro UI

**Airflow Variables** (Deployment → Variables tab):
- `gcp_project` — your GCP project ID
- `bq_dataset` — BigQuery dataset containing `fct_daily_ohlcv`

**Airflow Connections** (Deployment → Connections tab):
- `google_cloud_default`
  - Conn type: Google Cloud
  - Keyfile JSON: paste your service-account JSON
- `slack_default`
  - Conn type: Slack Incoming Webhook
  - Password: the webhook URL

**Environment variables** (Deployment → Environment Variables tab, mark Secret):
- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PRIVATE_KEY_PATH` (one var for both bridges + dbt; `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` only if encrypted)
- `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_ROLE`
- `SNOWFLAKE_DATABASE` (= `MARKET_STREAMING`)
- `SNOWFLAKE_SCHEMA`   (= `RECON`)
- `GOOGLE_CLOUD_PROJECT`
- `BQ_DATASET_ID`
- `GOOGLE_APPLICATION_CREDENTIALS_JSON` — paste the raw JSON of the SA key

The DAG's BashOperator preamble writes that JSON to a temp file and points
`GOOGLE_APPLICATION_CREDENTIALS` at the path before invoking the bridge script.

### 6. Deploy

```powershell
astro deploy
```

This builds the Dockerfile (installing the project from source), pushes the
image to Astro's registry, and rolls out the DAG. First deploy: 3–5 min.
Subsequent deploys: faster (Docker layer cache).

### 7. Trigger the first run

In the Astro UI → DAGs → `market_streaming_eod` → unpause → "Trigger DAG".
Watch the sensor poll, the bridges fan out, dbt build, and Slack post.

## Caveats

- The DAG only runs successfully if the **batch pipeline has actually
  published today's row**. If the batch repo isn't scheduled to run, the
  sensor will time out at the 4-hour mark, the DAG fails, and Slack fires.
- Astro deployments on the free trial / hobby tier are billable after 14
  days. Check pricing before relying on long-term durability.
- The streaming layer (Databricks Workflows) and the producer (Task
  Scheduler / VM) are **not** in this DAG. They run on their own schedules
  and are deliberately decoupled — Airflow is for cross-system orchestration,
  not for managing Spark micro-batches or long-running WebSocket clients.
