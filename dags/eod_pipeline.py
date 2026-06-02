"""
End-of-day market data pipeline DAG.

Orchestrates the nightly chain that depends on the companion batch pipeline:

    1. Wait for the batch pipeline (other repo) to publish today's row in
       BigQuery `fct_daily_ohlcv`.
    2. Bridge daily prices + returns from BigQuery into Snowflake RECON.
    3. Build the streaming warehouse with dbt — recon, analytics, fundamentals,
       observability marts all refresh.
    4. Notify Slack on completion.

Schedule: weekdays at 22:00 UTC (18:00 ET, ~2 hours after market close so the
batch pipeline has time to publish before the sensor starts polling).

Designed to run on Astronomer (Astro Cloud / Astro CLI). The Dockerfile at the
repo root copies `src/`, `scripts/`, and `warehouse/` into
`/usr/local/airflow/include/` and pip-installs the `market_streaming` package
with `[recon]` extras (dbt-core, dbt-snowflake, snowflake-connector-python).

Required Airflow Variables:
    gcp_project       — GCP project hosting the batch pipeline's BigQuery
    bq_dataset        — BigQuery dataset containing `fct_daily_ohlcv`

Required Airflow Connections:
    google_cloud_default — service account with BQ read on the batch dataset
    slack_default        — Slack incoming webhook (HTTP conn type, password = URL)

Required environment variables (set in the Astro deployment):
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
    SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA
    GOOGLE_CLOUD_PROJECT, BQ_DATASET_ID
    GOOGLE_APPLICATION_CREDENTIALS_JSON  — raw service-account JSON string
"""
from __future__ import annotations

from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
)
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator


# ---------------------------------------------------------------------------
# Astro paths — set by the Dockerfile in this repo's root.
# `include/` is the Astro convention for non-DAG project files mounted on
# every worker.
# ---------------------------------------------------------------------------
INCLUDE_DIR   = "/usr/local/airflow/include"
SCRIPTS_DIR   = f"{INCLUDE_DIR}/scripts"
WAREHOUSE_DIR = f"{INCLUDE_DIR}/warehouse"


# ---------------------------------------------------------------------------
# Shared bash preamble — materialize the GCP service-account JSON env var
# into a file (Google clients expect a file path in GOOGLE_APPLICATION_CREDENTIALS).
# ---------------------------------------------------------------------------
GCP_CRED_PREAMBLE = dedent("""
    set -euo pipefail
    SA_FILE=$(mktemp /tmp/gcp-sa.XXXX.json)
    printf '%s' "$GOOGLE_APPLICATION_CREDENTIALS_JSON" > "$SA_FILE"
    export GOOGLE_APPLICATION_CREDENTIALS="$SA_FILE"
    trap 'rm -f "$SA_FILE"' EXIT
""").strip()


# ---------------------------------------------------------------------------
# Failure handler — one Slack post per task failure with a link to the log.
# Uses the Hook directly (not the Operator) because callbacks in Airflow 3.x
# run outside the Task Runner context — calling Operator.execute() from a
# callback emits a warning even though it still works.
# ---------------------------------------------------------------------------
def slack_on_failure(context: dict) -> None:
    from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

    ti = context["task_instance"]
    msg = dedent(f"""
        :x: *EOD pipeline task failed*
        DAG: `{ti.dag_id}`
        Task: `{ti.task_id}`
        Run: `{context["run_id"]}`
        Logs: {ti.log_url}
    """).strip()
    SlackWebhookHook(slack_webhook_conn_id="slack_default").send_text(msg)


default_args = {
    "owner":                      "data-platform",
    "retries":                    2,
    "retry_delay":                timedelta(minutes=5),
    "retry_exponential_backoff":  True,
    "max_retry_delay":            timedelta(minutes=30),
    "email_on_failure":           False,
    "on_failure_callback":        slack_on_failure,
}


with DAG(
    dag_id          = "market_streaming_eod",
    description     = "End-of-day BQ bridge + dbt build for the streaming warehouse",
    default_args    = default_args,
    schedule        = "0 22 * * 1-5",        # 22:00 UTC = 18:00 ET, weekdays
    start_date      = datetime(2026, 5, 1),
    catchup         = False,
    max_active_runs = 1,
    tags            = ["eod", "snowflake", "dbt", "recon", "market-streaming"],
    doc_md          = __doc__,
) as dag:

    # ── 1. Wait for batch pipeline to publish today's row ──────────────────
    # The batch pipeline writes daily_bars (NOT partitioned). We use a SQL
    # existence check instead of a partition sensor because the table has
    # no BQ partition metadata. The query is parameterised by ds so it
    # tracks today's logical date.
    #
    # Sensor pokes every 5 min, reschedules between polls (doesn't block
    # a worker slot), times out after 4 hours.
    wait_for_batch = BigQueryCheckOperator(
        task_id     = "wait_for_batch_pipeline",
        gcp_conn_id = "google_cloud_default",
        use_legacy_sql = False,
        sql         = (
            "SELECT COUNT(*) > 0 "
            "FROM `{{ var.value.gcp_project }}.{{ var.value.bq_dataset }}.daily_bars` "
            "WHERE price_date = '{{ ds }}'"
        ),
        retries          = 24,            # 24 retries × 5 min = 2 hours of polling
        retry_delay      = timedelta(minutes=5),
        retry_exponential_backoff = False,
    )

    # ── 2. Bridge BQ → Snowflake (parallel) ────────────────────────────────
    # Both bridges hit the same BQ project but write to different Snowflake
    # tables. Independent — run them in parallel to halve total time.
    bridge_daily_prices = BashOperator(
        task_id      = "bridge_daily_prices",
        bash_command = (
            f"{GCP_CRED_PREAMBLE}\n"
            f"python {SCRIPTS_DIR}/bq_to_snowflake_batch.py --date {{{{ ds }}}}"
        ),
        env          = {"PYTHONIOENCODING": "utf-8"},
        append_env   = True,
    )

    bridge_returns = BashOperator(
        task_id      = "bridge_returns",
        bash_command = (
            f"{GCP_CRED_PREAMBLE}\n"
            f"python {SCRIPTS_DIR}/bq_to_snowflake_returns.py"
        ),
        env          = {"PYTHONIOENCODING": "utf-8"},
        append_env   = True,
    )

    # ── 3. dbt build (fans out to all marts) ───────────────────────────────
    dbt_deps = BashOperator(
        task_id      = "dbt_deps",
        bash_command = f"cd {WAREHOUSE_DIR} && dbt deps",
        retries      = 1,
    )

    dbt_build = BashOperator(
        task_id      = "dbt_build",
        bash_command = f"cd {WAREHOUSE_DIR} && dbt build --fail-fast",
        retries      = 1,
    )

    # ── 4. Success notification ────────────────────────────────────────────
    notify_success = SlackWebhookOperator(
        task_id              = "notify_success",
        slack_webhook_conn_id = "slack_default",
        message              = (
            ":white_check_mark: *EOD pipeline complete* for `{{ ds }}` — "
            "recon + analytics marts refreshed."
        ),
    )

    # ── DAG topology ───────────────────────────────────────────────────────
    wait_for_batch >> [bridge_daily_prices, bridge_returns] >> dbt_deps >> dbt_build >> notify_success
