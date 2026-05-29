# Astro Runtime 3.2-4 (Airflow 3.2.1) — matches the deployment created via
# `astro deployment create`. Bump when Astro publishes a newer runtime.
FROM quay.io/astronomer/astro-runtime:3.2-4

# ── Source for the market_streaming pip install ────────────────────────────
# Copied to /tmp (writable by the astro user) so setuptools can create its
# build/ directory during wheel build. Removed after install (as root, since
# build-time artifacts are owned by root).
USER root
COPY --chown=astro:0 pyproject.toml /tmp/project/pyproject.toml
COPY --chown=astro:0 README.md      /tmp/project/README.md
COPY --chown=astro:0 src/           /tmp/project/src/

# ── Runtime files referenced by the DAG's BashOperator tasks ───────────────
# Bridge scripts + dbt project live under include/ where Astro auto-mounts
# them on every worker.
COPY --chown=astro:0 scripts/   /usr/local/airflow/include/scripts/
COPY --chown=astro:0 warehouse/ /usr/local/airflow/include/warehouse/
USER astro

# Install the project + reconciliation extras (dbt-core, dbt-snowflake,
# snowflake-connector-python). google-cloud-bigquery + python-dotenv come from
# the core dependencies declared in pyproject.toml.
RUN pip install --no-cache-dir "/tmp/project[recon]"

# Cleanup must run as root because setuptools and pip leave behind build-time
# artifacts (egg-info, __pycache__) owned by root.
USER root
RUN rm -rf /tmp/project
USER astro
