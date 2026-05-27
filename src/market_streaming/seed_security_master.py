"""
Snapshot the batch pipeline's current-valid security master rows to a local
Parquet seed. Streaming Silver joins on `symbol` against this seed to attach
`composite_figi`, the identity used by every downstream layer and by recon.

Run daily before market open. Idempotent — overwrites the seed in place.

Reads from `int_security_master_historical` rather than `int_security_master_scd2`
because the historical model includes a manual-FIGI CSV seed for tickers that
Polygon's reference endpoint doesn't return a composite_figi for (ACN, BK, LIN,
MDT and similar). The SCD2 model still filters those out at source, so it gives
~96% coverage; the historical model gives full 104/104.

For real-time events, event_time ≈ now, so only currently-valid rows are
needed. Historical replay against this seed would mis-key trades around ticker
renames; that case is out of scope for v1.
"""
from __future__ import annotations

import sys

from google.cloud import bigquery

from market_streaming.config import (
    SECURITY_MASTER_SEED_PATH,
    SEED_DIR,
    load_symbols,
    require_env,
)


def build_query(project: str, dataset: str, symbols: list[str]) -> str:
    """Query int_security_master_historical for the currently-valid row per ticker.

    The historical model unions a manual CSV seed (covers tickers Polygon's
    reference endpoint doesn't return a composite_figi for — e.g. ACN, BK, LIN,
    MDT) with the dbt snapshot. Its schema is simpler than the SCD2 model:
    plain `valid_from` / `valid_to` (no dbt_ prefix) and no `active` flag.
    Current row is the one with NULL valid_to. DISTINCT guards against any
    overlap between seed and snapshot for the same composite_figi at the same
    moment in time.
    """
    symbol_list = ", ".join(f"'{s}'" for s in symbols)
    return f"""
        SELECT DISTINCT
            composite_figi,
            ticker AS symbol,
            name,
            valid_from,
            valid_to,
            source
        FROM `{project}.{dataset}.int_security_master_historical`
        WHERE valid_to IS NULL
          AND ticker IN ({symbol_list})
    """


def main() -> int:
    project = require_env("GOOGLE_CLOUD_PROJECT")
    dataset = require_env("BQ_DATASET_ID")
    require_env("GOOGLE_APPLICATION_CREDENTIALS")

    symbols = load_symbols()
    if not symbols:
        print("symbols.txt is empty; nothing to seed.", file=sys.stderr)
        return 1

    client = bigquery.Client(project=project)
    query = build_query(project, dataset, symbols)
    df = client.query(query).result().to_dataframe()

    missing = set(symbols) - set(df["symbol"].str.upper())
    if missing:
        print(
            f"warning: {len(missing)} symbol(s) not present in batch historical security master: "
            f"{sorted(missing)}",
            file=sys.stderr,
        )

    SEED_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(SECURITY_MASTER_SEED_PATH, index=False)
    print(f"wrote {len(df)} rows -> {SECURITY_MASTER_SEED_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
