"""
Post-pipeline data quality checks with Slack alerting.

Run after dbt to check recon status and quality scores for today.
Alerts fire to Slack if thresholds are breached.

Usage:
    python scripts/check_data_quality.py                  # check today
    python scripts/check_data_quality.py --date 2026-05-27
    python scripts/check_data_quality.py --dry-run        # print results, skip alerts
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date

from dotenv import load_dotenv

load_dotenv()

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from market_streaming.observability.alerts import (
    alert_quality_drop,
    alert_recon_mismatch,
)
from market_streaming.sync.snowflake_writer import connect_from_env


def _get_connection():
    return connect_from_env(database="MARKET_STREAMING")


def _query(conn, sql: str) -> list:
    cur = conn.cursor()
    try:
        cur.execute(sql)
        return cur.fetchall()
    finally:
        cur.close()


def check_recon(conn, check_date: str, dry_run: bool) -> bool:
    rows = _query(conn, f"""
        SELECT recon_status, COUNT(*) AS cnt
        FROM MARKET_STREAMING.MARTS.MART_RECON__DAILY_DELTA
        WHERE price_date = '{check_date}'
        GROUP BY recon_status
        ORDER BY cnt DESC
    """)

    if not rows:
        print(f"[recon] no data for {check_date}")
        return True

    total = sum(r[1] for r in rows)
    ok_count = sum(r[1] for r in rows if r[0] == "OK")
    mismatch_count = total - ok_count
    mismatch_pct = mismatch_count / total * 100 if total > 0 else 0

    breakdown = ", ".join(f"{r[0]}={r[1]}" for r in rows)
    print(f"[recon] {check_date}: {breakdown} — mismatch: {mismatch_pct:.1f}%")

    if mismatch_pct > 5 and not dry_run:
        alert_recon_mismatch(mismatch_pct, check_date, breakdown)
        return False
    return True


def check_quality(conn, check_date: str, dry_run: bool) -> bool:
    rows = _query(conn, f"""
        SELECT
            ROUND(AVG(quality_score), 1) AS avg_score,
            COUNT(*) AS symbols,
            SUM(total_invalid_bars) AS invalid_bars
        FROM MARKET_STREAMING.OBSERVABILITY.MART_OPS__DATA_QUALITY
        WHERE event_date = '{check_date}'
    """)

    if not rows or rows[0][0] is None:
        print(f"[quality] no data for {check_date}")
        return True

    avg_score, symbols, invalid_bars = rows[0]
    print(f"[quality] {check_date}: avg_score={avg_score}, "
          f"symbols={symbols}, invalid_bars={invalid_bars}")

    if avg_score < 90 and not dry_run:
        alert_quality_drop(avg_score, check_date)
        return False
    return True


def main() -> int:
    p = argparse.ArgumentParser(description="Post-pipeline data quality checks")
    p.add_argument("--date", default=date.today().isoformat(),
                   help="Trading date to check (YYYY-MM-DD)")
    p.add_argument("--dry-run", action="store_true",
                   help="Print results without sending alerts")
    args = p.parse_args()

    conn = _get_connection()
    try:
        recon_ok = check_recon(conn, args.date, args.dry_run)
        quality_ok = check_quality(conn, args.date, args.dry_run)
    finally:
        conn.close()

    if recon_ok and quality_ok:
        print(f"[ok] all checks passed for {args.date}")
        return 0
    else:
        print(f"[alert] issues detected for {args.date}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
