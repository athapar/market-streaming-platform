"""
Slack webhook alerting for pipeline operational events.

Lightweight — no external dependencies beyond stdlib. Sends a single POST
to SLACK_WEBHOOK_URL with a color-coded attachment. If the env var is unset,
alerts are logged but not sent (graceful degradation for local dev).

Alert categories:
  - Producer: WebSocket disconnect/reconnect, Kafka failure spikes
  - Pipeline: data staleness, recon mismatch spikes, quality score drops
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from urllib.error import URLError
from urllib.request import Request, urlopen

log = logging.getLogger(__name__)

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

_SEVERITY_COLORS = {
    "critical": "#FF0000",
    "warning": "#FFA500",
    "info": "#36A64F",
}


def send_alert(
    title: str,
    message: str,
    severity: str = "warning",
    fields: dict[str, str] | None = None,
) -> bool:
    """Post an alert to Slack. Returns True if sent, False otherwise."""
    if not SLACK_WEBHOOK_URL:
        log.warning("SLACK_WEBHOOK_URL not set — alert suppressed: %s", title)
        return False

    color = _SEVERITY_COLORS.get(severity, "#808080")

    attachment: dict = {
        "color": color,
        "title": f"[{severity.upper()}] {title}",
        "text": message,
        "ts": int(datetime.now(timezone.utc).timestamp()),
        "footer": "market-streaming-platform",
    }

    if fields:
        attachment["fields"] = [
            {"title": k, "value": str(v), "short": True}
            for k, v in fields.items()
        ]

    payload = {"attachments": [attachment]}
    req = Request(
        SLACK_WEBHOOK_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )

    try:
        urlopen(req, timeout=5)
        log.info("slack alert sent: %s", title)
        return True
    except (URLError, OSError) as exc:
        log.error("failed to send slack alert: %s — %s", title, exc)
        return False


def alert_reconnect(
    disconnect_at: datetime,
    reconnect_at: datetime,
    reason: str,
) -> bool:
    duration = reconnect_at - disconnect_at
    return send_alert(
        title="WebSocket Reconnect",
        message=f"Producer reconnected after {duration.total_seconds():.1f}s gap.",
        severity="warning",
        fields={
            "Disconnected": disconnect_at.strftime("%H:%M:%S UTC"),
            "Reconnected": reconnect_at.strftime("%H:%M:%S UTC"),
            "Duration": f"{duration.total_seconds():.1f}s",
            "Reason": reason[:100],
        },
    )


def alert_kafka_failures(total_failures: int, recent_error: str) -> bool:
    return send_alert(
        title="Kafka Delivery Failures",
        message=f"{total_failures} total delivery failures. Spillover is catching missed messages.",
        severity="critical",
        fields={
            "Total Failures": str(total_failures),
            "Latest Error": recent_error[:100],
        },
    )


def alert_data_staleness(last_event_age_s: float) -> bool:
    return send_alert(
        title="Data Staleness",
        message=f"No Polygon events received in {last_event_age_s:.0f}s during expected market hours.",
        severity="critical",
        fields={
            "Last Event Age": f"{last_event_age_s:.0f}s",
            "Threshold": "120s",
        },
    )


def alert_recon_mismatch(mismatch_pct: float, date: str, details: str) -> bool:
    return send_alert(
        title="Recon Mismatch Spike",
        message=f"{mismatch_pct:.1f}% of symbol-days have non-OK recon status for {date}.",
        severity="warning",
        fields={
            "Date": date,
            "Mismatch %": f"{mismatch_pct:.1f}%",
            "Breakdown": details[:200],
        },
    )


def alert_quality_drop(avg_score: float, date: str) -> bool:
    return send_alert(
        title="Quality Score Drop",
        message=f"Average data quality score fell to {avg_score:.1f} (target: 90+).",
        severity="warning",
        fields={
            "Date": date,
            "Avg Score": f"{avg_score:.1f}",
            "Target": "90",
        },
    )
