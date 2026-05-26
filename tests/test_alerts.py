"""Tests for the alerting module (Slack webhook)."""
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from market_streaming.observability.alerts import (
    alert_kafka_failures,
    alert_quality_drop,
    alert_reconnect,
    send_alert,
)


def test_send_alert_returns_false_without_webhook():
    with patch("market_streaming.observability.alerts.SLACK_WEBHOOK_URL", None):
        assert send_alert("test", "msg") is False


def test_send_alert_posts_to_webhook():
    with patch("market_streaming.observability.alerts.SLACK_WEBHOOK_URL", "https://hooks.example.com/test"), \
         patch("market_streaming.observability.alerts.urlopen") as mock_urlopen:
        mock_urlopen.return_value = MagicMock()
        result = send_alert("Test Alert", "Something happened", severity="critical")
        assert result is True
        call_args = mock_urlopen.call_args
        req = call_args[0][0]
        payload = json.loads(req.data.decode())
        assert payload["attachments"][0]["title"] == "[CRITICAL] Test Alert"
        assert payload["attachments"][0]["color"] == "#FF0000"


def test_send_alert_with_fields():
    with patch("market_streaming.observability.alerts.SLACK_WEBHOOK_URL", "https://hooks.example.com/test"), \
         patch("market_streaming.observability.alerts.urlopen") as mock_urlopen:
        mock_urlopen.return_value = MagicMock()
        send_alert("Test", "msg", fields={"Key": "Value", "Count": "42"})
        req = mock_urlopen.call_args[0][0]
        payload = json.loads(req.data.decode())
        fields = payload["attachments"][0]["fields"]
        assert len(fields) == 2
        assert fields[0]["title"] == "Key"
        assert fields[0]["value"] == "Value"


def test_send_alert_handles_network_error():
    from urllib.error import URLError
    with patch("market_streaming.observability.alerts.SLACK_WEBHOOK_URL", "https://hooks.example.com/test"), \
         patch("market_streaming.observability.alerts.urlopen", side_effect=URLError("timeout")):
        result = send_alert("Test", "msg")
        assert result is False


def test_alert_reconnect_formats_duration():
    t0 = datetime(2026, 5, 27, 14, 0, 0, tzinfo=timezone.utc)
    t1 = datetime(2026, 5, 27, 14, 0, 45, tzinfo=timezone.utc)
    with patch("market_streaming.observability.alerts.SLACK_WEBHOOK_URL", "https://hooks.example.com/test"), \
         patch("market_streaming.observability.alerts.urlopen") as mock_urlopen:
        mock_urlopen.return_value = MagicMock()
        result = alert_reconnect(t0, t1, "ConnectionClosed")
        assert result is True
        req = mock_urlopen.call_args[0][0]
        payload = json.loads(req.data.decode())
        fields = {f["title"]: f["value"] for f in payload["attachments"][0]["fields"]}
        assert fields["Duration"] == "45.0s"


def test_alert_kafka_failures_includes_count():
    with patch("market_streaming.observability.alerts.SLACK_WEBHOOK_URL", "https://hooks.example.com/test"), \
         patch("market_streaming.observability.alerts.urlopen") as mock_urlopen:
        mock_urlopen.return_value = MagicMock()
        alert_kafka_failures(500, "BrokerNotAvailable")
        req = mock_urlopen.call_args[0][0]
        payload = json.loads(req.data.decode())
        assert "500" in payload["attachments"][0]["text"]


def test_alert_quality_drop_includes_score():
    with patch("market_streaming.observability.alerts.SLACK_WEBHOOK_URL", "https://hooks.example.com/test"), \
         patch("market_streaming.observability.alerts.urlopen") as mock_urlopen:
        mock_urlopen.return_value = MagicMock()
        alert_quality_drop(72.3, "2026-05-27")
        req = mock_urlopen.call_args[0][0]
        payload = json.loads(req.data.decode())
        assert "72.3" in payload["attachments"][0]["text"]
