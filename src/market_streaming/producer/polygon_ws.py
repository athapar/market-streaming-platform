"""
Polygon WebSocket client.

Auth flow (delayed and realtime endpoints behave identically here):
  1. Connect -> server pushes {"ev":"status","status":"connected"}
  2. Send  {"action":"auth","params":"<API_KEY>"}
  3. Server replies {"ev":"status","status":"auth_success"}
  4. Send  {"action":"subscribe","params":"T.AAPL,T.MSFT,Q.AAPL,..."}
  5. Server streams arrays of event dicts; we yield one dict at a time.

`stream_events` is an async generator that owns the reconnect loop. On any
disconnect it records a gap (disconnect_at, reconnect_at, reason) to the gaps
directory and reconnects with exponential backoff. Gaps inform later REST
backfill decisions (out of scope for v1 producer, but the data is there).
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncIterator, Iterable

import websockets
from websockets.exceptions import ConnectionClosed, InvalidStatus, WebSocketException

from market_streaming.observability.alerts import alert_reconnect
from market_streaming.producer.metrics import Metrics
from market_streaming.producer.spillover import write_gap

log = logging.getLogger(__name__)

DEFAULT_WS_URL = "wss://socket.polygon.io/stocks"
DEFAULT_CHANNELS: tuple[str, ...] = ("AM", "T", "Q")


class AuthError(RuntimeError):
    pass


def build_subscription_params(
    symbols: Iterable[str],
    channels: Iterable[str] = DEFAULT_CHANNELS,
    channel_symbol_overrides: dict[str, Iterable[str]] | None = None,
) -> str:
    """Build a Polygon subscribe params string like 'AM.AAPL,AM.MSFT,T.AAPL,...'.

    By default every channel subscribes to all symbols. Use channel_symbol_overrides
    to restrict specific channels to a subset — e.g. ``{"Q": quote_symbols}`` to
    receive quotes only for the 20 most-liquid names while trades and aggregates
    cover the full universe.
    """
    overrides = channel_symbol_overrides or {}
    parts: list[str] = []
    for channel in channels:
        chan_symbols = overrides.get(channel, symbols)
        for sym in chan_symbols:
            parts.append(f"{channel}.{sym}")
    return ",".join(parts)


async def _auth_and_subscribe(
    ws: websockets.WebSocketClientProtocol,
    api_key: str,
    subscription: str,
) -> None:
    # Drain initial status frame ("connected").
    initial = await ws.recv()
    log.debug("polygon initial frame: %s", initial)

    await ws.send(json.dumps({"action": "auth", "params": api_key}))
    auth_reply_raw = await ws.recv()
    auth_reply = json.loads(auth_reply_raw)
    statuses = [f.get("status") for f in auth_reply if f.get("ev") == "status"]
    if "auth_success" not in statuses:
        raise AuthError(f"polygon auth failed: {auth_reply!r}")

    await ws.send(json.dumps({"action": "subscribe", "params": subscription}))
    sub_reply_raw = await ws.recv()
    log.info("polygon subscription reply: %s", sub_reply_raw)


async def stream_events(
    api_key: str,
    symbols: Iterable[str],
    metrics: Metrics,
    gaps_dir: Path,
    ws_url: str = DEFAULT_WS_URL,
    channels: Iterable[str] = DEFAULT_CHANNELS,
    channel_symbol_overrides: dict[str, Iterable[str]] | None = None,
    backoff_initial_s: float = 1.0,
    backoff_max_s: float = 60.0,
) -> AsyncIterator[dict]:
    """
    Connect to Polygon, auth, subscribe, and yield event dicts forever.
    Reconnects with exponential backoff on any disconnect.
    """
    subscription = build_subscription_params(symbols, channels, channel_symbol_overrides)
    backoff = backoff_initial_s

    while True:
        disconnect_at: datetime | None = None
        reason = "ok"
        try:
            async with websockets.connect(ws_url, max_size=2**22) as ws:
                await _auth_and_subscribe(ws, api_key, subscription)
                log.info("polygon connected; subscribed to %d channels", subscription.count(",") + 1)
                backoff = backoff_initial_s  # reset on successful connect
                async for raw in ws:
                    try:
                        frames = json.loads(raw)
                    except json.JSONDecodeError:
                        log.warning("non-JSON frame from polygon: %r", raw[:200])
                        continue
                    if not isinstance(frames, list):
                        frames = [frames]
                    for frame in frames:
                        if frame.get("ev") == "status":
                            metrics.polygon_status_frames += 1
                            log.info("polygon status: %s", frame)
                            continue
                        metrics.mark_polygon_event(frame.get("ev", "unknown"))
                        yield frame
        except AuthError:
            raise  # don't retry auth failures
        except (ConnectionClosed, InvalidStatus, WebSocketException, OSError) as exc:
            disconnect_at = datetime.now(timezone.utc)
            reason = f"{type(exc).__name__}: {exc}"
            log.warning("polygon connection dropped: %s", reason)
        except asyncio.CancelledError:
            raise

        if disconnect_at is not None:
            reconnect_at = datetime.now(timezone.utc)
            # backoff first, THEN log the gap so reconnect_at reflects the
            # actual recovery moment, not the moment we decided to retry.
            await asyncio.sleep(backoff)
            reconnect_at = datetime.now(timezone.utc)
            write_gap(gaps_dir, disconnect_at, reconnect_at, reason)
            metrics.mark_reconnect()
            alert_reconnect(disconnect_at, reconnect_at, reason)
            backoff = min(backoff * 2, backoff_max_s)
