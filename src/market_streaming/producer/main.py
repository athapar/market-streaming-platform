"""
Producer entry point: Polygon WebSocket -> Kafka.

Usage:
    python -m market_streaming.producer.main           # full run, requires Confluent creds
    python -m market_streaming.producer.main --dry-run # skip Kafka, just receive & count events

Exit on Ctrl-C; the Kafka producer is flushed on shutdown so in-flight
messages either commit or land in spillover.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from pathlib import Path

from confluent_kafka import Producer

from market_streaming.config import (
    DATA_DIR,
    SPILLOVER_DIR,
    load_symbols,
    require_env,
)
from market_streaming.producer.envelope import route_event
from market_streaming.producer.kafka_sink import (
    DryRunSink,
    KafkaSink,
    build_producer_config,
)
from market_streaming.producer.metrics import Metrics, heartbeat_loop
from market_streaming.producer.polygon_ws import (
    DEFAULT_CHANNELS,
    DEFAULT_WS_URL,
    stream_events,
)
from market_streaming.producer.spillover import SpilloverWriter

GAPS_DIR = DATA_DIR / "gaps"


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


async def _run(args: argparse.Namespace) -> int:
    symbols = load_symbols()
    if not symbols:
        print("symbols.txt is empty; nothing to subscribe to.", file=sys.stderr)
        return 1

    api_key = require_env("POLYGON_API_KEY")
    metrics = Metrics()
    spillover = SpilloverWriter(SPILLOVER_DIR)

    if args.dry_run:
        sink: KafkaSink | DryRunSink = DryRunSink(metrics)
    else:
        producer = Producer(
            build_producer_config(
                bootstrap_servers=require_env("KAFKA_BOOTSTRAP_SERVERS"),
                sasl_username=require_env("KAFKA_SASL_USERNAME"),
                sasl_password=require_env("KAFKA_SASL_PASSWORD"),
            )
        )
        sink = KafkaSink(producer, spillover, metrics)

    stop = asyncio.Event()

    def _signal_handler(*_: object) -> None:
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler for asyncio loops;
            # KeyboardInterrupt still escapes via the default handler.
            signal.signal(sig, lambda *_: _signal_handler())

    hb = asyncio.create_task(heartbeat_loop(metrics, args.heartbeat_s))
    try:
        async for event in stream_events(
            api_key=api_key,
            symbols=symbols,
            metrics=metrics,
            gaps_dir=GAPS_DIR,
            ws_url=args.ws_url,
            channels=args.channels.split(","),
        ):
            if stop.is_set():
                break
            envelope = route_event(event)
            if envelope is None:
                continue
            sink.publish(envelope)
    except asyncio.CancelledError:
        pass
    finally:
        hb.cancel()
        flushed = sink.flush(10.0)
        if flushed:
            print(f"warning: {flushed} message(s) still in producer queue at shutdown", file=sys.stderr)
        snap = metrics.snapshot()
        print(f"[final] {snap}")
    return 0


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true", help="skip Kafka, print samples only")
    p.add_argument("--ws-url", default=DEFAULT_WS_URL, help="Polygon WebSocket URL")
    p.add_argument(
        "--channels",
        default=",".join(DEFAULT_CHANNELS),
        help="Comma-separated Polygon channel prefixes (e.g. AM or AM,T,Q)",
    )
    p.add_argument("--heartbeat-s", type=float, default=10.0)
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()
    _setup_logging(args.verbose)
    try:
        return asyncio.run(_run(args))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
