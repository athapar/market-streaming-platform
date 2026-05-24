"""
Publish synthetic AM (minute aggregate) events to market.aggregates.

Use this to validate the full producer -> Kafka -> Bronze path without
waiting for real Polygon traffic (e.g. weekends, outside market hours, or
before a paid plan upgrade enables higher-volume channels).

The events have the same JSON shape Polygon's AM stream emits, so Bronze and
Silver process them indistinguishably from real ones. Optional knobs:
  --dup-rate    fraction of events to publish twice (tests Silver MERGE).
  --late-rate   fraction of events backdated by --late-window-min minutes
                (tests Silver watermark + late-data behavior).
"""
from __future__ import annotations

import argparse
import random
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

from market_streaming.config import load_symbols, require_env
from market_streaming.producer.envelope import route_event
from market_streaming.producer.kafka_sink import build_producer_config


def make_am_event(symbol: str, minute_start_ms: int, rng: random.Random) -> dict:
    base = 50 + rng.random() * 350
    open_p = round(base, 2)
    close = round(open_p + rng.uniform(-0.8, 0.8), 2)
    high = round(max(open_p, close) + rng.uniform(0, 0.4), 2)
    low = round(min(open_p, close) - rng.uniform(0, 0.4), 2)
    volume = rng.randint(1_000, 250_000)
    vwap = round((open_p + close + high + low) / 4, 4)
    return {
        "ev": "AM",
        "sym": symbol,
        "o": open_p,
        "h": high,
        "l": low,
        "c": close,
        "v": volume,
        "vw": vwap,
        "n": rng.randint(50, 5_000),
        "s": minute_start_ms,
        "e": minute_start_ms + 60_000,
    }


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--rate", type=float, default=5.0, help="events per second across all symbols")
    p.add_argument("--duration", type=float, default=60.0, help="seconds to run (0 = forever)")
    p.add_argument("--dup-rate", type=float, default=0.0, help="fraction of events to duplicate")
    p.add_argument("--late-rate", type=float, default=0.0, help="fraction of events backdated")
    p.add_argument("--late-window-min", type=int, default=15, help="how many minutes to backdate by")
    p.add_argument("--seed", type=int, default=None)
    args = p.parse_args()

    symbols = load_symbols()
    if not symbols:
        print("symbols.txt is empty.", file=sys.stderr)
        return 1

    rng = random.Random(args.seed)
    producer = Producer(
        build_producer_config(
            bootstrap_servers=require_env("KAFKA_BOOTSTRAP_SERVERS"),
            sasl_username=require_env("KAFKA_SASL_USERNAME"),
            sasl_password=require_env("KAFKA_SASL_PASSWORD"),
            client_id="synthetic-publisher",
        )
    )

    sleep_s = 1.0 / args.rate if args.rate > 0 else 0.0
    start = time.monotonic()
    sent = sent_dup = sent_late = failed = 0

    def cb(err, _msg) -> None:
        nonlocal failed
        if err is not None:
            failed += 1

    try:
        while True:
            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            minute_ms = (now_ms // 60_000) * 60_000

            is_late = args.late_rate > 0 and rng.random() < args.late_rate
            event_minute_ms = minute_ms - (args.late_window_min * 60_000 if is_late else 0)

            sym = rng.choice(symbols)
            event = make_am_event(sym, event_minute_ms, rng)
            env = route_event(event)
            assert env is not None, "synthetic AM event must be routable"

            producer.produce(env.topic, key=env.key, value=env.value, on_delivery=cb)
            sent += 1
            sent_late += int(is_late)

            if args.dup_rate > 0 and rng.random() < args.dup_rate:
                producer.produce(env.topic, key=env.key, value=env.value, on_delivery=cb)
                sent_dup += 1

            producer.poll(0)

            if args.duration > 0 and time.monotonic() - start >= args.duration:
                break
            if sleep_s:
                time.sleep(sleep_s)
    except KeyboardInterrupt:
        print("interrupted; flushing...", file=sys.stderr)
    finally:
        remaining = producer.flush(10.0)
        print(
            f"sent={sent} duplicates={sent_dup} late={sent_late} "
            f"failed={failed} unflushed={remaining}"
        )
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
