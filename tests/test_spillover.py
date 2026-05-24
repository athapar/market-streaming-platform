from datetime import datetime, timezone
from pathlib import Path

from market_streaming.producer.envelope import Envelope
from market_streaming.producer.spillover import (
    SpilloverWriter,
    mark_replayed,
    pending_files,
    read_spillover,
    replay_all,
    write_gap,
)


def _env(symbol: str, payload: str) -> Envelope:
    return Envelope(topic="market.trades", key=symbol, value=payload)


def test_writer_appends_ndjson(tmp_path: Path):
    w = SpilloverWriter(tmp_path)
    when = datetime(2026, 5, 23, 12, 0, tzinfo=timezone.utc)
    w.write(_env("AAPL", '{"a":1}'), when=when)
    w.write(_env("MSFT", '{"b":2}'), when=when)

    file = tmp_path / "2026-05-23.ndjson"
    lines = file.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2

    envs = list(read_spillover(file))
    assert [e.key for e in envs] == ["AAPL", "MSFT"]


def test_replay_all_renames_files(tmp_path: Path):
    w = SpilloverWriter(tmp_path)
    when = datetime(2026, 5, 23, 12, 0, tzinfo=timezone.utc)
    w.write(_env("AAPL", '{"x":1}'), when=when)

    published: list[Envelope] = []
    counts = replay_all(tmp_path, published.append)

    assert counts == {"files": 1, "envelopes": 1}
    assert pending_files(tmp_path) == []  # original .ndjson got renamed
    assert (tmp_path / "2026-05-23.ndjson.replayed").exists()
    assert published[0].key == "AAPL"


def test_replay_skips_already_replayed(tmp_path: Path):
    w = SpilloverWriter(tmp_path)
    when = datetime(2026, 5, 23, 12, 0, tzinfo=timezone.utc)
    file = w.write(_env("AAPL", '{"x":1}'), when=when)
    mark_replayed(file)

    published: list[Envelope] = []
    counts = replay_all(tmp_path, published.append)
    assert counts == {"files": 0, "envelopes": 0}
    assert published == []


def test_write_gap_appends(tmp_path: Path):
    t0 = datetime(2026, 5, 23, 12, 0, 0, tzinfo=timezone.utc)
    t1 = datetime(2026, 5, 23, 12, 0, 45, tzinfo=timezone.utc)
    write_gap(tmp_path, t0, t1, reason="ConnectionClosedError")

    path = tmp_path / "gaps-2026-05-23.ndjson"
    line = path.read_text(encoding="utf-8").strip()
    assert '"duration_s":45.0' in line
    assert '"reason":"ConnectionClosedError"' in line
