"""
NDJSON spillover for messages that couldn't be delivered to Kafka.

Append-only files rolled by UTC date. Replay reads each file line-by-line and
re-publishes via the same Kafka producer, then renames the file with a
.replayed suffix. This is the at-least-once boundary: a crash mid-replay means
re-running may publish duplicates, which is fine because Bronze is dedup'd
downstream in Silver via MERGE on trade_id (and on the structural dedup key
for quotes).
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from market_streaming.producer.envelope import Envelope


def _date_path(directory: Path, when: datetime | None = None) -> Path:
    when = when or datetime.now(timezone.utc)
    return directory / f"{when.strftime('%Y-%m-%d')}.ndjson"


class SpilloverWriter:
    def __init__(self, directory: Path) -> None:
        self.directory = directory
        self.directory.mkdir(parents=True, exist_ok=True)

    def write(self, envelope: Envelope, when: datetime | None = None) -> Path:
        path = _date_path(self.directory, when)
        record = envelope.to_spillover_record()
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, separators=(",", ":")))
            f.write("\n")
        return path


def read_spillover(path: Path) -> Iterator[Envelope]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield Envelope.from_spillover_record(json.loads(line))


def pending_files(directory: Path) -> list[Path]:
    if not directory.exists():
        return []
    return sorted(p for p in directory.iterdir() if p.suffix == ".ndjson")


def mark_replayed(path: Path) -> Path:
    new_path = path.with_suffix(path.suffix + ".replayed")
    path.rename(new_path)
    return new_path


def gap_log_path(directory: Path, when: datetime | None = None) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    when = when or datetime.now(timezone.utc)
    return directory / f"gaps-{when.strftime('%Y-%m-%d')}.ndjson"


def write_gap(
    directory: Path,
    disconnect_at: datetime,
    reconnect_at: datetime,
    reason: str,
) -> None:
    record = {
        "disconnect_at": disconnect_at.isoformat(),
        "reconnect_at": reconnect_at.isoformat(),
        "duration_s": round((reconnect_at - disconnect_at).total_seconds(), 3),
        "reason": reason,
    }
    path = gap_log_path(directory, reconnect_at)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, separators=(",", ":")))
        f.write("\n")


def replay_all(
    directory: Path,
    publish: "callable[[Envelope], None]",
) -> dict[str, int]:
    files = pending_files(directory)
    counts = {"files": 0, "envelopes": 0}
    for path in files:
        for env in read_spillover(path):
            publish(env)
            counts["envelopes"] += 1
        mark_replayed(path)
        counts["files"] += 1
    return counts
