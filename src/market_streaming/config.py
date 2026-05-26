import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
SEED_DIR = DATA_DIR / "seed"
SPILLOVER_DIR = DATA_DIR / "spillover"
SYMBOLS_PATH = PROJECT_ROOT / "symbols.txt"

SECURITY_MASTER_SEED_PATH = SEED_DIR / "security_master_current.parquet"
QUOTE_SYMBOLS_PATH = PROJECT_ROOT / "quote_symbols.txt"


def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"missing required env var: {name}")
    return v


def optional_env(name: str, default: str | None = None) -> str | None:
    return os.getenv(name, default)


def load_symbols() -> list[str]:
    return [s.strip().upper() for s in SYMBOLS_PATH.read_text().splitlines() if s.strip()]


def load_quote_symbols() -> list[str]:
    if not QUOTE_SYMBOLS_PATH.exists():
        return []
    return [s.strip().upper() for s in QUOTE_SYMBOLS_PATH.read_text().splitlines() if s.strip()]
