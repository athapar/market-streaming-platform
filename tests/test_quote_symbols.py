"""Tests for config: quote symbols loading and symbol validation."""
from market_streaming.config import load_quote_symbols, load_symbols


def test_load_symbols_returns_104():
    symbols = load_symbols()
    assert len(symbols) == 104


def test_load_symbols_includes_benchmarks():
    symbols = load_symbols()
    for benchmark in ["SPY", "QQQ", "IWM", "DIA"]:
        assert benchmark in symbols


def test_load_symbols_all_uppercase_alpha():
    symbols = load_symbols()
    for s in symbols:
        assert s == s.upper()
        assert s.isalpha()


def test_load_symbols_no_duplicates():
    symbols = load_symbols()
    assert len(symbols) == len(set(symbols))


def test_load_quote_symbols_returns_20():
    quote_syms = load_quote_symbols()
    assert len(quote_syms) == 20


def test_load_quote_symbols_is_subset_of_symbols():
    symbols = set(load_symbols())
    quote_syms = load_quote_symbols()
    for qs in quote_syms:
        assert qs in symbols, f"{qs} in quote_symbols.txt but not in symbols.txt"


def test_load_quote_symbols_includes_key_names():
    quote_syms = load_quote_symbols()
    for name in ["AAPL", "MSFT", "SPY", "NVDA"]:
        assert name in quote_syms
