from market_streaming.config import load_symbols


def test_load_symbols_nonempty_and_uppercase():
    symbols = load_symbols()
    assert symbols, "symbols.txt should not be empty"
    assert all(s == s.upper() for s in symbols)
    assert all(s.isalpha() for s in symbols)
