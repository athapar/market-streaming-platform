from market_streaming.producer.polygon_ws import build_subscription_params


def test_default_channels_include_am_t_q():
    params = build_subscription_params(["AAPL", "MSFT"])
    parts = params.split(",")
    assert "AM.AAPL" in parts
    assert "T.AAPL" in parts
    assert "Q.AAPL" in parts
    assert "AM.MSFT" in parts
    assert "T.MSFT" in parts
    assert "Q.MSFT" in parts


def test_explicit_channel_list():
    params = build_subscription_params(["AAPL"], channels=["AM", "T", "Q"])
    assert params.split(",") == ["AM.AAPL", "T.AAPL", "Q.AAPL"]


def test_channel_symbol_overrides():
    params = build_subscription_params(
        ["AAPL", "MSFT", "NVDA"],
        channels=["AM", "T", "Q"],
        channel_symbol_overrides={"Q": ["AAPL"]},
    )
    parts = params.split(",")
    assert "AM.AAPL" in parts
    assert "AM.MSFT" in parts
    assert "AM.NVDA" in parts
    assert "T.AAPL" in parts
    assert "T.MSFT" in parts
    assert "T.NVDA" in parts
    assert "Q.AAPL" in parts
    assert "Q.MSFT" not in parts
    assert "Q.NVDA" not in parts


def test_build_subscription_params_empty_symbols():
    assert build_subscription_params([]) == ""
